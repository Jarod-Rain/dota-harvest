"""Walk selection and removal.

Both features address the same need: a manifest accumulates several independent
walks, and operations should be able to name one without disturbing the others.
"""

from __future__ import annotations

import argparse
import sqlite3

import pytest

from dota_harvest.cli.main import walk_list
from dota_harvest.core.manifest import (
    SCHEMA,
    MatchStatus,
    known_walks,
    parse_walk,
    pending_ids,
    remove_walks,
)


@pytest.fixture
def conn():
    connection = sqlite3.connect(":memory:")
    connection.executescript(SCHEMA)
    yield connection
    connection.close()


def _add(conn, match_id, source="public", label="main", status=MatchStatus.DISCOVERED):
    conn.execute(
        "INSERT INTO matches (match_id, source, label, status, start_time) VALUES (?,?,?,?,?)",
        (match_id, source, label, status.value, 1_700_000_000 + match_id),
    )
    conn.commit()


def _cursor(conn, key, value=1):
    conn.execute("INSERT INTO cursors(key, value) VALUES(?,?)", (key, value))
    conn.commit()


# --- selector parsing ----------------------------------------------------


def test_bare_label_matches_any_source():
    assert parse_walk("recent-2d") == (None, "recent-2d")


def test_qualified_selector_pins_the_source():
    """Labels are only unique per source, so the qualified form disambiguates."""
    assert parse_walk("public:recent-2d") == ("public", "recent-2d")


def test_selector_strips_surrounding_whitespace():
    assert parse_walk("  public : recent-2d  ".replace(" : ", ":")) == ("public", "recent-2d")


@pytest.mark.parametrize("bad", ["", "   ", ":label", "source:", ":"])
def test_malformed_selectors_are_rejected(bad):
    """An empty half would silently select everything or nothing."""
    with pytest.raises(ValueError):
        parse_walk(bad)


def test_walk_list_splits_on_commas():
    assert walk_list("a,public:b") == ["a", "public:b"]


def test_walk_list_ignores_blank_entries():
    assert walk_list("a, ,b") == ["a", "b"]


@pytest.mark.parametrize("bad", ["", ",", "  ", ":x", "x:"])
def test_walk_list_rejects_malformed_input(bad):
    """Failing here stops a typo from looking like a fully-collected walk."""
    with pytest.raises(argparse.ArgumentTypeError):
        walk_list(bad)


# --- selecting pending ids ----------------------------------------------


def test_pending_without_walks_spans_every_walk(conn):
    _add(conn, 1, label="alpha")
    _add(conn, 2, label="beta")
    assert set(pending_ids(conn, 10)) == {1, 2}


def test_pending_restricted_to_one_walk(conn):
    _add(conn, 1, label="alpha")
    _add(conn, 2, label="beta")
    assert pending_ids(conn, 10, ["alpha"]) == [1]


def test_pending_accepts_several_walks(conn):
    _add(conn, 1, label="alpha")
    _add(conn, 2, label="beta")
    _add(conn, 3, label="gamma")
    assert set(pending_ids(conn, 10, ["alpha", "gamma"])) == {1, 3}


def test_pending_qualified_selector_respects_source(conn):
    _add(conn, 1, source="public", label="shared")
    _add(conn, 2, source="pro", label="shared")
    assert pending_ids(conn, 10, ["public:shared"]) == [1]
    assert pending_ids(conn, 10, ["pro:shared"]) == [2]


def test_pending_bare_label_spans_both_sources(conn):
    _add(conn, 1, source="public", label="shared")
    _add(conn, 2, source="pro", label="shared")
    assert set(pending_ids(conn, 10, ["shared"])) == {1, 2}


def test_pending_unknown_walk_is_empty(conn):
    _add(conn, 1, label="alpha")
    assert pending_ids(conn, 10, ["nope"]) == []


def test_pending_still_excludes_terminal_statuses(conn):
    """Walk selection must not resurrect already-fetched ids."""
    _add(conn, 1, label="alpha", status=MatchStatus.FETCHED)
    _add(conn, 2, label="alpha", status=MatchStatus.DISCOVERED)
    assert pending_ids(conn, 10, ["alpha"]) == [2]


# --- removal -------------------------------------------------------------


def test_remove_deletes_unfetched_rows_and_the_cursor(conn):
    _add(conn, 1, label="alpha")
    _add(conn, 2, label="alpha", status=MatchStatus.MISSING)
    _cursor(conn, "public:alpha")

    tally = remove_walks(conn, ["alpha"])

    assert tally["deleted"] == 2
    assert tally["cursors"] == 1
    assert conn.execute("SELECT COUNT(*) FROM matches").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM cursors").fetchone()[0] == 0


def test_remove_preserves_fetched_rows_by_default(conn):
    """Their responses are on disk; deleting the row would orphan that data."""
    _add(conn, 1, label="alpha", status=MatchStatus.FETCHED)
    _add(conn, 2, label="alpha")

    tally = remove_walks(conn, ["alpha"])

    assert tally["kept_fetched"] == 1
    assert tally["deleted"] == 1
    survivors = conn.execute("SELECT match_id, status FROM matches").fetchall()
    assert survivors == [(1, MatchStatus.FETCHED.value)]


def test_remove_all_deletes_fetched_rows_too(conn):
    _add(conn, 1, label="alpha", status=MatchStatus.FETCHED)
    _add(conn, 2, label="alpha")

    tally = remove_walks(conn, ["alpha"], include_fetched=True)

    assert tally["deleted"] == 2
    assert tally["kept_fetched"] == 0
    assert conn.execute("SELECT COUNT(*) FROM matches").fetchone()[0] == 0


def test_remove_leaves_other_walks_untouched(conn):
    _add(conn, 1, label="alpha")
    _add(conn, 2, label="beta")
    _cursor(conn, "public:alpha")
    _cursor(conn, "public:beta")

    remove_walks(conn, ["alpha"])

    assert [r[0] for r in conn.execute("SELECT match_id FROM matches")] == [2]
    assert [r[0] for r in conn.execute("SELECT key FROM cursors")] == ["public:beta"]


def test_remove_qualified_selector_spares_the_other_source(conn):
    _add(conn, 1, source="public", label="shared")
    _add(conn, 2, source="pro", label="shared")
    _cursor(conn, "public:shared")
    _cursor(conn, "pro:shared")

    remove_walks(conn, ["public:shared"])

    assert [r[0] for r in conn.execute("SELECT match_id FROM matches")] == [2]
    assert [r[0] for r in conn.execute("SELECT key FROM cursors")] == ["pro:shared"]


def test_remove_unknown_walk_is_a_no_op(conn):
    _add(conn, 1, label="alpha")
    tally = remove_walks(conn, ["nope"])
    assert tally["deleted"] == 0
    assert conn.execute("SELECT COUNT(*) FROM matches").fetchone()[0] == 1


def test_remove_reports_a_tally_per_status(conn):
    _add(conn, 1, label="alpha", status=MatchStatus.DISCOVERED)
    _add(conn, 2, label="alpha", status=MatchStatus.FAILED)
    _add(conn, 3, label="alpha", status=MatchStatus.MISSING)

    tally = remove_walks(conn, ["alpha"])

    assert tally[MatchStatus.DISCOVERED.value] == 1
    assert tally[MatchStatus.FAILED.value] == 1
    assert tally[MatchStatus.MISSING.value] == 1


def test_known_walks_lists_each_walk_with_a_count(conn):
    _add(conn, 1, label="alpha")
    _add(conn, 2, label="beta")
    _add(conn, 3, label="beta")
    assert known_walks(conn) == [("public", "beta", 2), ("public", "alpha", 1)]
