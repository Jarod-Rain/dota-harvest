"""Manifest lifecycle: which ids a fetch run will pick up, and what it records."""

from __future__ import annotations

import sqlite3

import pytest

from dota_harvest.core.manifest import (
    MAX_FETCH_ATTEMPTS,
    SCHEMA,
    MatchStatus,
    fmt_date,
    parse_date,
    pending_ids,
)
from dota_harvest.pipeline.fetch import _mark, _mark_fetched, chunks


@pytest.fixture
def conn():
    connection = sqlite3.connect(":memory:")
    connection.executescript(SCHEMA)
    yield connection
    connection.close()


def _add(conn, match_id, status=MatchStatus.DISCOVERED, attempts=0):
    conn.execute(
        "INSERT INTO matches (match_id, source, status, attempts, start_time) VALUES (?,?,?,?,?)",
        (match_id, "public", status.value, attempts, 1_700_000_000 + match_id),
    )
    conn.commit()


def test_status_values_are_plain_strings():
    """Rows are compared against these in SQL, so the wire format must not change."""
    assert MatchStatus.FETCHED.value == "fetched"
    assert f"{MatchStatus.MISSING}" == "missing"


def test_pending_includes_discovered_and_failed(conn):
    _add(conn, 1, MatchStatus.DISCOVERED)
    _add(conn, 2, MatchStatus.FAILED, attempts=1)
    assert set(pending_ids(conn, 10)) == {1, 2}


def test_pending_excludes_terminal_states(conn):
    """A fetched or missing id is done; retrying it wastes quota."""
    _add(conn, 1, MatchStatus.FETCHED)
    _add(conn, 2, MatchStatus.MISSING)
    assert pending_ids(conn, 10) == []


def test_pending_respects_the_attempt_ceiling(conn):
    _add(conn, 1, MatchStatus.FAILED, attempts=MAX_FETCH_ATTEMPTS)
    assert pending_ids(conn, 10) == []


def test_pending_returns_newest_first(conn):
    for match_id in (1, 2, 3):
        _add(conn, match_id)
    assert pending_ids(conn, 10) == [3, 2, 1]


def test_pending_honours_the_limit(conn):
    for match_id in (1, 2, 3):
        _add(conn, match_id)
    assert len(pending_ids(conn, 2)) == 2


def test_mark_records_per_id_error_messages(conn):
    _add(conn, 1)
    _add(conn, 2)
    _mark(conn, [1, 2], MatchStatus.FAILED, errors={1: "first", 2: "second"})
    rows = dict(conn.execute("SELECT match_id, last_error FROM matches").fetchall())
    assert rows == {1: "first", 2: "second"}


def test_mark_binds_status_rather_than_interpolating(conn):
    """The one statement that writes run state must not be injectable."""
    _add(conn, 1)
    _mark(conn, [1], "x'; DROP TABLE matches; --")
    assert conn.execute("SELECT COUNT(*) FROM matches").fetchone()[0] == 1


def test_mark_increments_attempts(conn):
    _add(conn, 1)
    _mark(conn, [1], MatchStatus.FAILED, "boom")
    _mark(conn, [1], MatchStatus.FAILED, "boom")
    assert conn.execute("SELECT attempts FROM matches").fetchone()[0] == 2


def test_mark_fetched_records_the_shard(conn):
    """Every row must be traceable back to the file holding its response."""
    _add(conn, 1)
    _mark_fetched(conn, [1], "part-abc.jsonl.gz")
    row = conn.execute("SELECT status, raw_file FROM matches").fetchone()
    assert row == (MatchStatus.FETCHED.value, "part-abc.jsonl.gz")


def test_mark_with_no_ids_is_a_no_op(conn):
    _add(conn, 1)
    _mark(conn, [], MatchStatus.FAILED)
    assert conn.execute("SELECT attempts FROM matches").fetchone()[0] == 0


def test_chunks_covers_every_id_without_overlap():
    assert list(chunks([1, 2, 3, 4, 5], 2)) == [[1, 2], [3, 4], [5]]


def test_date_round_trip():
    assert fmt_date(parse_date("2026-08-25")) == "2026-08-25"
