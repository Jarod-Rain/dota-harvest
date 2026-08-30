"""Walk parameter locking and resumability.

A walk's rows are only a coherent sample if every page passed through identical
filters. These tests pin the two halves of that guarantee: parameters are fixed
when the walk is created, and a walk knows whether it has more ground to cover.
"""

from __future__ import annotations

import sqlite3

import pytest

from dota_harvest.core.manifest import (
    RANGE_PARAMS,
    SCHEMA,
    STOP_ARCHIVE_EXHAUSTED,
    STOP_PAGE_BUDGET,
    STOP_QUOTA,
    STOP_REACHED_FLOOR,
    STOP_RESERVE,
    TERMINAL_REASONS,
    MatchStatus,
    WalkState,
    finish_walk,
    get_walk,
    list_walks,
    remove_walks,
    start_walk,
    walk_key,
)

PARAMS = {
    "source": "public",
    "min_rank": 60,
    "pages": 20,
    "until_ts": None,
    "sample": 1.0,
    "ranked_only": True,
    "min_age_hours": 48,
    "reserve": 50,
    "seek": True,
}


@pytest.fixture
def conn():
    connection = sqlite3.connect(":memory:")
    connection.executescript(SCHEMA)
    yield connection
    connection.close()


# --- parameter locking ---------------------------------------------------


def test_first_run_stores_the_parameters(conn):
    assert start_walk(conn, "public", "alpha", PARAMS) == PARAMS
    assert get_walk(conn, "alpha")["params"] == PARAMS


def test_later_runs_get_the_original_filters_back(conn):
    """The lock: a second run under different filters must not take effect."""
    start_walk(conn, "public", "alpha", PARAMS)
    returned = start_walk(
        conn, "public", "alpha", {**PARAMS, "min_rank": 80, "sample": 0.5, "ranked_only": False}
    )

    assert returned["min_rank"] == 60
    assert returned["sample"] == 1.0
    assert returned["ranked_only"] is True
    assert get_walk(conn, "alpha")["params"]["min_rank"] == 60


def test_a_new_walk_is_open_before_it_runs(conn):
    start_walk(conn, "public", "alpha", PARAMS)
    walk = get_walk(conn, "alpha")
    assert walk["state"] == WalkState.OPEN
    assert walk["reason"] is None


def test_walks_under_different_labels_are_independent(conn):
    start_walk(conn, "public", "alpha", PARAMS)
    start_walk(conn, "public", "beta", {**PARAMS, "min_rank": 80})
    assert get_walk(conn, "alpha")["params"]["min_rank"] == 60
    assert get_walk(conn, "beta")["params"]["min_rank"] == 80


def test_same_label_under_two_sources_is_ambiguous(conn):
    """Resuming the wrong one would append to an unrelated sample."""
    start_walk(conn, "public", "shared", PARAMS)
    start_walk(conn, "pro", "shared", {**PARAMS, "source": "pro"})

    with pytest.raises(ValueError, match="ambiguous"):
        get_walk(conn, "shared")

    assert get_walk(conn, "public:shared")["params"]["source"] == "public"
    assert get_walk(conn, "pro:shared")["params"]["source"] == "pro"


def test_get_walk_returns_none_when_absent(conn):
    assert get_walk(conn, "nope") is None


# --- completion state ----------------------------------------------------


@pytest.mark.parametrize("reason", [STOP_REACHED_FLOOR, STOP_ARCHIVE_EXHAUSTED])
def test_terminal_reasons_close_the_walk(conn, reason):
    start_walk(conn, "public", "alpha", PARAMS)
    finish_walk(conn, "public", "alpha", reason, 3)
    assert get_walk(conn, "alpha")["state"] == WalkState.DONE


@pytest.mark.parametrize("reason", [STOP_PAGE_BUDGET, STOP_QUOTA, STOP_RESERVE])
def test_interruptions_leave_the_walk_open(conn, reason):
    """Page budget counts as unfinished: more pages remain to collect."""
    start_walk(conn, "public", "alpha", PARAMS)
    finish_walk(conn, "public", "alpha", reason, 3)
    assert get_walk(conn, "alpha")["state"] == WalkState.OPEN


def test_terminal_reasons_are_exactly_the_two_completions():
    assert frozenset({STOP_REACHED_FLOOR, STOP_ARCHIVE_EXHAUSTED}) == TERMINAL_REASONS


def test_finish_records_why_it_stopped(conn):
    start_walk(conn, "public", "alpha", PARAMS)
    finish_walk(conn, "public", "alpha", STOP_QUOTA, 2)
    assert get_walk(conn, "alpha")["reason"] == STOP_QUOTA


def test_pages_accumulate_across_resumes(conn):
    """A resumed walk should report total progress, not the last run's."""
    start_walk(conn, "public", "alpha", PARAMS)
    finish_walk(conn, "public", "alpha", STOP_PAGE_BUDGET, 2)
    finish_walk(conn, "public", "alpha", STOP_PAGE_BUDGET, 3)
    assert get_walk(conn, "alpha")["pages_done"] == 5


def test_a_closed_walk_can_be_reopened_by_a_later_interruption(conn):
    """State reflects the most recent run, not a one-way latch."""
    start_walk(conn, "public", "alpha", PARAMS)
    finish_walk(conn, "public", "alpha", STOP_ARCHIVE_EXHAUSTED, 1)
    assert get_walk(conn, "alpha")["state"] == WalkState.DONE
    finish_walk(conn, "public", "alpha", STOP_QUOTA, 1)
    assert get_walk(conn, "alpha")["state"] == WalkState.OPEN


# --- listing and removal -------------------------------------------------


def test_list_walks_decodes_every_record(conn):
    start_walk(conn, "public", "alpha", PARAMS)
    start_walk(conn, "pro", "beta", {**PARAMS, "source": "pro"})
    listed = list_walks(conn)
    assert {walk["key"] for walk in listed} == {"public:alpha", "pro:beta"}
    assert all(isinstance(walk["params"], dict) for walk in listed)


def test_removing_a_walk_drops_its_record(conn):
    start_walk(conn, "public", "alpha", PARAMS)
    conn.execute(
        "INSERT INTO matches (match_id, source, label, status) VALUES (?,?,?,?)",
        (1, "public", "alpha", MatchStatus.DISCOVERED.value),
    )
    conn.commit()

    remove_walks(conn, ["alpha"])

    assert get_walk(conn, "alpha") is None
    assert list_walks(conn) == []


def test_removing_one_walk_spares_another(conn):
    start_walk(conn, "public", "alpha", PARAMS)
    start_walk(conn, "public", "beta", PARAMS)
    remove_walks(conn, ["alpha"])
    assert [walk["key"] for walk in list_walks(conn)] == ["public:beta"]


def test_qualified_removal_spares_the_other_source(conn):
    start_walk(conn, "public", "shared", PARAMS)
    start_walk(conn, "pro", "shared", {**PARAMS, "source": "pro"})
    remove_walks(conn, ["public:shared"])
    assert [walk["key"] for walk in list_walks(conn)] == ["pro:shared"]


def test_walk_key_matches_the_cursor_format(conn):
    start_walk(conn, "public", "alpha", PARAMS)
    assert walk_key("public", "alpha") == "public:alpha"
    assert get_walk(conn, "alpha")["key"] == walk_key("public", "alpha")


# --- range overrides -----------------------------------------------------


def test_range_params_are_exactly_pages_and_until():
    assert frozenset({"pages", "until_ts"}) == RANGE_PARAMS


def test_resuming_may_change_the_page_budget(conn):
    """--pages is a per-run budget, not a filter on which matches qualify."""
    start_walk(conn, "public", "alpha", PARAMS)
    returned = start_walk(conn, "public", "alpha", {**PARAMS, "pages": 50})
    assert returned["pages"] == 50


def test_resuming_may_change_the_until_floor(conn):
    """--until is a boundary the walk travels toward, not a property filter."""
    start_walk(conn, "public", "alpha", PARAMS)
    returned = start_walk(conn, "public", "alpha", {**PARAMS, "until_ts": 1_700_000_000})
    assert returned["until_ts"] == 1_700_000_000


def test_range_overrides_persist_to_the_record(conn):
    """Status must keep describing the walk as it actually ran."""
    start_walk(conn, "public", "alpha", PARAMS)
    start_walk(conn, "public", "alpha", {**PARAMS, "pages": 50})
    assert get_walk(conn, "alpha")["params"]["pages"] == 50


def test_overriding_a_range_leaves_filters_locked(conn):
    """The two must not leak: a filter passed beside an override is still ignored."""
    start_walk(conn, "public", "alpha", PARAMS)
    returned = start_walk(conn, "public", "alpha", {**PARAMS, "pages": 50, "min_rank": 80})
    assert returned["pages"] == 50
    assert returned["min_rank"] == 60


def test_unchanged_params_leave_the_record_alone(conn):
    """A plain resume must not rewrite the row for no reason."""
    start_walk(conn, "public", "alpha", PARAMS)
    before = get_walk(conn, "alpha")["updated_at"]
    start_walk(conn, "public", "alpha", PARAMS)
    assert get_walk(conn, "alpha")["updated_at"] == before
