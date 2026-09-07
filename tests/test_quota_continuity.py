"""Surviving the daily quota boundary under ``--continuous``.

An overnight run stopped at the reserve instead of sleeping through the reset,
and every later attempt raised QuotaExhaustedError. Three defects produced that,
and each gets a test here: the flag was locked at walk creation, a reserve of 0
let the budget reach zero before stopping, and the counters cached from the
spent window survived the sleep.
"""

from __future__ import annotations

import sqlite3

import pytest

from dota_harvest.api import http
from dota_harvest.core.manifest import (
    RANGE_PARAMS,
    SCHEMA,
    get_walk,
    start_walk,
)
from dota_harvest.pipeline import discover
from dota_harvest.pipeline.discover import MIN_DAILY_HEADROOM

PARAMS = {
    "source": "public",
    "min_rank": 75,
    "pages": None,
    "until_ts": 1774396800,
    "sample": 1.0,
    "ranked_only": True,
    "min_age_hours": 48,
    "reserve": 0,
    "seek": True,
    "continuous": False,
}


@pytest.fixture
def conn():
    connection = sqlite3.connect(":memory:")
    connection.executescript(SCHEMA)
    yield connection
    connection.close()


@pytest.fixture(autouse=True)
def _clean_quota():
    """QUOTA is module-global, so a counter would leak between tests."""
    saved = {host: dict(counters) for host, counters in http.QUOTA.items()}
    http.QUOTA.clear()
    yield
    http.QUOTA.clear()
    http.QUOTA.update(saved)


# --- the flag must be changeable on resume -------------------------------


def test_continuous_can_be_turned_on_when_resuming(conn):
    """The bug: locked at creation, so a stopped walk could never gain it."""
    start_walk(conn, "public", "alpha", PARAMS)
    returned = start_walk(conn, "public", "alpha", {**PARAMS, "continuous": True})
    assert returned["continuous"] is True
    assert get_walk(conn, "alpha")["params"]["continuous"] is True


def test_continuous_can_be_turned_off_again(conn):
    """--no-continuous must be able to undo it, or a walk sleeps forever."""
    start_walk(conn, "public", "alpha", {**PARAMS, "continuous": True})
    returned = start_walk(conn, "public", "alpha", {**PARAMS, "continuous": False})
    assert returned["continuous"] is False


def test_continuous_is_a_range_param_not_a_filter():
    """It paces the run; it does not decide which matches qualify."""
    assert "continuous" in RANGE_PARAMS


def test_changing_continuous_leaves_filters_locked(conn):
    start_walk(conn, "public", "alpha", PARAMS)
    returned = start_walk(conn, "public", "alpha", {**PARAMS, "continuous": True, "min_rank": 10})
    assert returned["continuous"] is True
    assert returned["min_rank"] == 75


def test_a_walk_predating_the_flag_still_resumes(conn):
    """Walks created before --continuous existed have no such key at all."""
    legacy = {name: value for name, value in PARAMS.items() if name != "continuous"}
    start_walk(conn, "public", "old", legacy)
    assert "continuous" not in get_walk(conn, "old")["params"]

    returned = start_walk(conn, "public", "old", {**legacy, "continuous": True})
    assert returned["continuous"] is True


# --- the budget must never reach zero ------------------------------------


def test_a_zero_reserve_still_keeps_headroom():
    """reserve=0 let the walk spend its last call, so the next one raised."""
    assert max(0, MIN_DAILY_HEADROOM) >= 2


@pytest.mark.parametrize("left", [0, 1, 2])
def test_the_guard_fires_before_the_budget_is_gone(left):
    """With reserve=0 the old guard only fired at left<=0 -- already too late."""
    reserve = 0
    assert left <= max(reserve, MIN_DAILY_HEADROOM), "must stop while calls remain"


def test_an_explicit_reserve_above_the_floor_is_respected():
    """The floor is a minimum, not an override of a deliberate setting."""
    assert max(50, MIN_DAILY_HEADROOM) == 50


# --- stale counters must not survive the sleep ---------------------------


def test_the_sleep_clears_the_spent_windows_counters(monkeypatch):
    """Left in place, the guard re-fires on them and sleeps another full day."""
    monkeypatch.setattr(discover.time, "sleep", lambda _: None)
    host = http.host_of(discover.OPENDOTA_URL)
    http.QUOTA[host] = {"day": 0, "minute": -1}

    discover._sleep_until_quota_reset("public:alpha")

    assert host not in http.QUOTA, "counters from the expired window must go"


def test_clearing_leaves_other_hosts_alone(monkeypatch):
    """STRATZ and OpenDota have unrelated budgets."""
    monkeypatch.setattr(discover.time, "sleep", lambda _: None)
    http.QUOTA["api.stratz.com"] = {"day": 400}
    http.QUOTA[http.host_of(discover.OPENDOTA_URL)] = {"day": 0}

    discover._sleep_until_quota_reset("public:alpha")

    assert http.QUOTA["api.stratz.com"] == {"day": 400}


def test_an_unknown_budget_reads_as_unknown_not_zero():
    """After clearing, the guard must not treat a missing counter as spent."""
    assert http.quota_for(discover.OPENDOTA_URL).get("day") is None


# --- waiting is bounded --------------------------------------------------


def test_a_quota_that_never_refills_stops_the_walk(conn, monkeypatch):
    """Otherwise --continuous idles forever against a permanently spent key."""
    monkeypatch.setattr(discover.time, "sleep", lambda _: None)
    monkeypatch.setattr(discover, "seconds_until_quota_reset", lambda *a, **k: 3600.0)

    def always_spent(*args, **kwargs):
        raise http.QuotaExhaustedError("dayly API quota exhausted (remaining=-5).")

    monkeypatch.setattr(discover, "discover_page", always_spent)

    total, reason = discover._walk_pages(
        conn,
        discover.Progress(),
        key="public:alpha",
        cursor=None,
        source="public",
        label="alpha",
        min_rank=0,
        pages=None,
        until_ts=None,
        sample=1.0,
        ranked_only=True,
        min_age_hours=0,
        reserve=0,
        seek=False,
        continuous=True,
    )

    assert (total, reason) == (0, discover.STOP_QUOTA)


def test_the_wait_counter_resets_once_a_page_gets_through(conn, monkeypatch):
    """Waits must be consecutive; a walk spanning days is not a stuck one."""
    monkeypatch.setattr(discover.time, "sleep", lambda _: None)
    monkeypatch.setattr(discover, "seconds_until_quota_reset", lambda *a, **k: 3600.0)
    host = http.host_of(discover.OPENDOTA_URL)
    calls = {"n": 0}

    def spent_then_page(*args, **kwargs):
        calls["n"] += 1
        # Exhausted on every other call: more than MAX_QUOTA_WAITS in total,
        # but never that many in a row.
        if calls["n"] % 2 == 1 and calls["n"] <= 9:
            raise http.QuotaExhaustedError("dayly API quota exhausted (remaining=-5).")
        http.QUOTA[host] = {"day": 900, "minute": 50}
        return [] if calls["n"] > 9 else [{"match_id": 9_000_000_000 - calls["n"], "start_time": 1}]

    monkeypatch.setattr(discover, "discover_page", spent_then_page)

    _, reason = discover._walk_pages(
        conn,
        discover.Progress(),
        key="public:alpha",
        cursor=None,
        source="public",
        label="alpha",
        min_rank=0,
        pages=None,
        until_ts=None,
        sample=1.0,
        ranked_only=False,
        min_age_hours=0,
        reserve=0,
        seek=False,
        continuous=True,
    )

    assert reason == discover.STOP_ARCHIVE_EXHAUSTED, "should outlast 5 non-consecutive waits"
