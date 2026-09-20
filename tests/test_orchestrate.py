"""The combined pipeline: stage order, budget arithmetic, and resumability.

The whole point of the command is that a walk joins at whatever stage it left
off, so most of these set up a manifest in one of the four possible states and
assert which stages run. The stages themselves are stubbed: their own behaviour
is covered elsewhere, and what matters here is the orchestration.
"""

from __future__ import annotations

import pytest

from dota_harvest.core import manifest as mf
from dota_harvest.core.manifest import (
    MatchStatus,
    connect,
    finish_walk,
    start_walk,
)
from dota_harvest.pipeline import orchestrate

PARAMS = {
    "source": "public",
    "min_rank": 11,
    "pages": None,
    "until_ts": None,
    "sample": 1.0,
    "ranked_only": True,
    "min_age_hours": 48,
    "reserve": 50,
    "seek": True,
    "continuous": False,
}


@pytest.fixture(autouse=True)
def manifest(tmp_path, monkeypatch):
    """An isolated manifest, so no test can touch the real corpus.

    Note:
        ``MANIFEST_PATH`` is resolved at import time from ``DOTA_DATA_DIR``, so
        setting the variable here would be too late -- every test would share
        the real manifest and collide on ``matches.match_id``. The resolved
        constant is patched instead.

        Connections are also short-lived by design: the manifest runs in
        journal mode, so one left holding a write transaction locks out the
        connection ``orchestrate.run`` opens for itself.
    """
    monkeypatch.setattr(mf, "MANIFEST_PATH", tmp_path / "manifest.sqlite")
    conn = connect()
    conn.close()
    return tmp_path


def _pending_left():
    """Count ids still awaiting detail, on a short-lived connection."""
    conn = connect()
    try:
        return conn.execute(
            "SELECT COUNT(*) FROM matches WHERE status = ?", (MatchStatus.DISCOVERED.value,)
        ).fetchone()[0]
    finally:
        conn.close()


@pytest.fixture
def stages(monkeypatch):
    """Stub the three stages and record how often each ran."""
    calls = {"discover": 0, "fetch": 0, "transform": 0}

    def fake_discover(**kwargs):  # noqa: ARG001
        calls["discover"] += 1
        return 0

    def fake_fetch(**kwargs):  # noqa: ARG001
        calls["fetch"] += 1
        return 0

    def fake_transform(*args, **kwargs):  # noqa: ARG001
        calls["transform"] += 1

    monkeypatch.setattr(orchestrate.discover, "run", fake_discover)
    monkeypatch.setattr(orchestrate.fetch, "run", fake_fetch)
    monkeypatch.setattr(orchestrate.transform, "run", fake_transform)
    return calls


def _close(reason=mf.STOP_REACHED_FLOOR):
    """Register a walk and finish it, then release the connection."""
    conn = connect()
    try:
        start_walk(conn, "public", "s", PARAMS)
        finish_walk(conn, "public", "s", reason, 9)
    finally:
        conn.close()


def _open_partway(reason=mf.STOP_PAGE_BUDGET, pages=3):
    """Leave the walk OPEN, as a page-budget or quota stop would."""
    conn = connect()
    try:
        start_walk(conn, "public", "s", PARAMS)
        finish_walk(conn, "public", "s", reason, pages)
    finally:
        conn.close()


def _add_pending(count):
    conn = connect()
    try:
        conn.executemany(
            "INSERT INTO matches (match_id, source, label, status) VALUES (?,?,?,?)",
            [(i, "public", "s", MatchStatus.DISCOVERED.value) for i in range(count)],
        )
        conn.commit()
    finally:
        conn.close()


# --- budget arithmetic ---------------------------------------------------


def test_the_daily_budget_is_what_stratz_can_actually_fetch():
    """Discovery is capped by the fetcher, which is the real bottleneck."""
    assert orchestrate.DAILY_MATCH_BUDGET == orchestrate.STRATZ_DAILY_CALLS * 6
    assert orchestrate.DAILY_MATCH_BUDGET == 90_000


def test_the_slice_count_stays_within_the_budget():
    """Five alternations a day, and they must not exceed what a day can fetch.

    They no longer fill it exactly: the slice is sized below the ceiling on
    purpose, so probes and retries have somewhere to come from.
    """
    assert orchestrate.MAX_SLICES == 5
    assert orchestrate.SLICE_MATCHES * orchestrate.MAX_SLICES <= orchestrate.DAILY_MATCH_BUDGET


def test_a_slice_gets_enough_pages_to_reach_its_target():
    """A page yields ~100 ids of which half survive, so this must overshoot."""
    assert orchestrate._slice_pages(18_000) * 100 > 18_000


def test_a_tiny_slice_still_gets_one_page():
    assert orchestrate._slice_pages(1) == 1


# --- joining at any stage ------------------------------------------------


def test_a_brand_new_walk_runs_every_stage(stages):
    """No walk row at all: the first slice creates it."""
    orchestrate.discover.run = lambda **k: 10  # noqa: ARG005
    orchestrate.fetch.run = lambda **k: 10  # noqa: ARG005
    orchestrate.run(label="s", slice_size=100, daily_budget=100)
    assert stages["transform"] == 1


def test_an_open_walk_discovers_before_fetching(stages):
    """A part-discovered walk must keep discovering, not skip to fetch."""
    _open_partway()

    orchestrate.run(label="s", slice_size=100, daily_budget=100)

    assert stages["discover"] == 1
    assert stages["fetch"] == 1


def test_a_closed_walk_skips_discovery_but_still_fetches(stages):
    """The backlog case: discovery is done, detail is not."""
    _close()
    _add_pending(50)

    orchestrate.run(label="s", slice_size=50, daily_budget=100)

    assert stages["discover"] == 0
    assert stages["fetch"] >= 1


def test_a_finished_drained_walk_does_nothing(stages):
    """Nothing to do must cost no API calls and no 30-minute rebuild."""
    _close()

    totals = orchestrate.run(label="s", slice_size=100, daily_budget=100)

    assert stages == {"discover": 0, "fetch": 0, "transform": 0}
    assert totals.stopped == "walk complete and drained"


# --- stopping conditions -------------------------------------------------


def test_a_stalled_fetch_stops_instead_of_hammering(stages):
    """Pending work plus zero progress means the quota is gone."""
    _close()
    _add_pending(500)

    totals = orchestrate.run(label="s", slice_size=50, daily_budget=1000)

    assert totals.slices == 1, "must not burn every slice on a spent quota"
    assert "no progress" in totals.stopped


def test_the_backlog_survives_a_stalled_run(stages):
    """Ids must stay pending so the next invocation picks them up."""
    _close()
    _add_pending(80)

    orchestrate.run(label="s", slice_size=50, daily_budget=1000)

    assert _pending_left() == 80


def test_the_discovery_budget_bounds_the_run(stages):
    """Discovering past what the fetcher can consume only inflates the backlog."""
    orchestrate.discover.run = lambda **k: 10_000  # noqa: ARG005
    orchestrate.fetch.run = lambda **k: 1  # noqa: ARG005

    totals = orchestrate.run(label="s", slice_size=5_000, daily_budget=10_000)

    assert totals.discovered >= 10_000
    assert "budget spent" in totals.stopped


# --- transform gating ----------------------------------------------------


def test_transform_is_skipped_when_nothing_was_fetched(stages):
    """A rebuild with no new data produces identical tables for ~30 minutes."""
    _close()
    _add_pending(10)

    orchestrate.run(label="s", slice_size=10, daily_budget=10)

    assert stages["transform"] == 0


def test_transform_runs_when_detail_landed(stages):
    _close()
    _add_pending(10)
    orchestrate.fetch.run = lambda **k: 5  # noqa: ARG005

    totals = orchestrate.run(label="s", slice_size=10, daily_budget=10)

    assert totals.transformed is True


def test_no_transform_wins_even_with_new_data(stages):
    """Discovery/fetch-only runs are useful when batching several walks."""
    _close()
    _add_pending(10)
    orchestrate.fetch.run = lambda **k: 5  # noqa: ARG005

    totals = orchestrate.run(label="s", slice_size=10, daily_budget=10, do_transform=False)

    assert totals.transformed is False
    assert stages["transform"] == 0


# --- failure isolation ---------------------------------------------------


def test_a_failed_discovery_slice_does_not_lose_the_fetch(stages):
    """The backlog is still worth draining when discovery breaks."""
    _open_partway(pages=1)
    _add_pending(10)

    def boom(**kwargs):  # noqa: ARG001
        raise RuntimeError("opendota is down")

    orchestrate.discover.run = boom

    orchestrate.run(label="s", slice_size=10, daily_budget=10)

    assert stages["fetch"] == 1


def test_an_interrupt_is_not_swallowed(stages):
    """Ctrl-C must stop the pipeline, not be treated as a failed slice."""
    _open_partway(pages=1)

    def interrupt(**kwargs):  # noqa: ARG001
        raise KeyboardInterrupt

    orchestrate.discover.run = interrupt

    with pytest.raises(KeyboardInterrupt):
        orchestrate.run(label="s", slice_size=10, daily_budget=10)


# --- continuous: sleeping through the reset ------------------------------


@pytest.fixture
def sleepless(monkeypatch):
    """Record sleeps instead of taking them, and report the quota as spent."""
    slept: list[float] = []
    monkeypatch.setattr(orchestrate.time, "sleep", slept.append)
    monkeypatch.setattr(orchestrate, "_spent_apis", lambda: ["STRATZ"])
    return slept


def test_a_single_day_run_never_sleeps(stages, sleepless):
    """Without --continuous the run must stop at the wall, not wait at it."""
    _close()
    _add_pending(100)

    orchestrate.run(label="s", slice_size=50, daily_budget=100)

    assert sleepless == []


def test_transform_runs_before_the_sleep(stages, sleepless, monkeypatch):
    """The sleep is hours long and Ctrl-C through it is expected.

    Anything fetched but not yet written to Parquet would otherwise sit in raw
    until the next successful run, so a day's work must be made durable first.
    """
    order: list[str] = []
    _close()
    _add_pending(100)
    day = {"n": 0}

    def fetch_then_stall(limit, batch, sleep, walks, **kwargs):  # noqa: ARG001
        """Day one collects some detail, then the budget runs out.

        Rows are left pending on purpose: that is what makes the run stop with
        work outstanding, which is the only state a sleep is reachable from.
        """
        day["n"] += 1
        if day["n"] == 1:
            order.append("fetched")
            return 10
        if day["n"] == 2:
            return 0
        # After the wait, drain everything so the loop can terminate.
        conn = connect()
        try:
            conn.execute("UPDATE matches SET status = 'fetched'")
            conn.commit()
        finally:
            conn.close()
        return 90

    monkeypatch.setattr(orchestrate.fetch, "run", fetch_then_stall)
    monkeypatch.setattr(orchestrate.transform, "run", lambda *a, **k: order.append("transform"))
    monkeypatch.setattr(orchestrate.time, "sleep", lambda _: order.append("sleep"))

    orchestrate.run(label="s", slice_size=50, daily_budget=100, continuous=True)

    assert "transform" in order and "sleep" in order
    assert order.index("transform") < order.index("sleep")


def test_continuous_stops_once_the_walk_is_drained(stages, sleepless, monkeypatch):
    """The exit condition, so an unattended run is not immortal."""
    _close()

    totals = orchestrate.run(label="s", slice_size=50, daily_budget=50, continuous=True)

    assert totals.stopped == "walk complete and drained"
    assert sleepless == []


def test_the_sleep_targets_the_next_quota_reset(monkeypatch):
    """Reuses discover's UTC-midnight arithmetic rather than a fixed delay."""
    slept: list[float] = []
    monkeypatch.setattr(orchestrate.time, "sleep", slept.append)
    monkeypatch.setattr(orchestrate.discover, "seconds_until_quota_reset", lambda: 1234.0)

    orchestrate._sleep_until_quota_returns(["STRATZ"])

    assert slept == [1234.0]


def test_spent_apis_treats_an_absent_counter_as_unknown(monkeypatch):
    """An API that has not replied yet is not out of budget."""
    monkeypatch.setattr(orchestrate, "quota_for", lambda url: {})  # noqa: ARG005
    assert orchestrate._spent_apis() == []


def test_spent_apis_names_an_exhausted_budget(monkeypatch):
    """OpenDota's daily counter goes negative once exceeded."""
    monkeypatch.setattr(orchestrate, "quota_for", lambda url: {"day": -5})  # noqa: ARG005
    assert "STRATZ" in orchestrate._spent_apis()


def test_the_sleep_line_distinguishes_a_refusal_from_our_own_cap(monkeypatch, capsys):
    """'daily daily budget spent' was the tell: the two causes read alike."""
    monkeypatch.setattr(orchestrate.time, "sleep", lambda _: None)
    monkeypatch.setattr(orchestrate.discover, "seconds_until_quota_reset", lambda: 60.0)

    orchestrate._sleep_until_quota_returns([])
    assert "daily budget reached" in capsys.readouterr().out

    orchestrate._sleep_until_quota_returns(["STRATZ"])
    assert "STRATZ daily budget spent" in capsys.readouterr().out


# --- fetch takes priority over discovery ---------------------------------


@pytest.fixture
def order(monkeypatch):
    """Record the stage sequence, with a fetcher that really drains rows."""
    seen: list[str] = []

    def draining_fetch(limit, batch, sleep, walks, **kwargs):  # noqa: ARG001
        conn = connect()
        try:
            ids = [
                row[0]
                for row in conn.execute(
                    "SELECT match_id FROM matches WHERE status = ? LIMIT ?",
                    (MatchStatus.DISCOVERED.value, limit),
                ).fetchall()
            ]
            conn.executemany(
                "UPDATE matches SET status = 'fetched' WHERE match_id = ?", [(i,) for i in ids]
            )
            conn.commit()
        finally:
            conn.close()
        seen.append(f"fetch({len(ids)})")
        return len(ids)

    monkeypatch.setattr(orchestrate.fetch, "run", draining_fetch)
    monkeypatch.setattr(orchestrate.discover, "run", lambda **k: seen.append("discover") or 0)
    monkeypatch.setattr(orchestrate.transform, "run", lambda *a, **k: seen.append("transform"))
    return seen


def test_a_resumed_walk_with_a_backlog_fetches_first(order):
    """A backlog is OpenDota budget already spent with no detail behind it."""
    _open_partway()
    _add_pending(100)

    orchestrate.run(label="s", slice_size=50, daily_budget=250)

    assert order[0].startswith("fetch")


def test_no_discovery_happens_while_ids_stay_pending(order):
    """Discovering here would push the backlog further out of reach."""
    _open_partway()
    _add_pending(150)

    orchestrate.run(label="s", slice_size=50, daily_budget=100)

    # Each slice drains 50 of 150, so nothing is ever fully pending-free.
    assert "discover" not in order


def test_discovery_resumes_once_the_backlog_is_drained(order):
    """Priority must not become starvation: an OPEN walk still needs widening."""
    _open_partway()
    _add_pending(40)

    orchestrate.run(label="s", slice_size=50, daily_budget=100)

    assert order.index("fetch(40)") < order.index("discover")


def test_a_closed_walk_with_a_backlog_never_discovers(order):
    """Nothing left to discover, so every slice is pure fetch."""
    _close()
    _add_pending(80)

    orchestrate.run(label="s", slice_size=40, daily_budget=200)

    assert "discover" not in order


def test_an_exhausted_fetch_budget_still_transforms(order, monkeypatch):
    """The requirement: a stalled fetch must not strand the day's matches.

    Whatever landed before the refusal is in raw/ only, so skipping transform
    would leave it out of Parquet until some later run happened to succeed.
    """
    _close()
    _add_pending(100)

    calls = {"n": 0}
    real_fetch = orchestrate.fetch.run

    def stalls_after_one(**kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            return real_fetch(**kwargs)
        order.append("fetch(0)")
        return 0

    monkeypatch.setattr(orchestrate.fetch, "run", stalls_after_one)

    totals = orchestrate.run(label="s", slice_size=50, daily_budget=250)

    assert "API budget likely spent" in totals.stopped
    assert totals.transformed is True
    assert order[-1] == "transform"


# --- the fetch quota must not block below the orchestrator ----------------


def test_the_slice_leaves_headroom_under_the_daily_ceiling():
    """A run budgeted at the theoretical 90,000 hit its 429 at 87,000 written.

    Seek probes, reference tables and retried batches all draw on the same
    allowance, so spending the full ceiling guarantees the last slice dies
    mid-shard.
    """
    calls = orchestrate.MAX_SLICES * orchestrate.SLICE_MATCHES // 6
    assert calls < orchestrate.STRATZ_DAILY_CALLS
    assert orchestrate.STRATZ_DAILY_CALLS - calls >= 800


def test_the_fetch_stage_is_told_not_to_wait(monkeypatch):
    """Blocking inside the fetch is what stranded a day's matches in raw/."""
    seen = {}

    def record(**kwargs):
        seen.update(kwargs)
        return 0

    monkeypatch.setattr(orchestrate.fetch, "run", record)
    monkeypatch.setattr(orchestrate.discover, "run", lambda **k: 0)  # noqa: ARG005
    monkeypatch.setattr(orchestrate.transform, "run", lambda *a, **k: None)
    _close()
    _add_pending(10)

    orchestrate.run(label="s", slice_size=10, daily_budget=10)

    assert seen["wait_for_quota"] is False


def test_a_spent_fetch_quota_is_named_as_such(monkeypatch):
    """Name the refusal, rather than inferring it from a zero return.

    "made no progress" and "quota spent" call for the same pause but read very
    differently in a log, and only one of them is the API refusing us.
    """
    monkeypatch.setattr(orchestrate.discover, "run", lambda **k: 0)  # noqa: ARG005
    monkeypatch.setattr(orchestrate.transform, "run", lambda *a, **k: None)

    def refused(**kwargs):  # noqa: ARG001
        orchestrate.fetch.LAST.quota_spent = True
        return 0

    monkeypatch.setattr(orchestrate.fetch, "run", refused)
    _close()
    _add_pending(10)

    totals = orchestrate.run(label="s", slice_size=10, daily_budget=10)

    assert totals.stopped == "daily fetch quota spent"


def test_transform_precedes_the_sleep_when_the_quota_is_refused(monkeypatch):
    """The requirement: the shard just written must reach Parquet first."""
    order: list[str] = []
    monkeypatch.setattr(orchestrate.discover, "run", lambda **k: 0)  # noqa: ARG005
    monkeypatch.setattr(orchestrate.transform, "run", lambda *a, **k: order.append("transform"))
    monkeypatch.setattr(
        orchestrate, "_sleep_until_quota_returns", lambda spent: order.append("sleep")
    )

    calls = {"n": 0}

    def one_good_batch(**kwargs):  # noqa: ARG001
        calls["n"] += 1
        if calls["n"] == 1:
            order.append("fetch")
            return 10
        orchestrate.fetch.LAST.quota_spent = True
        return 0

    monkeypatch.setattr(orchestrate.fetch, "run", one_good_batch)
    _close()
    _add_pending(100)

    orchestrate.run(label="s", slice_size=50, daily_budget=100, continuous=True)

    assert order.index("transform") < order.index("sleep")


def test_a_quota_that_never_returns_stops_the_loop(monkeypatch):
    """The bug: this spun 256,034 times, writing and deleting a shard each pass."""
    monkeypatch.setattr(orchestrate.discover, "run", lambda **k: 0)  # noqa: ARG005
    monkeypatch.setattr(orchestrate.transform, "run", lambda *a, **k: None)
    monkeypatch.setattr(orchestrate, "_sleep_until_quota_returns", lambda spent: None)  # noqa: ARG005

    def always_refused(**kwargs):  # noqa: ARG001
        orchestrate.fetch.LAST.quota_spent = True
        return 0

    monkeypatch.setattr(orchestrate.fetch, "run", always_refused)
    _close()
    _add_pending(100)

    totals = orchestrate.run(label="s", slice_size=50, daily_budget=100, continuous=True)

    # Barren days are counted before the sleep, so the last one is never slept
    # on: there is no sense waiting out a reset whose predecessors bought
    # nothing. What matters is that it is bounded at all.
    assert totals.days == orchestrate.MAX_BARREN_WAITS - 1
    assert "no progress across" in totals.stopped


def test_progress_resets_the_barren_count(monkeypatch):
    """A walk spanning many days is not a stuck one."""
    monkeypatch.setattr(orchestrate.discover, "run", lambda **k: 0)  # noqa: ARG005
    monkeypatch.setattr(orchestrate.transform, "run", lambda *a, **k: None)
    monkeypatch.setattr(orchestrate, "_sleep_until_quota_returns", lambda spent: None)  # noqa: ARG005

    calls = {"n": 0}

    def slow_drip(limit, batch, sleep, walks, **kwargs):  # noqa: ARG001
        """One match a day: never barren, so it must outlive MAX_BARREN_WAITS."""
        calls["n"] += 1
        if calls["n"] > 8:
            conn = connect()
            try:
                conn.execute("UPDATE matches SET status = 'fetched'")
                conn.commit()
            finally:
                conn.close()
            return 1
        conn = connect()
        try:
            row = conn.execute(
                "SELECT match_id FROM matches WHERE status = ? LIMIT 1",
                (MatchStatus.DISCOVERED.value,),
            ).fetchone()
            if row:
                conn.execute("UPDATE matches SET status = 'fetched' WHERE match_id = ?", (row[0],))
                conn.commit()
        finally:
            conn.close()
        orchestrate.fetch.LAST.quota_spent = True
        return 1 if row else 0

    monkeypatch.setattr(orchestrate.fetch, "run", slow_drip)
    _close()
    _add_pending(20)

    totals = orchestrate.run(label="s", slice_size=50, daily_budget=50, continuous=True)

    assert totals.days > orchestrate.MAX_BARREN_WAITS
