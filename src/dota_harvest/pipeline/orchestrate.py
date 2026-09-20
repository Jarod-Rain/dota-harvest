"""One command that carries a walk from discovery through to Parquet.

The three stages have very different costs. Discovery is nearly free: one
OpenDota call returns 100 match ids. Detail fetching is the bottleneck by two
orders of magnitude -- STRATZ allows 15,000 calls a day and each carries
:data:`~dota_harvest.core.config.STRATZ_BATCH` matches, so the whole day buys
detail for about 90,000 matches. Transform is neither, but it rebuilds every
table from the entire corpus, so it is worth doing once rather than per stage.

That asymmetry sets the shape of the loop. Discovery is capped to what a day of
fetching can actually consume, and the two alternate in slices so the pending
backlog stays bounded instead of growing to hundreds of thousands of ids that
will not be fetched for days. Transform runs last, and only if detail actually
landed.

Within a slice, fetching comes first and discovery only runs once nothing is
pending. A backlog is OpenDota budget already spent, recorded in the manifest
with no detail behind it, so draining it is worth more than widening it -- and
a resumed walk typically starts with one.

Every stage reads its own progress from the manifest rather than being told
where it is, so a walk joins the loop at whatever stage it left off: brand new,
part-discovered, fully discovered with a fetch backlog, or finished and merely
needing a transform.
"""

from __future__ import annotations

import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Final

from dota_harvest.api.http import quota_for
from dota_harvest.core.config import OPENDOTA_URL, STRATZ_BATCH, STRATZ_SLEEP, STRATZ_URL
from dota_harvest.core.manifest import (
    WalkState,
    connect,
    get_walk,
    pending_ids,
    walk_key,
)
from dota_harvest.pipeline import discover, fetch, transform

#: Matches a full day of STRATZ calls can fetch detail for: the daily call
#: allowance times the matches carried per call. Discovery is capped here
#: because ids discovered beyond it cannot be fetched today anyway, and a
#: backlog that outruns the fetcher by days only makes `status` harder to read.
STRATZ_DAILY_CALLS: Final[int] = 15_000
DAILY_MATCH_BUDGET: Final[int] = STRATZ_DAILY_CALLS * STRATZ_BATCH

#: Matches worked per slice. Five slices come to 85,000 matches -- about 14,166
#: calls -- leaving roughly 830 calls of headroom under the 15,000/day ceiling.
#:
#: The headroom is deliberate. A run at the theoretical 18,000 hit its 429 at
#: 87,000 matches written, not 90,000: the seek probes, the reference tables and
#: any retried batch all draw on the same allowance, so budgeting the full
#: ceiling guarantees the last slice dies mid-shard.
SLICE_MATCHES: Final[int] = 17_000

#: Ceiling on slices per invocation, derived rather than guessed so the two
#: constants above cannot drift apart.
MAX_SLICES: Final[int] = DAILY_MATCH_BUDGET // SLICE_MATCHES

#: Pages a discovery slice may spend. Each page returns 100 ids, of which a
#: little over half survive the filters, so this overshoots deliberately -- the
#: slice is bounded by whichever of pages or new ids runs out first.
PAGES_PER_SLICE: Final[int] = SLICE_MATCHES // 50

#: Quota resets a continuous run may wait through without collecting anything
#: before giving up. A budget still refusing after this many rollovers is not a
#: daily limit refilling, and each further wait costs a day for nothing.
MAX_BARREN_WAITS: Final[int] = 3


@dataclass
class Totals:
    """What one invocation accomplished, for the closing summary.

    Attributes:
        discovered: New match ids recorded across every discovery slice.
        fetched: Matches whose detail was written to a raw shard.
        slices: Discover/fetch alternations actually run.
        days: Quota resets slept through, so 0 for a single-day run.
        transformed: Whether the Parquet tables were rebuilt at least once.
        stopped: Why the loop ended, in the same vocabulary as ``status``.
    """

    discovered: int = 0
    fetched: int = 0
    slices: int = 0
    days: int = 0
    transformed: bool = False
    stopped: str = ""


def _walk_is_open(conn, selector: str) -> bool:
    """Report whether a walk still has ground left to discover.

    Args:
        conn: Open manifest connection.
        selector: ``"label"`` or ``"source:label"``.

    Returns:
        ``True`` while the walk is unfinished, including a walk that has never
        run. A walk absent from the manifest counts as open: the first slice
        creates it.
    """
    walk = get_walk(conn, selector)
    if walk is None:
        return True
    return walk["state"] == WalkState.OPEN


def _spent_apis() -> list[str]:
    """Name the APIs whose daily budget is gone.

    Returns:
        Hosts with a non-positive daily counter, as read from the last response
        each one sent. Empty when both still have room, or when neither has
        been called yet this process -- an absent counter means unknown, never
        zero.

    Note:
        This is the only honest signal available. ``fetch.run`` catches
        :class:`~dota_harvest.api.http.QuotaExhaustedError` itself and returns
        a count, so a zero return cannot distinguish a spent quota from an
        empty backlog; the counters can.
    """
    spent = []
    for name, url in (("STRATZ", STRATZ_URL), ("OpenDota", OPENDOTA_URL)):
        remaining = quota_for(url).get("day")
        if remaining is not None and remaining <= 0:
            spent.append(name)
    return spent


def _sleep_until_quota_returns(spent: list[str]) -> None:
    """Block until the exhausted daily budgets are expected to refill.

    Args:
        spent: Names of the APIs whose counters are exhausted. Empty when the
            run stopped for its own ``--daily-budget`` rather than a refusal,
            which is worth saying plainly -- the two look identical in a log
            otherwise, and only one of them is the API's fault.

    Note:
        Reuses discover's UTC-midnight arithmetic. STRATZ states its own reset
        in ``Retry-After``, but that value is attached to a 429 response the
        orchestrator never sees -- the transport already slept on it or the
        stage swallowed it -- so midnight plus a margin is what is actually
        available here. Overshooting costs a few idle minutes; undershooting
        wakes into another refusal.
    """
    wait = discover.seconds_until_quota_reset()
    wake = datetime.fromtimestamp(time.time() + wait, UTC)
    hours, minutes = divmod(int(wait) // 60, 60)
    why = f"{' and '.join(spent)} daily budget spent" if spent else "daily budget reached"
    print(
        f"\n{why}; sleeping {hours}h {minutes:02d}m until "
        f"{wake:%Y-%m-%d %H:%M} UTC (Ctrl-C to stop)",
        flush=True,
    )
    time.sleep(wait)


def _pending_count(conn, selector: str, ceiling: int) -> int:
    """Count ids awaiting detail, up to a ceiling.

    Args:
        conn: Open manifest connection.
        selector: Walk to restrict to.
        ceiling: Stop counting here; the caller only needs to know whether
            there is work and roughly how much.

    Returns:
        Number of pending ids, saturating at ``ceiling``.
    """
    return len(pending_ids(conn, ceiling, [selector]))


def run(
    *,
    source: str = "public",
    label: str,
    min_rank: int = 11,
    until_ts: int | None = None,
    sample: float = 1.0,
    ranked_only: bool = True,
    min_age_hours: float = 48,
    reserve: int = 50,
    seek: bool = True,
    resume: bool = False,
    batch: int = STRATZ_BATCH,
    sleep: float = STRATZ_SLEEP,
    daily_budget: int = DAILY_MATCH_BUDGET,
    slice_size: int = SLICE_MATCHES,
    do_transform: bool = True,
    continuous: bool = False,
) -> Totals:
    """Carry one walk as far as today's API budget allows.

    Alternates fetching and discovery in slices of ``slice_size`` matches until
    the walk is fully discovered and drained, the daily budget is spent, or an
    API refuses further calls. Then rebuilds the Parquet tables, but only if
    detail actually landed.

    Fetching is prioritised: a slice discovers only when nothing is pending, so
    a walk resumed with a backlog spends every slice draining it first.

    Args:
        source: Which OpenDota endpoint to walk.
        label: The walk's label. With ``resume`` this may also be
            ``"source:label"``.
        min_rank: Rank-tier floor for public matches.
        until_ts: Stop discovery once it reaches matches older than this.
        sample: Fraction of matches to keep.
        ranked_only: Drop non-ranked lobbies.
        min_age_hours: Skip matches younger than this.
        reserve: Daily OpenDota calls to leave unspent.
        seek: Jump straight to the collectable window on a fresh walk.
        resume: Treat ``label`` as a full selector, so a label used under both
            sources can be named unambiguously as ``"source:label"``. The
            stored filters are adopted either way -- ``start_walk`` locks them
            at creation -- so this only affects how the walk is addressed.
        batch: Matches per STRATZ request.
        sleep: Seconds between STRATZ requests.
        daily_budget: Matches to discover at most, across all slices.
        slice_size: Matches to discover before handing over to the fetcher.
        do_transform: Rebuild the Parquet tables when new detail landed.
        continuous: Sleep through each quota reset and keep going until the
            walk is fully discovered and drained. The day's matches are
            transformed *before* each sleep, so a long unattended run leaves
            durable Parquet at every reset rather than only at the end.

    Returns:
        A :class:`Totals` describing the invocation.

    Note:
        Each stage rereads the manifest, so this is safe to interrupt and
        re-run, and it does not care which stage a walk stopped in.
    """
    conn = connect()
    selector = label if resume else walk_key(source, label)
    totals = Totals()
    max_slices = max(daily_budget // slice_size, 1)
    unsaved = 0

    horizon = "until the walk is done" if continuous else f"up to {max_slices} slices"
    print(f"pipeline '{selector}': {horizon}, {slice_size:,} matches per slice\n")

    day = 0
    barren = 0
    while True:
        day += 1
        discovered_today = 0
        progress_before_day = totals.fetched + totals.discovered

        for index in range(max_slices):
            open_before = _walk_is_open(conn, selector)
            pending_before = _pending_count(conn, selector, slice_size)

            if not open_before and not pending_before:
                totals.stopped = "walk complete and drained"
                break
            if discovered_today >= daily_budget:
                totals.stopped = "daily discovery budget spent"
                break

            totals.slices += 1
            label_day = f"day {day} " if continuous else ""
            print(f"--- {label_day}slice {index + 1}/{max_slices} ---")

            # Fetch first, and discover only once nothing is pending. A
            # backlog is already-spent OpenDota budget sitting in the manifest
            # with no detail behind it, so draining it is strictly more
            # valuable than widening it -- and a resumed walk usually starts
            # with one. Discovering first would also push the backlog further
            # out of reach on exactly the runs that are already behind.
            if pending_before:
                print(f"fetch: {pending_before:,}+ pending, draining before discovery")
            written = fetch.run(
                limit=slice_size,
                batch=batch,
                sleep=sleep,
                walks=[selector],
                # Never block inside the fetch. A spent quota has to come back
                # here so the shard just written can be transformed before the
                # wait -- the transport's own sleep would hold it for hours
                # inside one HTTP call, with the day's matches still in raw/.
                wait_for_quota=False,
            )
            totals.fetched += written
            unsaved += written
            quota_spent = fetch.LAST.quota_spent

            still_pending = _pending_count(conn, selector, 1)
            if quota_spent or (written == 0 and still_pending):
                # Either the API said no, or it served nothing while work
                # remained; another slice would only repeat the refusal.
                totals.stopped = (
                    "daily fetch quota spent"
                    if quota_spent
                    else "fetch made no progress; API budget likely spent"
                )
                break

            if still_pending:
                # More detail to collect under the ids we already hold; spend
                # the next slice on it rather than discovering more.
                continue

            if open_before:
                found = _discover_slice(
                    source=source,
                    label=label,
                    min_rank=min_rank,
                    until_ts=until_ts,
                    sample=sample,
                    ranked_only=ranked_only,
                    min_age_hours=min_age_hours,
                    reserve=reserve,
                    seek=seek,
                    pages=_slice_pages(slice_size),
                )
                totals.discovered += found
                discovered_today += found
            else:
                print("discovery: walk already complete")
        else:
            totals.stopped = f"ran {max_slices} slices"

        if not totals.stopped:
            totals.stopped = "walk complete and drained"

        done = not _walk_is_open(conn, selector) and not _pending_count(conn, selector, 1)
        if not continuous or done:
            break

        # A wait that buys nothing must not repeat forever. Without this a
        # quota that never recovers -- a revoked key, a monthly cap, a clock
        # skew -- spins the loop, writing and deleting an empty shard each
        # time. Any progress at all resets the count.
        #
        # Counted in barren *days*, and checked before the sleep, so the loop
        # sleeps at most MAX_BARREN_WAITS - 1 times before giving up: there is
        # no sense waiting on a reset whose predecessors all bought nothing.
        barren = 0 if (totals.fetched + totals.discovered) > progress_before_day else barren + 1
        if barren >= MAX_BARREN_WAITS:
            totals.stopped = f"no progress across {barren} quota resets; stopping"
            break

        # Save the day's work before going to sleep. The sleep is hours long and
        # Ctrl-C through it is expected, so anything not written to Parquet
        # first would stay stranded in raw until the next successful run.
        if do_transform and unsaved:
            print("\n--- transform (before sleeping) ---")
            transform.run()
            totals.transformed = True
            unsaved = 0

        _sleep_until_quota_returns(_spent_apis())
        totals.days += 1
        totals.stopped = ""

    if do_transform and unsaved:
        print("\n--- transform ---")
        transform.run()
        totals.transformed = True
    elif do_transform and not totals.transformed:
        print("\nno new matches fetched; skipping transform")

    _report(totals, selector)
    return totals


def _slice_pages(slice_size: int) -> int:
    """Convert a slice's match target into a page budget.

    Args:
        slice_size: Matches the slice aims to discover.

    Returns:
        Pages to allow. A page returns 100 ids and a little over half survive
        the filters, so this deliberately overshoots: whichever of the page
        budget or the walk's own bounds runs out first ends the slice.
    """
    return max(slice_size // 50, 1)


def _discover_slice(
    *,
    source: str,
    label: str,
    min_rank: int,
    until_ts: int | None,
    sample: float,
    ranked_only: bool,
    min_age_hours: float,
    reserve: int,
    seek: bool,
    pages: int,
) -> int:
    """Run one bounded discovery slice.

    Args:
        source: Which endpoint to walk.
        label: The walk's label.
        min_rank: Rank-tier floor.
        until_ts: Discovery floor.
        sample: Fraction to keep.
        ranked_only: Drop non-ranked lobbies.
        min_age_hours: Skip matches younger than this.
        reserve: Daily calls to leave unspent.
        seek: Whether to seek to the collectable window.
        pages: Page budget for this slice.

    Returns:
        New match ids recorded.

    Note:
        Resuming needs no flag here. ``start_walk`` returns a known walk's
        stored filters and ignores whatever this passes, so slice two onwards
        -- and any run against an existing label -- discovers under the
        original parameters by construction.

        ``--continuous`` is deliberately not threaded through. A continuous
        discovery run sleeps until the quota resets, which would strand the
        fetcher for hours with a full backlog it could have been draining.
        The pipeline's own slicing is what spans the reset.
    """
    print(f"discovery: up to {pages} pages")
    try:
        return discover.run(
            source=source,
            min_rank=min_rank,
            pages=pages,
            label=label,
            until_ts=until_ts,
            sample=sample,
            ranked_only=ranked_only,
            min_age_hours=min_age_hours,
            reserve=reserve,
            seek=seek,
            continuous=False,
        )
    except KeyboardInterrupt:
        raise
    except Exception as exc:  # noqa: BLE001 - a failed slice must not lose the fetch
        print(f"  discovery slice failed: {exc}", file=sys.stderr)
        return 0


def _report(totals: Totals, selector: str) -> None:
    """Print the closing summary.

    Args:
        totals: What the invocation accomplished.
        selector: Walk identifier.
    """
    print(f"\npipeline '{selector}' finished: {totals.stopped}")
    print(f"  slices      {totals.slices}")
    if totals.days:
        print(f"  quota waits {totals.days}")
    print(f"  discovered  {totals.discovered:,} new ids")
    print(f"  fetched     {totals.fetched:,} matches")
    print(f"  transform   {'rebuilt' if totals.transformed else 'skipped'}")
