"""Stage 1: build the match-id sampling frame.

Discovery runs through OpenDota rather than STRATZ because ``/publicMatches``
returns 100 ids per call with a server-side ``min_rank`` filter, making the frame
nearly free. Walking ``less_than_match_id`` backwards also gives a time-ordered
census rather than the usual leaderboard-crawl, which oversamples a few thousand
accounts and their narrow hero pools -- a bias that would be indistinguishable
from genuine draft-conditioning in the final model.
"""

from __future__ import annotations

import sqlite3
import time
from collections import Counter
from enum import StrEnum
from typing import Any, Final

from dota_harvest.api.clients import opendota_get
from dota_harvest.api.http import QuotaExhaustedError, quota_for
from dota_harvest.core.config import MIN_MATCH_AGE_HOURS, OPENDOTA_URL
from dota_harvest.core.manifest import connect, fmt_date, get_cursor, set_cursor
from dota_harvest.core.types import JSONMapping

#: One match summary as returned by OpenDota's discovery endpoints.
MatchRow = JSONMapping


class Source(StrEnum):
    """Which OpenDota endpoint a walk draws from."""

    PUBLIC = "public"
    PRO = "pro"


#: Endpoint backing each source.
_ENDPOINTS: Final[dict[Source, str]] = {
    Source.PUBLIC: "publicMatches",
    Source.PRO: "proMatches",
}

#: OpenDota's lobby type for ranked matchmaking.
RANKED_LOBBY_TYPE: Final[int] = 7

#: Modulus for the deterministic subsample. Match ids are spread finely enough
#: that the low four digits give ~0.01% granularity.
SAMPLE_MODULUS: Final[int] = 10_000

SECONDS_PER_HOUR: Final[int] = 3600

#: Reason string for a match STRATZ has not had time to index. Named because the
#: walk special-cases it: a page dropped entirely for this reason means the walk
#: simply has not travelled far enough back yet.
TOO_NEW: Final[str] = "too new"

#: Probes the seek may spend locating the collectable window. Interpolation
#: converges in three or four; the cap only bounds a pathological case.
SEEK_MAX_PROBES: Final[int] = 6

#: Stop seeking once the entry page's newest match is within this many hours
#: below the age cutoff. Tighter than this wastes probes for no extra coverage,
#: since a page only spans ~18 minutes.
SEEK_TOLERANCE_HOURS: Final[float] = 1.0

#: Fallback id-per-second rate if a page is too uniform to measure one. Match
#: ids climb at roughly this rate; see DATA.md.
DEFAULT_ID_RATE: Final[float] = 30.0

_INSERT_SQL: Final[str] = (
    "INSERT OR IGNORE INTO matches "
    "(match_id, source, label, start_time, avg_rank_tier, lobby_type, game_mode, discovered_at) "
    "VALUES (?,?,?,?,?,?,?,?)"
)


def discover_page(source: str, less_than: int | None, min_rank: int) -> list[MatchRow]:
    """Fetch one page of match summaries from OpenDota.

    Args:
        source: Either ``"public"`` or ``"pro"``.
        less_than: Walk backwards from this match id; ``None`` starts at newest.
        min_rank: Server-side rank floor. Applies to public matches only.

    Returns:
        Up to 100 match summaries, newest first. An empty list means the walk
        has reached the end of the archive.
    """
    endpoint = _ENDPOINTS[Source(source)]
    params: dict[str, Any] = {}
    if less_than is not None:
        params["less_than_match_id"] = less_than
    if source == Source.PUBLIC:
        params["min_rank"] = min_rank
    return opendota_get(endpoint, params)


def _is_sampled(match_id: int, sample: float) -> bool:
    """Decide whether a match id falls in the subsample.

    Args:
        match_id: The id to test.
        sample: Fraction to keep, in ``(0, 1]``.

    Returns:
        ``True`` if the id should be kept.

    Note:
        Keyed on the id rather than ``random()`` so a re-run over the same range
        keeps the same matches, which makes a partial collection reproducible.
    """
    if sample >= 1.0:
        return True
    return (match_id % SAMPLE_MODULUS) < sample * SAMPLE_MODULUS


def _drop_reason(
    row: MatchRow,
    *,
    source: str,
    min_rank: int,
    sample: float,
    ranked_only: bool,
    age_cutoff: float,
    until_ts: int | None,
) -> str | None:
    """Explain why a match summary was excluded, if it was.

    Args:
        row: A match summary from :func:`discover_page`.
        source: Which endpoint produced the row.
        min_rank: Rank-tier floor for public matches.
        sample: Subsample fraction, in ``(0, 1]``.
        ranked_only: Drop non-ranked lobbies (public matches only).
        age_cutoff: Unix time; matches newer than this are skipped.
        until_ts: Unix time floor; matches older than this are skipped.

    Returns:
        A short reason naming the filter that excluded the match, e.g.
        :data:`TOO_NEW` or ``"not ranked"``, or ``None`` when the match belongs
        in the sampling frame.

    Note:
        Returning the reason rather than a bool is what lets a walk report why a
        page kept nothing. A page that drops all 100 rows for a single reason is
        the normal signature of a misconfigured walk, and it used to be
        indistinguishable from an empty archive.
    """
    started = row.get("start_time")
    if started and started > age_cutoff:
        return TOO_NEW
    if until_ts and started and started < until_ts:
        return "below --until"
    if not _is_sampled(row["match_id"], sample):
        return "not sampled"

    if source == Source.PUBLIC:
        tier = row.get("avg_rank_tier")
        if tier is None or tier < min_rank:
            return "below --min-rank"
        if ranked_only and row.get("lobby_type") != RANKED_LOBBY_TYPE:
            return "not ranked"
    return None


def _should_keep(row: MatchRow, **criteria: Any) -> bool:
    """Decide whether a match summary belongs in the sampling frame.

    Args:
        row: A match summary from :func:`discover_page`.
        **criteria: Passed through to :func:`_drop_reason`.

    Returns:
        ``True`` if no filter excluded the match.
    """
    return _drop_reason(row, **criteria) is None


def _id_rate(rows: list[MatchRow]) -> float:
    """Estimate how fast match ids climb, in ids per second.

    Args:
        rows: One page of match summaries.

    Returns:
        Ids per second measured across the page, or :data:`DEFAULT_ID_RATE` when
        the page spans too little time to measure.

    Note:
        This is only a seed for the seek, never a converged answer. A page is a
        rank-filtered *sample* of the id space, so it always understates the
        true rate -- measured live at ~14 ids/s within a page against ~23 ids/s
        across pages, and the gap widens as the filter gets stricter. The seek
        corrects for this by re-deriving the rate from the distance its own
        probes actually travelled; see :func:`seek_to_age_cutoff`.
    """
    times = [row["start_time"] for row in rows if row.get("start_time")]
    ids = [row["match_id"] for row in rows if row.get("start_time")]
    if len(times) < 2:
        return DEFAULT_ID_RATE
    span_seconds = max(times) - min(times)
    if span_seconds <= 0:
        return DEFAULT_ID_RATE
    return (max(ids) - min(ids)) / span_seconds


def _newest_start(rows: list[MatchRow]) -> int | None:
    """Return the newest ``start_time`` on a page, or ``None`` if unavailable."""
    times = [row["start_time"] for row in rows if row.get("start_time")]
    return max(times) if times else None


def seek_to_age_cutoff(
    source: str,
    min_rank: int,
    age_cutoff: float,
) -> tuple[int | None, int]:
    """Find a match id just below the ingest-lag cutoff, without paging there.

    Args:
        source: Which endpoint to walk.
        min_rank: Server-side rank floor, passed through to the endpoint.
        age_cutoff: Unix time; the walk wants to enter just below this.

    Returns:
        A ``(match_id, probes)`` pair. ``match_id`` is ``None`` when the newest
        matches are already old enough to keep, or when the seek could not
        converge -- in both cases the caller should start from the newest page.

    Note:
        ``/publicMatches`` starts at the present moment, but nothing younger
        than the cutoff is collectable, because STRATZ has not indexed it yet.
        A page spans only ~18 minutes, so walking to a 48h cutoff costs ~161
        pages of rows that are all discarded. Match ids climb at a steady rate,
        so interpolating against that rate lands in the window in a few probes
        instead -- the technique
        :func:`~dota_harvest.diagnostics._find_near` uses, specialised here to
        the discovery endpoints.

        The rate is re-derived from how far each probe actually travelled rather
        than from the page contents. A rank-filtered page samples the id space
        sparsely and so understates the true rate; seeding from it and never
        correcting makes every step undershoot, which converges geometrically
        and runs out of probes short of the target.
    """
    rows = discover_page(source, None, min_rank)
    if not rows:
        return None, 1

    newest = _newest_start(rows)
    if newest is None or newest <= age_cutoff:
        # Already collectable; nothing to skip.
        return None, 1

    tolerance = SEEK_TOLERANCE_HOURS * SECONDS_PER_HOUR
    anchor_id = int(max(row["match_id"] for row in rows))
    anchor_ts = newest
    rate = _id_rate(rows)
    guess = anchor_id
    probes = 1

    for _ in range(SEEK_MAX_PROBES):
        guess = int(guess - (newest - age_cutoff) * rate)
        if guess <= 0:
            return None, probes
        probed = discover_page(source, guess, min_rank)
        probes += 1
        if not probed:
            return None, probes

        newest = _newest_start(probed)
        if newest is None:
            return None, probes
        # Land below the cutoff, close enough that little of the window is lost.
        if 0 <= age_cutoff - newest <= tolerance:
            return guess, probes

        # Secant step: ids and seconds actually traversed between the anchor and
        # this probe give the real rate over the range being searched, which the
        # page contents systematically underestimate.
        travelled_ids = anchor_id - guess
        travelled_seconds = anchor_ts - newest
        if travelled_ids > 0 and travelled_seconds > 0:
            rate = travelled_ids / travelled_seconds
        anchor_id, anchor_ts = guess, newest

    return None, probes


def record_matches(conn: sqlite3.Connection, rows: list[MatchRow], source: str, label: str) -> int:
    """Insert discovered matches into the manifest, ignoring duplicates.

    Args:
        conn: Open manifest connection.
        rows: Matches that passed :func:`_should_keep`.
        source: Which endpoint produced them.
        label: Walk label, recorded so each walk's coverage stays separable.

    Returns:
        The number of genuinely new ids, never negative.
    """
    now = int(time.time())
    inserted = conn.executemany(
        _INSERT_SQL,
        [
            (
                row["match_id"],
                source,
                label,
                row.get("start_time"),
                row.get("avg_rank_tier"),
                row.get("lobby_type"),
                row.get("game_mode"),
                now,
            )
            for row in rows
        ],
    ).rowcount
    conn.commit()
    return max(inserted, 0)


def run(
    source: str = Source.PUBLIC,
    min_rank: int = 75,
    pages: int = 20,
    start_before: int | None = None,
    label: str = "main",
    until_ts: int | None = None,
    sample: float = 1.0,
    ranked_only: bool = True,
    min_age_hours: float = MIN_MATCH_AGE_HOURS,
    reserve: int = 50,
    seek: bool = True,
) -> int:
    """Walk a discovery endpoint backwards, recording match ids in the manifest.

    Args:
        source: ``"public"`` or ``"pro"``.
        min_rank: Rank-tier floor, e.g. 75 for Divine and above.
        pages: Maximum pages to request; each yields up to 100 ids.
        start_before: Begin below this match id, overriding the saved cursor.
        label: Names this walk's cursor. Use a distinct label per backfill.
        until_ts: Stop once the walk reaches matches older than this.
        sample: Fraction of matches to keep, in ``(0, 1]``.
        ranked_only: Drop non-ranked lobbies (public matches only).
        min_age_hours: Skip matches younger than this, since STRATZ trails
            OpenDota on ingest.
        reserve: Stop with this many daily API calls unspent.
        seek: Jump straight to the collectable window on a fresh walk rather
            than paging through matches too new to keep.

    Returns:
        The count of new match ids recorded this run.

    Note:
        The cursor is committed per page, so an interrupted walk -- whether by
        quota, an empty page, or the operator -- resumes exactly where it
        stopped when the same command runs again.
    """
    conn = connect()

    # Forward collection and historical backfill are independent walks over the
    # same source. Sharing one cursor means whichever ran last dictates where
    # the other resumes -- silently, since both keep appearing to work.
    key = f"{source}:{label}"
    cursor = start_before or get_cursor(conn, key)
    age_cutoff = time.time() - min_age_hours * SECONDS_PER_HOUR

    # Seek on a fresh walk, and also on a resumed one still stranded above the
    # cutoff -- a walk that stalled there has no way to free itself otherwise,
    # since every page it fetches is too new to keep and the cursor barely
    # moves. An explicit --start-before is always honoured: that is a deliberate
    # override, and moving it would silently re-collect or skip a range.
    if seek and start_before is None and min_age_hours > 0:
        entry, probes = seek_to_age_cutoff(source, min_rank, age_cutoff)
        if entry is not None and (cursor is None or entry < cursor):
            resumed = "" if cursor is None else f" (was stalled at {cursor:,})"
            cursor = entry
            print(
                f"seeked to the {min_age_hours:g}h cutoff in {probes} calls -> {cursor:,}{resumed}"
            )
    print(f"walk '{key}' " + (f"resuming from {cursor:,}" if cursor else "from newest"))

    total_new = 0
    for page in range(pages):
        try:
            rows = discover_page(source, cursor, min_rank)
        except QuotaExhaustedError as exc:
            # The cursor is already saved from the previous page, so re-running
            # this same command tomorrow resumes exactly here.
            print(f"\n{exc}")
            print(f"stopped at page {page} with {total_new} new ids this run.")
            print(f"re-run the same command to resume walk '{key}' from {cursor:,}.")
            break

        if not rows:
            print("no more results")
            break

        # max() is the newest match on the page; if even that predates the
        # floor, every later page is older still.
        if until_ts and max((row.get("start_time") or 0) for row in rows) < until_ts:
            print(f"reached floor {fmt_date(until_ts)}; stopping")
            break

        age_cutoff = time.time() - min_age_hours * SECONDS_PER_HOUR
        kept: list[MatchRow] = []
        dropped: Counter[str] = Counter()
        for row in rows:
            reason = _drop_reason(
                row,
                source=source,
                min_rank=min_rank,
                sample=sample,
                ranked_only=ranked_only,
                age_cutoff=age_cutoff,
                until_ts=until_ts,
            )
            if reason is None:
                kept.append(row)
            else:
                dropped[reason] += 1
        total_new += record_matches(conn, kept, source, label)

        # Advance over every row returned, not just the kept ones: the cursor
        # tracks how far the walk has travelled, which is independent of what
        # the filters accepted.
        cursor = min(row["match_id"] for row in rows)
        set_cursor(conn, key, cursor)

        oldest = min((row.get("start_time") or 0) for row in rows)
        left = quota_for(OPENDOTA_URL).get("day")
        budget = f", {left} left today" if left is not None else ""
        print(
            f"  page {page + 1}/{pages}: {len(rows)} returned, {len(kept)} kept, "
            f"at {fmt_date(oldest)}{budget}"
        )
        if dropped:
            summary = ", ".join(f"{count} {reason}" for reason, count in dropped.most_common())
            print(f"    dropped: {summary}")
        # A page that keeps nothing because everything is too new means the walk
        # has not travelled far enough back yet -- not that the archive is empty.
        if not kept and dropped[TOO_NEW] == len(rows):
            print(
                f"    note: all rows are younger than the {min_age_hours:g}h STRATZ "
                f"ingest lag, so none are collectable yet."
            )
            if not seek:
                print("    drop --no-seek to jump straight to the collectable window.")

        # The counter arrives on every response, so the wall is visible before
        # we walk into it. Stopping here costs nothing; taking the 429 costs a
        # wasted call and leaves the counter negative.
        if left is not None and left <= reserve:
            print(f"\ndaily quota nearly spent ({left} left, reserve={reserve}).")
            print(f"re-run the same command tomorrow to resume '{key}' from {cursor:,}.")
            break

    print(f"discovered {total_new} new ids")
    return total_new
