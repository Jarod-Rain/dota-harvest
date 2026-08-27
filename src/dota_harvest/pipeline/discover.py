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


def _should_keep(
    row: MatchRow,
    *,
    source: str,
    min_rank: int,
    sample: float,
    ranked_only: bool,
    age_cutoff: float,
    until_ts: int | None,
) -> bool:
    """Apply every client-side filter to one match summary.

    Args:
        row: A match summary from :func:`discover_page`.
        source: Which endpoint produced the row.
        min_rank: Rank-tier floor for public matches.
        sample: Subsample fraction, in ``(0, 1]``.
        ranked_only: Drop non-ranked lobbies (public matches only).
        age_cutoff: Unix time; matches newer than this are skipped.
        until_ts: Unix time floor; matches older than this are skipped.

    Returns:
        ``True`` if the match belongs in the sampling frame.

    Note:
        The age cutoff exists because STRATZ has not indexed very recent
        matches. Keeping them would burn fetch attempts and land them in
        ``missing`` for a reason unrelated to coverage.
    """
    started = row.get("start_time")
    if started and started > age_cutoff:
        return False
    if until_ts and started and started < until_ts:
        return False
    if not _is_sampled(row["match_id"], sample):
        return False

    if source == Source.PUBLIC:
        tier = row.get("avg_rank_tier")
        if tier is None or tier < min_rank:
            return False
        if ranked_only and row.get("lobby_type") != RANKED_LOBBY_TYPE:
            return False
    return True


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
        kept = [
            row
            for row in rows
            if _should_keep(
                row,
                source=source,
                min_rank=min_rank,
                sample=sample,
                ranked_only=ranked_only,
                age_cutoff=age_cutoff,
                until_ts=until_ts,
            )
        ]
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

        # The counter arrives on every response, so the wall is visible before
        # we walk into it. Stopping here costs nothing; taking the 429 costs a
        # wasted call and leaves the counter negative.
        if left is not None and left <= reserve:
            print(f"\ndaily quota nearly spent ({left} left, reserve={reserve}).")
            print(f"re-run the same command tomorrow to resume '{key}' from {cursor:,}.")
            break

    print(f"discovered {total_new} new ids")
    return total_new
