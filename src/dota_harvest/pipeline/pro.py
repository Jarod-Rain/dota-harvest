"""Discovery by league and by team, for pro match collection.

The public-match path walks a time-ordered census backwards. Pro collection is
the opposite shape: the population is small, enumerable, and addressed by
tournament or roster rather than by time. OpenDota exposes both directly, so
there is no sampling frame to build -- you ask for a league's matches and get
all of them.

Note the age filter is deliberately absent here. `discover` skips matches
younger than ~48h because STRATZ trails OpenDota on public match ingest, but a
tournament in progress is precisely the case where you want today's games. Pull
them, and let anything STRATZ has not indexed yet land in 'missing' and be
retried on the next run.
"""

from __future__ import annotations

import sqlite3
import time
from typing import Final

from dota_harvest.api.clients import opendota_get
from dota_harvest.api.http import QuotaExhaustedError, quota_for
from dota_harvest.core.config import OPENDOTA_URL
from dota_harvest.core.manifest import connect, fmt_date
from dota_harvest.pipeline.discover import MatchRow, record_matches

SECONDS_PER_DAY: Final[int] = 86_400

#: Value recorded in the manifest's ``source`` column for every pro match.
PRO_SOURCE: Final[str] = "pro"


def find_leagues(pattern: str, limit: int = 25) -> list[MatchRow]:
    """Search leagues by name, case-insensitively.

    Args:
        pattern: Substring to look for, e.g. ``"International 2026"``.
        limit: Maximum matches to return.

    Returns:
        Matching leagues, newest league id first.
    """
    leagues = opendota_get("leagues")
    hits = [row for row in leagues if pattern.lower() in str(row.get("name", "")).lower()]
    hits.sort(key=lambda row: row.get("leagueid", 0), reverse=True)
    return hits[:limit]


def find_teams(pattern: str, limit: int = 25) -> list[MatchRow]:
    """Search teams by name, case-insensitively.

    Args:
        pattern: Substring to look for.
        limit: Maximum matches to return.

    Returns:
        Matching teams, highest rated first.
    """
    teams = opendota_get("teams")
    hits = [row for row in teams if pattern.lower() in str(row.get("name", "")).lower()]
    hits.sort(key=lambda row: row.get("rating", 0), reverse=True)
    return hits[:limit]


def league_teams(league_id: int) -> list[MatchRow]:
    """List the teams that actually appear in a league's matches.

    Args:
        league_id: OpenDota league id.

    Returns:
        One ``{"team_id", "name"}`` record per team, ordered by id. Teams whose
        name is missing fall back to their id.

    Note:
        ``/leagues/{id}/teams`` exists but has been unreliable; deriving from the
        match list is slower by one call and always agrees with the data
        actually there.
    """
    matches = opendota_get(f"leagues/{league_id}/matches")
    seen: dict[int, str] = {}
    for match in matches:
        for side in ("radiant", "dire"):
            team_id, name = match.get(f"{side}_team_id"), match.get(f"{side}_name")
            if team_id:
                seen.setdefault(int(team_id), name or str(team_id))
    return [{"team_id": team_id, "name": name} for team_id, name in sorted(seen.items())]


def _insert(
    conn: sqlite3.Connection,
    rows: list[MatchRow],
    source: str,
    label: str,
) -> int:
    """Record pro matches in the manifest, ignoring duplicates.

    Args:
        conn: Open manifest connection.
        rows: Match summaries from a league or team endpoint.
        source: Value for the ``source`` column, always ``"pro"`` here.
        label: Walk label, e.g. ``"league17119"``.

    Returns:
        The number of genuinely new ids.

    Note:
        Rows without a ``match_id`` are dropped; the tournament endpoints
        occasionally return placeholder entries for scheduled-but-unplayed
        games. ``avg_rank_tier`` is always null for pro matches, which have no
        MMR bracket.
    """
    complete = [row for row in rows if row.get("match_id")]
    return record_matches(conn, complete, source, label)


def from_league(league_id: int, label: str | None = None) -> int:
    """Collect every match in a league into the manifest.

    Args:
        league_id: OpenDota league id.
        label: Walk label; defaults to ``"league{id}"``.

    Returns:
        The number of new match ids recorded, or zero if the league has no
        matches or the quota ran out.

    Note:
        One API call regardless of match count.
    """
    conn = connect()
    label = label or f"league{league_id}"
    try:
        rows = opendota_get(f"leagues/{league_id}/matches")
    except QuotaExhaustedError as exc:
        print(exc)
        return 0

    if not rows:
        print(f"league {league_id}: no matches yet")
        return 0

    inserted = _insert(conn, rows, PRO_SOURCE, label)
    earliest = min((row.get("start_time") or 0) for row in rows)
    latest = max((row.get("start_time") or 0) for row in rows)
    print(
        f"league {league_id}: {len(rows)} matches, {inserted} new "
        f"({fmt_date(earliest)} -> {fmt_date(latest)})"
    )
    return inserted


def from_team(
    team_id: int,
    name: str = "",
    limit: int | None = None,
    since_ts: int | None = None,
    label: str | None = None,
) -> int:
    """Collect one team's match history into the manifest.

    Args:
        team_id: OpenDota team id.
        name: Display name, used only in the progress line.
        limit: Keep at most this many matches, newest first.
        since_ts: Drop matches that started before this Unix time.
        label: Walk label; defaults to ``"team{id}"``.

    Returns:
        The number of new match ids recorded, or zero if the team has no
        matches or the quota ran out.

    Note:
        ``/teams/{id}/matches`` returns the full history in one call, so ``limit``
        and ``since_ts`` filter locally rather than saving requests.
    """
    conn = connect()
    label = label or f"team{team_id}"
    try:
        rows = opendota_get(f"teams/{team_id}/matches")
    except QuotaExhaustedError as exc:
        print(exc)
        return 0

    if not rows:
        print(f"team {team_id} {name}: no matches")
        return 0

    kept = rows
    if since_ts:
        kept = [row for row in kept if (row.get("start_time") or 0) >= since_ts]
    if limit:
        kept = kept[:limit]

    inserted = _insert(conn, kept, PRO_SOURCE, label)
    print(f"team {team_id:>10} {name[:22]:<22} {len(kept):>4} matches, {inserted:>4} new")
    return inserted


def tournament_and_history(
    league_id: int, days_back: int = 365, per_team: int | None = None
) -> None:
    """Collect a tournament plus every participating team's recent history.

    Args:
        league_id: OpenDota league id.
        days_back: How far back to pull each team's history.
        per_team: Cap matches per team, newest first.

    Note:
        The tournament itself is one call; each team is one more. For a 16-team
        event that is ~18 calls total, negligible against the 3000/day budget --
        the expensive part is fetching detail from STRATZ afterwards. A quota
        failure mid-way stops cleanly, since every team collected so far is
        already committed.
    """
    print(f"=== league {league_id} ===")
    from_league(league_id)

    teams = league_teams(league_id)
    if not teams:
        print("no teams found; the league may have no completed matches yet")
        return

    since = int(time.time() - days_back * SECONDS_PER_DAY)
    print(f"\n=== {len(teams)} teams, history since {fmt_date(since)} ===")
    total = 0
    for t in teams:
        try:
            total += from_team(t["team_id"], t["name"], limit=per_team, since_ts=since)
        except QuotaExhaustedError as exc:
            print(f"\n{exc}\nRe-run to continue from here.")
            break

    # `is not None`, not truthiness: 0 calls left is the one number worth
    # printing, and `if left` is exactly when it would be suppressed.
    left = quota_for(OPENDOTA_URL).get("day")
    budget = f"; {left} API calls left today" if left is not None else ""
    print(f"\n{total} new match ids{budget}")
