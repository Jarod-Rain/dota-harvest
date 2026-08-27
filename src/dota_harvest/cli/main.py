"""Command-line entry point: ``dota-harvest <command>``.

Each ``cmd_*`` function translates parsed arguments into one pipeline call and
holds no logic of its own, so every command stays reachable as a plain function
for scripting and testing.
"""

from __future__ import annotations

import argparse
import time
from typing import Final

from dota_harvest.core.config import (
    MIN_MATCH_AGE_HOURS,
    RAW_DIR,
    STRATZ_BATCH,
    STRATZ_SLEEP,
    describe_paths,
)
from dota_harvest.core.manifest import connect, fmt_date, parse_date
from dota_harvest.diagnostics import check, probe_retention, probe_versions
from dota_harvest.pipeline import discover, fetch, pro, reference, transform

SECONDS_PER_DAY: Final[int] = 86_400

#: Bytes per gigabyte, for the raw-collection size readout.
BYTES_PER_GB: Final[float] = 1e9


def fraction(value: str) -> float:
    """Parse a sampling fraction, rejecting anything outside ``(0, 1]``.

    Args:
        value: Raw command-line text.

    Returns:
        The parsed fraction.

    Raises:
        argparse.ArgumentTypeError: If the value is not a number or falls
            outside the valid range. A percentage gets a corrective hint.

    Note:
        Rejecting rather than clamping: ``--sample 50`` reads as "50 percent",
        but ``50 < 1.0`` is false, so the subsample would silently keep
        everything -- the run looks like it worked and the data is wrong.
    """
    try:
        parsed = float(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"{value!r} is not a number") from None
    if not 0.0 < parsed <= 1.0:
        hint = f" (did you mean {parsed / 100:g}?)" if 1 < parsed <= 100 else ""
        raise argparse.ArgumentTypeError(f"must be a fraction in (0, 1], got {parsed:g}{hint}")
    return parsed


def cmd_check(args: argparse.Namespace) -> None:
    """Validate the API token, the field names, and the stored collection."""
    check(args.id)


def cmd_discover(args: argparse.Namespace) -> None:
    """Build the match-id sampling frame."""
    discover.run(
        source=args.source,
        min_rank=args.min_rank,
        pages=args.pages,
        start_before=args.start_before,
        label=args.label,
        until_ts=args.until,
        sample=args.sample,
        ranked_only=not args.all_lobbies,
        min_age_hours=args.min_age_hours,
        reserve=args.reserve,
    )


def cmd_fetch(args: argparse.Namespace) -> None:
    """Pull match detail from STRATZ into the raw landing zone."""
    fetch.run(limit=args.limit, batch=args.batch, sleep=args.sleep)


def cmd_leagues(args: argparse.Namespace) -> None:
    """Search leagues by name and print the matches as a table."""
    hits = pro.find_leagues(args.pattern)
    if not hits:
        print(f"no leagues matching {args.pattern!r}")
        return
    print(f"{'league_id':>10}  {'tier':<10} name")
    for league in hits:
        print(
            f"{league.get('leagueid', '?'):>10}  "
            f"{str(league.get('tier', '-')):<10} {league.get('name', '')}"
        )


def cmd_teams(args: argparse.Namespace) -> None:
    """Search teams by name and print the matches as a table."""
    hits = pro.find_teams(args.pattern)
    if not hits:
        print(f"no teams matching {args.pattern!r}")
        return
    print(f"{'team_id':>10}  {'rating':>7}  {'wins':>5}  name")
    for team in hits:
        print(
            f"{team.get('team_id', '?'):>10}  {team.get('rating', 0):>7.0f}  "
            f"{team.get('wins', 0):>5}  {team.get('name', '')}"
        )


def cmd_league(args: argparse.Namespace) -> None:
    """Collect a league's matches, optionally with each team's recent history."""
    if args.with_history:
        pro.tournament_and_history(args.league_id, days_back=args.days_back, per_team=args.per_team)
    else:
        pro.from_league(args.league_id)


def cmd_team(args: argparse.Namespace) -> None:
    """Collect one team's match history."""
    since = int(time.time() - args.days_back * SECONDS_PER_DAY) if args.days_back else None
    pro.from_team(args.team_id, limit=args.limit, since_ts=since)


def cmd_reference(args: argparse.Namespace) -> None:
    """Rebuild the constants, patch table, and item taxonomy."""
    reference.fetch_constants()
    print("\npatches:")
    reference.build_patches(check_only=args.check)
    if not args.check:
        print("\nitem taxonomy:")
        reference.build_item_meta()


def cmd_transform(args: argparse.Namespace) -> None:  # noqa: ARG001
    """Convert the raw landing zone into partitioned Parquet."""
    transform.run()


def cmd_paths(args: argparse.Namespace) -> None:  # noqa: ARG001
    """Print where every configured path resolved to."""
    print(describe_paths())


def cmd_status(args: argparse.Namespace) -> None:  # noqa: ARG001
    """Summarise the manifest by status, by walk, and by raw size on disk."""
    conn = connect()

    print("by status:")
    for status, count in conn.execute(
        "SELECT status, COUNT(*) FROM matches GROUP BY 1 ORDER BY 2 DESC"
    ):
        print(f"  {status:<12} {count:>10,}")

    print("\nby walk:")
    for source, label, count, earliest, latest in conn.execute(
        "SELECT source, label, COUNT(*), MIN(start_time), MAX(start_time) "
        "FROM matches GROUP BY 1,2 ORDER BY 3 DESC"
    ):
        span = f"{fmt_date(earliest)} -> {fmt_date(latest)}" if earliest else "-"
        print(f"  {source}:{label or '-':<12} {count:>10,}   {span}")

    files = list(RAW_DIR.glob("*.jsonl.gz")) if RAW_DIR.exists() else []
    total_gb = sum(path.stat().st_size for path in files) / BYTES_PER_GB
    print(f"\nraw: {total_gb:.2f} GB across {len(files)} files")


def cmd_probe(args: argparse.Namespace) -> None:
    """Run an API diagnostic probe."""
    if args.what == "versions":
        probe_versions()
    else:
        probe_retention()


def main() -> None:
    """Parse command-line arguments and dispatch to the selected command.

    Each subparser stores its handler via ``set_defaults(fn=...)``, so dispatch
    stays a single call rather than a chain of comparisons.
    """
    ap = argparse.ArgumentParser(
        prog="dota-harvest", description="Collect Dota 2 match data into patch-partitioned Parquet."
    )
    sub = ap.add_subparsers(dest="cmd", required=True)

    c = sub.add_parser("check", help="validate token and GraphQL field names")
    c.add_argument("--id", type=int, default=None)
    c.set_defaults(fn=cmd_check)

    d = sub.add_parser("discover", help="build the match-id sampling frame")
    d.add_argument("--source", choices=["public", "pro"], default="public")
    d.add_argument(
        "--min-rank",
        type=int,
        default=11,
        help="rank tier: 1x=Herald, 2x=Guardian, ... , "
        "8x=Immortal; x=star value [1-5]. Default 11 (no minimum).",
    )
    d.add_argument("--pages", type=int, default=20, help="100 ids per page")
    d.add_argument("--start-before", type=int, default=None)
    d.add_argument(
        "--label",
        default="main",
        help="names this walk's cursor; use a distinct label per backfill",
    )
    d.add_argument("--until", type=parse_date, default=None, help="stop at YYYY-MM-DD")
    d.add_argument(
        "--sample",
        type=fraction,
        default=1.0,
        help="keep this fraction (0-1]. A percentage like 50 is rejected.",
    )
    d.add_argument(
        "--all-lobbies",
        action="store_true",
        help="keep unranked lobbies too (public source only; default is ranked-only)",
    )
    d.add_argument(
        "--min-age-hours",
        type=float,
        default=MIN_MATCH_AGE_HOURS,
        help=f"skip matches younger than this; STRATZ trails OpenDota on ingest. "
        f"Default {MIN_MATCH_AGE_HOURS:g}.",
    )
    d.add_argument(
        "--reserve",
        type=int,
        default=50,
        help="stop with this many daily API calls unspent, leaving "
        "headroom for reference/probe commands",
    )
    d.set_defaults(fn=cmd_discover)

    f = sub.add_parser("fetch", help="pull match detail from STRATZ")
    f.add_argument("--limit", type=int, default=1000)
    f.add_argument("--batch", type=int, default=STRATZ_BATCH)
    f.add_argument("--sleep", type=float, default=STRATZ_SLEEP)
    f.set_defaults(fn=cmd_fetch)

    r = sub.add_parser("reference", help="constants, patch table, item taxonomy")
    r.add_argument("--check", action="store_true", help="report only, write nothing")
    r.set_defaults(fn=cmd_reference)

    t = sub.add_parser("transform", help="raw JSONL -> partitioned Parquet")
    t.set_defaults(fn=cmd_transform)

    lg = sub.add_parser("leagues", help="search leagues by name")
    lg.add_argument("pattern", help="substring, e.g. 'International 2026'")
    lg.set_defaults(fn=cmd_leagues)

    tm = sub.add_parser("teams", help="search teams by name")
    tm.add_argument("pattern")
    tm.set_defaults(fn=cmd_teams)

    lm = sub.add_parser("league", help="collect a league's matches")
    lm.add_argument("league_id", type=int)
    lm.add_argument(
        "--with-history",
        action="store_true",
        help="also collect each participating team's recent matches",
    )
    lm.add_argument("--days-back", type=int, default=365, help="history window for --with-history")
    lm.add_argument(
        "--per-team", type=int, default=None, help="cap matches per team (newest first)"
    )
    lm.set_defaults(fn=cmd_league)

    tmm = sub.add_parser("team", help="collect one team's match history")
    tmm.add_argument("team_id", type=int)
    tmm.add_argument("--days-back", type=int, default=365)
    tmm.add_argument("--limit", type=int, default=None)
    tmm.set_defaults(fn=cmd_team)

    pp = sub.add_parser("paths", help="show where everything resolved to")
    pp.set_defaults(fn=cmd_paths)

    s = sub.add_parser("status", help="manifest summary")
    s.set_defaults(fn=cmd_status)

    p = sub.add_parser("probe", help="API diagnostics")
    p.add_argument("what", choices=["versions", "retention"])
    p.set_defaults(fn=cmd_probe)

    a = ap.parse_args()
    a.fn(a)


if __name__ == "__main__":
    main()
