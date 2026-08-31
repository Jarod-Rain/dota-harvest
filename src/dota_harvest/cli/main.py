"""Command-line entry point: ``dota-harvest <command>``.

Each ``cmd_*`` function translates parsed arguments into one pipeline call and
holds no logic of its own, so every command stays reachable as a plain function
for scripting and testing.
"""

from __future__ import annotations

import argparse
import sys
import time
from typing import Final

from dota_harvest.core.config import (
    MIN_MATCH_AGE_HOURS,
    RAW_DIR,
    STRATZ_BATCH,
    STRATZ_SLEEP,
    describe_paths,
)
from dota_harvest.core.manifest import (
    WalkState,
    connect,
    fmt_date,
    get_walk,
    list_walks,
    parse_date,
    parse_walk,
    pending_ids,
    remove_walks,
)
from dota_harvest.diagnostics import check, probe_retention, probe_versions
from dota_harvest.pipeline import discover, fetch, pro, reference, transform

SECONDS_PER_DAY: Final[int] = 86_400

#: Bytes per gigabyte, for the raw-collection size readout.
BYTES_PER_GB: Final[float] = 1e9

#: Pages a discovery run requests when --pages is not given.
DEFAULT_PAGES: Final[int] = 3000


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


#: Flags that decide which matches a walk keeps. Passing any of these alongside
#: --resume is refused: they are locked when the walk is created, and changing
#: one would blend a differently-filtered population under the same label.
TUNING_FLAGS: Final[tuple[str, ...]] = (
    "--source",
    "--min-rank",
    "--start-before",
    "--label",
    "--sample",
    "--all-lobbies",
    "--min-age-hours",
    "--no-seek",
    "--reserve",
)

#: Flags a resume may override. These bound how far a run travels rather than
#: which matches qualify, so changing one extends or shortens the walk without
#: making its rows heterogeneous.
RESUMABLE_OVERRIDES: Final[tuple[str, ...]] = ("--pages", "--until")


def cmd_discover(args: argparse.Namespace) -> None:
    """Build the match-id sampling frame, or resume an unfinished walk."""
    if args.resume:
        _resume_walk(args.resume, pages=args.pages, until_ts=args.until)
        return

    discover.run(
        source=args.source,
        min_rank=args.min_rank,
        pages=DEFAULT_PAGES if args.pages is None else args.pages,
        start_before=args.start_before,
        label=args.label,
        until_ts=args.until,
        sample=args.sample,
        ranked_only=not args.all_lobbies,
        min_age_hours=args.min_age_hours,
        reserve=args.reserve,
        seek=not args.no_seek,
    )


def _resume_walk(selector: str, pages: int | None = None, until_ts: int | None = None) -> None:
    """Continue an unfinished walk, optionally widening how far it runs.

    Args:
        selector: ``"label"`` or ``"source:label"`` naming the walk.
        pages: Page budget for this run, overriding the stored one.
        until_ts: Floor for this run, overriding the stored one.

    Raises:
        SystemExit: If no walk matches, the label is ambiguous across sources,
            or the walk has already finished.

    Note:
        Every *filter* comes from the stored record; only the two range bounds
        may be overridden. ``--pages`` is a per-run budget and ``--until`` is a
        boundary the walk travels toward, so neither changes whether a given
        match qualifies -- unlike ``--min-rank`` or ``--sample``, which would
        leave the label holding two differently-filtered populations.
    """
    conn = connect()
    try:
        walk = get_walk(conn, selector)
    except ValueError as exc:
        sys.exit(f"error: {exc}")

    if walk is None:
        _print_resumable(conn, f"no walk named {selector!r}")
        raise SystemExit(1)

    if walk["state"] == WalkState.DONE:
        print(f"walk '{walk['key']}' already finished ({walk['reason']}).")
        print("Nothing left to collect under its parameters.")
        if until_ts is not None and _extends_floor(walk["params"]["until_ts"], until_ts):  # pyright: ignore[reportIndexIssue]
            print(f"To carry it further back, start a new walk with --until {fmt_date(until_ts)}.")
        return

    params = walk["params"]
    print(f"resuming '{walk['key']}' ({walk['pages_done']} pages so far)")
    print(f"  stopped: {walk['reason'] or 'not yet run'}")
    print(f"  {_format_params(params)}")  # pyright: ignore[reportArgumentType]

    run_pages = params["pages"] if pages is None else pages  # pyright: ignore[reportIndexIssue]
    run_until = params["until_ts"] if until_ts is None else until_ts  # pyright: ignore[reportIndexIssue]
    for name, stored, supplied in (
        ("pages", params["pages"], run_pages),  # pyright: ignore[reportIndexIssue]
        ("until", params["until_ts"], run_until),  # pyright: ignore[reportIndexIssue]
    ):
        if supplied != stored:
            shown_old = fmt_date(stored) if name == "until" and stored else stored
            shown_new = fmt_date(supplied) if name == "until" and supplied else supplied
            print(f"  override: {name} {shown_old or '-'} -> {shown_new or '-'}")

    # Raising the floor cannot un-collect what the walk already has, so the
    # rows below the new floor stay. Say so rather than implying a clean trim.
    if until_ts is not None and not _extends_floor(params["until_ts"], until_ts):  # pyright: ignore[reportIndexIssue]
        print(
            "  note: raising --until only stops this run earlier; matches already "
            "collected below it remain in the walk."
        )
    print()

    discover.run(
        source=params["source"],  # pyright: ignore[reportIndexIssue]
        min_rank=params["min_rank"],  # pyright: ignore[reportIndexIssue]
        pages=run_pages,  # pyright: ignore[reportIndexIssue]
        label=walk["label"],  # pyright: ignore[reportArgumentType]
        until_ts=run_until,
        sample=params["sample"],  # pyright: ignore[reportIndexIssue]
        ranked_only=params["ranked_only"],  # pyright: ignore[reportIndexIssue]
        min_age_hours=params["min_age_hours"],  # pyright: ignore[reportIndexIssue]
        reserve=params["reserve"],  # pyright: ignore[reportIndexIssue]
        seek=params["seek"],  # pyright: ignore[reportIndexIssue]
    )


def _extends_floor(stored: int | None, supplied: int | None) -> bool:
    """Report whether a new ``--until`` carries the walk further back in time.

    Args:
        stored: The walk's recorded floor, if any.
        supplied: The floor requested for this run.

    Returns:
        ``True`` when the new floor reaches further back, so the walk gains
        ground. Dropping the floor entirely also extends it.
    """
    if supplied is None:
        return stored is not None
    if stored is None:
        return False
    return supplied < stored


def _format_params(params: dict[str, object]) -> str:
    """Render a walk's locked parameters as a single readable line.

    Args:
        params: The stored parameter mapping.

    Returns:
        A compact ``key=value`` summary, with the ``--until`` floor shown as a
        date rather than a raw timestamp.
    """
    until = params.get("until_ts")
    parts = [
        f"source={params['source']}",
        f"min_rank={params['min_rank']}",
        f"pages={params['pages']}",
        f"until={fmt_date(until) if until else '-'}",  # pyright: ignore[reportArgumentType]
        f"sample={params['sample']:g}",
        f"ranked_only={params['ranked_only']}",
        f"min_age_hours={params['min_age_hours']:g}",
        f"reserve={params['reserve']}",
    ]
    return "  ".join(parts)


def _print_resumable(conn, headline: str) -> None:
    """Print an error headline followed by the walks that can be resumed."""
    print(headline)
    open_walks = [walk for walk in list_walks(conn) if walk["state"] == WalkState.OPEN]
    if not open_walks:
        print("  no unfinished walks to resume")
        return
    print("  unfinished walks:")
    for walk in open_walks:
        print(f"    {walk['key']:<24} {walk['reason'] or 'not yet run'}")


def walk_list(value: str) -> list[str]:
    """Parse a comma-separated list of walk selectors.

    Args:
        value: Raw command-line text, e.g. ``"recent-2d,public:patch-741"``.

    Returns:
        The individual selectors, whitespace stripped.

    Raises:
        argparse.ArgumentTypeError: If the list is empty or any selector is
            malformed. Failing here rather than silently matching nothing keeps
            a typo from looking like a fully-collected walk.
    """
    selectors = [part.strip() for part in value.split(",") if part.strip()]
    if not selectors:
        raise argparse.ArgumentTypeError("expected at least one walk")
    for selector in selectors:
        try:
            parse_walk(selector)
        except ValueError as exc:
            raise argparse.ArgumentTypeError(str(exc)) from None
    return selectors


def cmd_fetch(args: argparse.Namespace) -> None:
    """Pull match detail from STRATZ into the raw landing zone."""
    fetch.run(limit=args.limit, batch=args.batch, sleep=args.sleep, walks=args.walks)


def cmd_remove(args: argparse.Namespace) -> None:
    """Delete a walk's pending matches and its discovery cursor."""
    conn = connect()

    pending = len(pending_ids(conn, 10**9, args.walks))
    tally = remove_walks(conn, args.walks, include_fetched=args.all)

    deleted = tally.pop("deleted", 0)
    kept = tally.pop("kept_fetched", 0)
    cursors = tally.pop("cursors", 0)

    print(f"removing {', '.join(args.walks)}")
    for status, count in sorted(tally.items()):
        print(f"  {status:<12} {count:>8,} deleted")
    if kept:
        print(f"  {'fetched':<12} {kept:>8,} KEPT (data is in raw/; use --all to delete)")
    print(f"  {'cursors':<12} {cursors:>8,} dropped")
    print(f"\n{deleted:,} rows removed ({pending:,} were still pending)")


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

    counts = {
        f"{source}:{label}": (count, earliest, latest)
        for source, label, count, earliest, latest in conn.execute(
            "SELECT source, label, COUNT(*), MIN(start_time), MAX(start_time) "
            "FROM matches GROUP BY 1,2 ORDER BY 3 DESC"
        )
    }
    registered = {walk["key"]: walk for walk in list_walks(conn)}

    print("\nby walk:")
    for key in [*counts, *(k for k in registered if k not in counts)]:
        count, earliest, latest = counts.get(key, (0, None, None))
        span = f"{fmt_date(earliest)} -> {fmt_date(latest)}" if earliest else "-"
        walk = registered.get(key)

        if walk is None:
            # Rows with no walk record predate this manifest's walks table.
            print(f"  {key:<24} {count:>10,}   {span}   (no walk record)")
            continue

        marker = "done" if walk["state"] == WalkState.DONE else "OPEN"
        print(f"  {key:<24} {count:>10,}   {span}   [{marker}]")
        print(f"      {walk['pages_done']} pages, stopped: {walk['reason'] or 'not yet run'}")
        print(f"      {_format_params(walk['params'])}")  # pyright: ignore[reportArgumentType]

    resumable = [walk["key"] for walk in registered.values() if walk["state"] == WalkState.OPEN]
    if resumable:
        print(f"\n{len(resumable)} unfinished walk(s); resume with:")
        for key in resumable:
            print(f"  dota-harvest discover --resume {key}")

    files = list(RAW_DIR.glob("*.jsonl.gz")) if RAW_DIR.exists() else []  # pyright: ignore[reportAttributeAccessIssue]
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
    # Defaults to None rather than DEFAULT_PAGES so a resume can tell an
    # explicit --pages from an untouched one and fall back to the stored value.
    d.add_argument(
        "--pages",
        type=int,
        default=None,
        help=f"100 ids per page. Default {DEFAULT_PAGES}.",
    )
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
        "--no-seek",
        action="store_true",
        help="do not jump to the collectable window first; page from the newest "
        "match instead. Slower by ~160 pages when --min-age-hours is set.",
    )
    d.add_argument(
        "--reserve",
        type=int,
        default=50,
        help="stop with this many daily API calls unspent, leaving "
        "headroom for reference/probe commands",
    )
    d.add_argument(
        "--resume",
        metavar="WALK",
        default=None,
        help="continue an unfinished walk under its original filters: "
        "'label' or 'source:label'. --pages and --until may be given to change "
        "how far this run goes; every other flag is refused, since a walk's "
        "filters are locked when it is created.",
    )
    d.set_defaults(fn=cmd_discover)

    f = sub.add_parser("fetch", help="pull match detail from STRATZ")
    f.add_argument("--limit", type=int, default=1000)
    f.add_argument("--batch", type=int, default=STRATZ_BATCH)
    f.add_argument("--sleep", type=float, default=STRATZ_SLEEP)
    f.add_argument(
        "--walks",
        type=walk_list,
        default=None,
        help="only fetch matches from these walks: comma-separated "
        "'label' or 'source:label' (e.g. recent-2d,public:patch-741). "
        "Default is every walk.",
    )
    f.set_defaults(fn=cmd_fetch)

    rm = sub.add_parser("remove", help="delete a walk's pending matches and cursor")
    rm.add_argument(
        "walks",
        type=walk_list,
        help="comma-separated 'label' or 'source:label' to remove",
    )
    rm.add_argument(
        "--all",
        action="store_true",
        help="also delete rows already fetched. Their responses stay in raw/ but "
        "nothing will map them back to a match id.",
    )
    rm.set_defaults(fn=cmd_remove)

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
    if getattr(a, "resume", None):
        # Inspect argv rather than the parsed namespace: defaults are
        # indistinguishable from values the user typed, and only a typed flag
        # signals an expectation that it will take effect.
        typed = sys.argv[1:]
        supplied = sorted(
            flag
            for flag in TUNING_FLAGS
            if any(arg == flag or arg.startswith(f"{flag}=") for arg in typed)
        )
        if supplied:
            ap.error(
                f"--resume cannot be combined with {', '.join(supplied)}. "
                f"A walk's filters are locked when it is created; omit the flag to "
                f"resume, or use --label to start a new walk. "
                f"({' and '.join(RESUMABLE_OVERRIDES)} may be changed on a resume.)"
            )
    a.fn(a)


if __name__ == "__main__":
    main()
