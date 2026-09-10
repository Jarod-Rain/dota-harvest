"""API diagnostics. Kept in the package because their findings are load-bearing.

Re-run these when something looks wrong; each one isolates a specific failure
that is otherwise silent -- a null match reads the same as a bad field name, an
empty page reads the same as an exhausted archive.
"""

from __future__ import annotations

import gzip
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Final

import duckdb

from dota_harvest.api.clients import (
    build_batch_query,
    opendota_get,
    parse_batch_response,
    selected_fields,
    stratz_query,
)
from dota_harvest.core.config import PARQUET_DIR, RAW_DIR
from dota_harvest.core.manifest import fmt_date, parse_date
from dota_harvest.core.types import FieldTree, JSONMapping
from dota_harvest.pipeline.transform import pinned_fields

SECONDS_PER_DAY: Final[int] = 86_400

#: Step back this many ids to land on matches STRATZ has certainly indexed.
#: Ids are sequential at roughly 1.5M/day, so this is about three days.
PROBE_ID_LOOKBACK: Final[int] = 5_000_000

#: Records to read from each shard when checking for schema drift.
STALE_PROBE_RECORDS: Final[int] = 200

#: Field paths to name individually before collapsing into a count.
DRIFT_PREVIEW_FIELDS: Final[int] = 4

#: Interpolation search stops once a probe lands within this many days.
NEAR_ENOUGH_DAYS: Final[int] = 5

#: Match ids tried when looking for a historical anchor in /proMatches.
ANCHOR_CANDIDATES: Final[tuple[int, ...]] = (5_000_000_000, 6_000_000_000, 7_500_000_000)

#: Item ids STRATZ reports in match inventories but never in ``itemPurchases``.
#:
#: These are current, purchasable 7.41 items. STRATZ returns them in ``item0Id``
#: through ``backpack2Id`` and they show up in tens of thousands of finished
#: inventories, yet zero appear in any purchase log -- measured at 0% logged
#: against 91% for every other held item, on fully parsed matches, and
#: reproduced against the live API. They are also missing from STRATZ's own
#: ``constants { items }``, which is the likely root: an id their catalogue
#: does not know cannot be attributed to a purchase event.
#:
#: The consequence is not ours to fix and refetching does not help. Anything
#: reconstructing a build order from ``purchases`` will never see these items,
#: so a timeline that ends holding one has an unexplained gap.
UNLOGGED_ITEMS: Final[tuple[int, ...]] = (
    1847,  # Splintmail
    1848,  # Shawl
    1849,  # Wizard Hat
    1851,  # Essence Distiller Recipe
    1852,  # Essence Distiller
    1853,  # Consecrated Wraps Recipe
    1854,  # Consecrated Wraps
    1855,  # Crella's Crozier Recipe
    1856,  # Crella's Crozier
    1857,  # Hydra's Breath Recipe
    1858,  # Hydra's Breath
    1872,  # Chasm Stone
)


def _probe_ids(count: int = 2) -> list[JSONMapping]:
    """Pick match ids old enough that STRATZ has certainly indexed them.

    Args:
        count: How many match summaries to return.

    Returns:
        Match summaries roughly three days old.

    Note:
        ``/publicMatches`` surfaces games minutes after they end while STRATZ
        ingests on a lag, so probing with the newest ids tests the wrong thing:
        an unindexed match returns null with no error, indistinguishable from a
        schema problem.
    """
    newest = opendota_get("publicMatches")[0]["match_id"]
    params = {"less_than_match_id": newest - PROBE_ID_LOOKBACK}
    return opendota_get("publicMatches", params)[:count]


def check(match_id: int | None = None) -> None:
    """Validate the token, the GraphQL field names, and the stored collection.

    Runs three escalating steps: a minimal query that isolates auth from schema,
    the full aliased batch that exercises every field name, and a schema-drift
    comparison across the query, the transform's pinned schema, and the raw
    shards on disk.

    Args:
        match_id: Probe this specific match instead of choosing recent ones.
            Useful when the automatic pick lands on an unindexed match.

    Raises:
        SystemExit: If authentication fails or the probe match is unavailable,
            since later steps cannot produce a meaningful result.
    """
    if match_id:
        ids = [match_id]
        print(f"probing match {match_id}")
    else:
        rows = _probe_ids(2)
        ids = [row["match_id"] for row in rows]
        age = (time.time() - rows[0]["start_time"]) / SECONDS_PER_DAY
        print(f"probing {ids} (~{age:.1f} days old)")

    print("\nstep 1: minimal query (token + indexing) ...")
    payload = stratz_query(f"query {{ match(id: {ids[0]}) {{ id }} }}")
    if payload.get("errors"):
        print("\nfailed on the simplest possible query -- this is auth, not schema:")
        for error in payload["errors"]:
            print(f"  {error.get('message')}")
        sys.exit(1)
    if not (payload.get("data") or {}).get("match"):
        print(f"\n  match {ids[0]} came back null -- STRATZ has no record.")
        print("  Almost always ingest lag. Retry with an older id: dota-harvest check --id N")
        sys.exit(1)
    print("  OK")

    print("\nstep 2: full aliased batch (field names) ...")
    payload = stratz_query(build_batch_query(ids))
    found, errors = parse_batch_response(payload, ids)
    if errors:
        print("\nerrors -- edit MATCH_FIELDS in clients.py:")
        for error in payload.get("errors") or []:
            path = ".".join(str(part) for part in error.get("path", [])) or "(root)"
            print(f"  [{path}] {error.get('message')}")
        if not found:
            sys.exit(1)
    if not found:
        print("\nno data and no errors. Raw response:")
        print(json.dumps(payload, indent=2)[:2000])
        return

    match = next(iter(found.values()))
    print(f"\nOK, {len(found)}/{len(ids)} returned\n")
    print(json.dumps({k: v for k, v in match.items() if k != "players"}, indent=2))
    player = (match.get("players") or [{}])[0]
    purchases = (player.get("stats") or {}).get("itemPurchases") or []
    print(
        f"\nitemPurchases: {len(purchases)} entries; parsedDateTime={match.get('parsedDateTime')}"
    )
    if not purchases:
        print("  No purchase log -- this match is unparsed.")

    print("\nstep 3: schema drift ...")
    check_schema(sample=found)

    print("\nstep 4: unlogged items ...")
    check_unlogged_items()


def check_unlogged_items(out: Path = PARQUET_DIR) -> None:
    """Measure how many held items never appear in the purchase log.

    Args:
        out: Directory holding the built Parquet tables.

    Note:
        Reports the size of a known STRATZ defect rather than asserting a
        threshold: see :data:`UNLOGGED_ITEMS`. Nothing here can fix it, but a
        build order reconstructed from ``purchases`` is incomplete by exactly
        this much, and that number belongs in the open where it can be cited.
    """
    players = out / "players"
    purchases = out / "purchases"
    if not players.exists() or not purchases.exists():
        print("  no built tables yet; run `dota-harvest transform` first")
        return

    ids = ", ".join(str(item) for item in UNLOGGED_ITEMS)
    con = duckdb.connect()
    con.execute("SET enable_progress_bar=false")
    held = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{players}/**/*.parquet', hive_partitioning=true) p, "
        f"UNNEST(list_concat(p.inventory, p.backpack)) AS u(item) WHERE u.item IN ({ids})"
    ).fetchone()
    bought = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{purchases}/**/*.parquet', hive_partitioning=true) "
        f"WHERE item_id IN ({ids})"
    ).fetchone()

    held_n = held[0] if held else 0
    bought_n = bought[0] if bought else 0
    print(f"  {held_n:,} inventory slots hold an item STRATZ never logs a purchase for")
    print(f"  {bought_n:,} matching purchase rows exist")
    if held_n and not bought_n:
        print("  as expected: these ids reach us only as terminal state, never as events")
    elif bought_n:
        print("  STRATZ appears to have started logging these -- revisit UNLOGGED_ITEMS")


def _flatten(tree: FieldTree, prefix: str = "") -> set[str]:
    """Flatten a nested field tree into dotted paths.

    Args:
        tree: Nested field names, as produced by
            :func:`~dota_harvest.api.clients.selected_fields`.
        prefix: Path prefix used during recursion.

    Returns:
        Every path in the tree, including intermediate nodes, e.g.
        ``{"players", "players.stats", "players.stats.campStack"}``.
    """
    paths: set[str] = set()
    for name, child in tree.items():
        path = f"{prefix}{name}"
        paths.add(path)
        paths |= _flatten(child, f"{path}.")
    return paths


def _observed_fields(value: Any, prefix: str = "") -> set[str]:
    """Collect the field paths actually present in decoded JSON.

    Args:
        value: Any decoded JSON value; lists and dicts are descended into.
        prefix: Path prefix used during recursion.

    Returns:
        Dotted paths for every key encountered.

    Note:
        A field STRATZ omits for one player may be present for another (unparsed
        stat categories, mainly), so paths are unioned across every element
        rather than sampled from the first.
    """
    paths: set[str] = set()
    if isinstance(value, list):
        for item in value:
            paths |= _observed_fields(item, prefix)
    elif isinstance(value, dict):
        for name, child in value.items():
            path = f"{prefix}{name}"
            paths.add(path)
            paths |= _observed_fields(child, f"{path}.")
    return paths


def check_schema(sample: dict[int, JSONMapping] | None = None) -> bool:
    """Diff the query, the pinned transform schema, and the stored raw data.

    Args:
        sample: Freshly fetched matches to compare against the query. When
            omitted, only the query, the pinned schema, and the stored shards
            are compared, so the check runs offline.

    Returns:
        ``True`` when all available sources agree.

    Note:
        These three drifted apart once already: ``MATCH_FIELDS`` grew fields the
        corpus predated, and ``check`` could not see it because it only ever
        looked at a fresh response. Pinning the reader's schema fixed the crash
        but made the next drift silent -- an unpinned or renamed field now
        yields an all-NULL column instead of an error. Comparing all three is
        what makes either kind of drift loud.
    """
    queried = _flatten(selected_fields())
    pinned = _flatten(pinned_fields())
    ok = True

    missing = sorted(queried - pinned)
    extra = sorted(pinned - queried)
    if missing:
        ok = False
        print("  downloaded but NOT in transform's RAW_COLUMNS (silently dropped):")
        for f in missing:
            print(f"    {f}")
    if extra:
        ok = False
        print("  in RAW_COLUMNS but never requested (will be all-NULL):")
        for f in extra:
            print(f"    {f}")
    if not missing and not extra:
        print(f"  query <-> RAW_COLUMNS: agree ({len(queried)} fields)")

    if sample:
        observed: set[str] = set()
        for match in sample.values():
            observed |= _observed_fields(match)
        absent = sorted(queried - observed)
        if absent:
            ok = False
            print("  requested but ABSENT from the live response (renamed upstream?):")
            for f in absent:
                print(f"    {f}")
        else:
            print("  query <-> live response: all requested fields present")

    stale = _stale_raw_files(queried)
    if stale:
        ok = False
        print(f"\n  {len(stale)} raw file(s) predate the current query:")
        for name, gap in stale:
            head = ", ".join(gap[:DRIFT_PREVIEW_FIELDS])
            if len(gap) > DRIFT_PREVIEW_FIELDS:
                head += f", +{len(gap) - DRIFT_PREVIEW_FIELDS} more"
            print(f"    {name}: missing {len(gap)} field(s) -- {head}")
        print("  Those columns are NULL for these matches. To backfill, reset them:")
        print("    UPDATE matches SET status='discovered', attempts=0 WHERE raw_file=...;")
        print("  then move the stale file aside and re-run `dota-harvest fetch`.")
    elif RAW_DIR.exists():
        print("  query <-> stored raw: no stale files")

    return ok


def _stale_raw_files(
    queried: set[str],
    probe: int = STALE_PROBE_RECORDS,
) -> list[tuple[str, list[str]]]:
    """Find raw shards whose records lack fields the current query asks for.

    Args:
        queried: Dotted field paths the query requests.
        probe: Records to read from each shard before moving on.

    Returns:
        One ``(filename, missing_paths)`` pair per stale shard.

    Note:
        Reads a bounded prefix of each file: vintage is a property of the run
        that wrote it, so the first records are representative and a full scan
        of a multi-GB landing zone is not worth the seconds. Unreadable shards
        are skipped silently here; ``valid_raw_files`` reports them at transform
        time.
    """
    if not RAW_DIR.exists():
        return []

    stale: list[tuple[str, list[str]]] = []
    for path in sorted(RAW_DIR.glob("*.jsonl.gz")):
        observed: set[str] = set()
        records = 0
        try:
            with gzip.open(path, "rt", encoding="utf-8") as handle:
                for line in handle:
                    if not line.strip():
                        continue
                    observed |= _observed_fields(json.loads(line))
                    records += 1
                    if records >= probe:
                        break
        except (OSError, EOFError, json.JSONDecodeError):
            continue
        if records and (gap := queried - observed):
            stale.append((path.name, sorted(gap)))
    return stale


def _pro_below(match_id: int | None) -> JSONMapping | None:
    """Fetch the newest pro match below a given id.

    Args:
        match_id: Upper bound, exclusive. ``None`` fetches the newest overall.

    Returns:
        One match summary, or ``None`` below the archive floor.
    """
    params = {} if match_id is None else {"less_than_match_id": match_id}
    rows = opendota_get("proMatches", params)
    return rows[0] if rows else None


def _find_near(
    target: int,
    anchors: list[tuple[int, int]],
    budget: int = 7,
) -> JSONMapping | None:
    """Find a pro match played near a target time.

    Args:
        target: Desired start time, as a Unix timestamp.
        anchors: Two or more known ``(match_id, start_time)`` points bracketing
            the search range.
        budget: Maximum API probes to spend.

    Returns:
        The closest match found, or ``None`` if the range collapsed before any
        probe succeeded.

    Note:
        Ids increment at a steady rate, so linear interpolation lands close on
        the first guess and converges in a handful of probes. Binary search over
        a 2-billion-wide range takes thirty-plus and trips the rate limiter.
    """
    lo, hi = min(anchors), max(anchors)
    best = None
    for _ in range(budget):
        if hi[1] <= lo[1]:
            break
        frac = (target - lo[1]) / (hi[1] - lo[1])
        guess = max(min(int(lo[0] + frac * (hi[0] - lo[0])), hi[0]), lo[0] + 1)
        row = _pro_below(guess)
        if row is None:
            # Below the archive floor: the answer is higher, not lower.
            lo = (guess, lo[1])
            continue
        got = row["start_time"]
        if best is None or abs(got - target) < abs(best["start_time"] - target):
            best = row
        if abs(got - target) <= NEAR_ENOUGH_DAYS * SECONDS_PER_DAY:
            return row
        if got < target:
            lo = (row["match_id"], got)
        else:
            hi = (row["match_id"], got)
    return best


def _anchors() -> list[tuple[int, int]]:
    """Establish the endpoints for an interpolation search.

    Returns:
        A ``[(old_id, old_ts), (newest_id, newest_ts)]`` pair bracketing the
        pro-match archive.

    Raises:
        SystemExit: If ``/proMatches`` is unreachable or no historical anchor
            responds, since every probe depends on these bounds.
    """
    newest = _pro_below(None)
    if not newest:
        sys.exit("could not reach /proMatches")
    for candidate in ANCHOR_CANDIDATES:
        row = _pro_below(candidate)
        if row:
            return [
                (row["match_id"], row["start_time"]),
                (newest["match_id"], newest["start_time"]),
            ]
    sys.exit("no historical anchor found")


#: Dates sampled by :func:`probe_versions`, spanning the freeze boundary.
DEFAULT_VERSION_DATES: Final[list[str]] = [
    "2024-12-15",
    "2025-04-01",
    "2025-08-15",
    "2025-12-20",
    "2026-03-01",
    "2026-06-01",
    "2026-08-01",
]


def probe_versions(dates: list[str] | None = None) -> None:
    """Check whether STRATZ's gameVersionId still tracks patch releases.

    Args:
        dates: ``YYYY-MM-DD`` dates to sample. Future dates are skipped.

    Note:
        It does not. Matches after 7.40b all report version 182, which is why
        the patch table is derived from dates instead. Re-run this to confirm
        the freeze still holds before trusting any version-derived column.
    """
    anchors = _anchors()
    names = {}
    gv = PARQUET_DIR / "game_versions.parquet"
    if Path(gv).exists():
        names = dict(
            duckdb.connect().execute(f"SELECT id, name FROM read_parquet('{gv}')").fetchall()
        )

    print(f"{'target':>12} {'actual':>12} {'match_id':>14} {'ver':>5}  patch")
    for d in dates or DEFAULT_VERSION_DATES:
        target = parse_date(d)
        if target > time.time():
            continue
        row = _find_near(target, anchors)
        if not row:
            continue
        m = (
            stratz_query(f"query {{ match(id: {row['match_id']}) {{ gameVersionId }} }}").get(
                "data"
            )
            or {}
        ).get("match")
        ver = m.get("gameVersionId") if m else None
        print(
            f"{d:>12} {fmt_date(row['start_time']):>12} {row['match_id']:>14,} "
            f"{str(ver):>5}  {names.get(ver, 'NOT IN CONSTANTS' if ver else '-')}"
        )


def probe_retention(ages: list[int] | None = None) -> None:
    """Measure how far back ``/publicMatches`` still serves data.

    Args:
        ages: Ages in days to probe. Defaults to a ladder spanning the known
            boundary.

    Note:
        The answer as of 2026-08 is 365 days: data at 365, empty at 380. Re-run
        this before planning a historical backfill, since the window rolls
        forward continuously.
    """
    if not os.environ.get("OPENDOTA_KEY"):
        print("note: OPENDOTA_KEY unset; going slow\n", file=sys.stderr)
    anchors = _anchors()
    now = time.time()
    deepest = None
    print(f"{'age':>6} {'target':>12} {'publicMatches':>16} {'returned':>12}")
    for age in sorted(ages or [30, 120, 240, 340, 365, 380, 430]):
        row = _find_near(int(now - age * SECONDS_PER_DAY), anchors)
        if not row:
            continue
        rows = opendota_get("publicMatches", {"less_than_match_id": row["match_id"]})
        got = fmt_date(rows[0]["start_time"]) if rows else "-"
        if rows:
            deepest = age
        print(
            f"{age:>5}d {fmt_date(now - age * SECONDS_PER_DAY):>12} "
            f"{'DATA' if rows else 'empty':>16} {got:>12}"
        )
    print(f"\nwindow reaches ~{deepest} days" if deepest else "\nnothing returned")
