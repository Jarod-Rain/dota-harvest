"""Reference tables: game versions, heroes, items, patches, item taxonomy.

These decode the bare integers in the match data. They are snapshotted rather
than resolved live: item names and properties change across patches, so a live
lookup would silently rewrite the meaning of historical rows.
"""

from __future__ import annotations

import json
import time
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Final

import duckdb

from dota_harvest.api.clients import opendota_get, stratz_query
from dota_harvest.core.config import OVERRIDE_PATH, PARQUET_DIR, STRATZ_TRUSTED_UNTIL_TS
from dota_harvest.core.manifest import fmt_date
from dota_harvest.core.types import JSONMapping
from dota_harvest.pipeline.transform import scalar

#: Timestamp formats seen across OpenDota and STRATZ, tried in order.
ISO_FORMATS: Final[tuple[str, ...]] = (
    "%Y-%m-%dT%H:%M:%S.%fZ",
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%d",
)

#: Warn when the newest known patch is older than this many days: everything
#: since is pooling into it, the same collapse STRATZ's own labels suffer.
STALE_PATCH_DAYS: Final[int] = 45

SECONDS_PER_DAY: Final[int] = 86_400

# STRATZ constants

VERSIONS_QUERY = "query { constants { gameVersions { id name asOfDateTime } } }"
ITEMS_QUERY = """
query { constants { items { id displayName shortName
    stat { cost isSellable isPurchasable isSideShop } } } }
"""
HEROES_QUERY = """
query { constants { heroes { id displayName shortName
    stats { primaryAttribute complexity } roles { roleId level } } } }
"""


def _write(
    con: duckdb.DuckDBPyConnection,
    rows: list[JSONMapping],
    out: Path,
    name: str,
) -> None:
    """Write records to a Parquet file via a temporary JSON staging file.

    Args:
        con: Open DuckDB connection.
        rows: Records to write. An empty list is reported and skipped.
        out: Destination directory.
        name: Base name for the output, written as ``<name>.parquet``.

    Note:
        Routing through JSON lets DuckDB infer the schema, including the nested
        list columns in the item taxonomy that would otherwise need declaring by
        hand. ``json.dumps`` escapes non-ASCII, so the staging file is pure
        ASCII regardless of the platform's default encoding.
    """
    if not rows:
        print(f"  {name}: nothing to write")
        return
    staging = out / f"_{name}.json"
    staging.write_text("\n".join(json.dumps(row) for row in rows), encoding="utf-8")
    try:
        con.execute(
            f"COPY (SELECT * FROM read_json_auto('{staging}', "
            f"format='newline_delimited', union_by_name=true)) "
            f"TO '{out}/{name}.parquet' (FORMAT PARQUET)"
        )
    finally:
        staging.unlink(missing_ok=True)
    print(f"  {name}: {len(rows):,} rows")


def _stratz_constant(field: str, query: str) -> list[JSONMapping]:
    """Fetch one constants collection from STRATZ.

    Args:
        field: Key under ``data.constants`` to extract, e.g. ``"heroes"``.
        query: GraphQL document selecting that field.

    Returns:
        The requested records, or an empty list if STRATZ reported errors. The
        errors are printed with their paths so an unknown field name can be
        identified and removed from the selection set.
    """
    payload = stratz_query(query)
    if payload.get("errors"):
        print(f"  {field}: GraphQL errors -- trim the selection set:")
        for error in payload["errors"]:
            path = ".".join(str(part) for part in error.get("path", [])) or "(root)"
            print(f"    [{path}] {error.get('message')}")
        return []
    return ((payload.get("data") or {}).get("constants") or {}).get(field) or []


def fetch_constants(out: Path = PARQUET_DIR) -> None:
    """Snapshot the STRATZ constants tables to Parquet.

    Args:
        out: Destination directory for ``game_versions``, ``items``, and
            ``heroes``.

    Note:
        Snapshotted rather than resolved live: item names and properties change
        across patches, so a live lookup would silently rewrite the meaning of
        historical rows.
    """
    out.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    for field, query, name in (
        ("gameVersions", VERSIONS_QUERY, "game_versions"),
        ("items", ITEMS_QUERY, "items"),
        ("heroes", HEROES_QUERY, "heroes"),
    ):
        _write(con, _stratz_constant(field, query), out, name)


# --- patch table ---------------------------------------------------------


def _parse_iso(value: str) -> int | None:
    """Parse a timestamp in any of the formats the sources use.

    Args:
        value: Date or datetime string.

    Returns:
        A UTC Unix timestamp, or ``None`` if no known format matched. OpenDota
        and STRATZ each use a different one, and both appear with and without
        fractional seconds.
    """
    for pattern in ISO_FORMATS:
        try:
            return int(datetime.strptime(value, pattern).replace(tzinfo=UTC).timestamp())
        except ValueError:
            continue
    return None


def _merge_stratz_patches(merged: dict[str, JSONMapping], out: Path) -> None:
    """Add STRATZ's patch names, restricted to the range it labels correctly.

    Args:
        merged: Accumulator keyed by patch name, updated in place.
        out: Directory holding ``game_versions.parquet``.

    Note:
        STRATZ contributes only its trusted range, where it is also the most
        granular source, having lettered patches. Beyond
        :data:`~dota_harvest.core.config.STRATZ_TRUSTED_UNTIL_TS` its version
        ids are frozen and would collapse every later patch into one.
    """
    versions = out / "game_versions.parquet"
    if not versions.exists():
        return
    rows = (
        duckdb.connect()
        .execute(
            f"SELECT name, asOfDateTime FROM read_parquet('{versions}') "
            f"WHERE asOfDateTime <= {STRATZ_TRUSTED_UNTIL_TS} ORDER BY asOfDateTime"
        )
        .fetchall()
    )
    for name, timestamp in rows:
        merged[name] = {"name": name, "start_ts": int(timestamp), "source": "stratz"}
    print(f"  stratz: {len(rows)} patches through {fmt_date(STRATZ_TRUSTED_UNTIL_TS)}")


def _merge_opendota_patches(merged: dict[str, JSONMapping]) -> None:
    """Add OpenDota's patch list, overriding STRATZ where they overlap.

    Args:
        merged: Accumulator keyed by patch name, updated in place.

    Note:
        A network failure here is tolerated: STRATZ and the override file can
        still produce a usable table, and failing the whole command over an
        unreachable secondary source would be worse than proceeding.
    """
    try:
        for patch in opendota_get("constants/patch"):
            timestamp = _parse_iso(str(patch.get("date", "")))
            if timestamp and patch.get("name"):
                name = str(patch["name"])
                merged[name] = {"name": name, "start_ts": timestamp, "source": "opendota"}
        print("  opendota: loaded")
    except Exception as exc:  # noqa: BLE001 - secondary source, keep going
        print(f"  opendota: unreachable ({exc})")


def _seed_override_template() -> None:
    """Create an example override file when none exists.

    Note:
        Seeded rather than left absent so the file is discoverable: the warning
        about a stale patch table points here, and an operator who has never
        seen the format needs an example to copy.
    """
    OVERRIDE_PATH.parent.mkdir(parents=True, exist_ok=True)
    OVERRIDE_PATH.write_text(
        json.dumps(
            [
                {
                    "name": "7.99z",
                    "date": "2030-01-01T00:00:00Z",
                    "_comment": "Example. Replace with real releases the sources lack. "
                    "Entries here win over OpenDota and STRATZ.",
                }
            ],
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"  override: seeded template at {OVERRIDE_PATH}")


def _merge_override_patches(merged: dict[str, JSONMapping]) -> None:
    """Apply the manual override file, which wins over both APIs.

    Args:
        merged: Accumulator keyed by patch name, updated in place.
    """
    if not OVERRIDE_PATH.exists():
        _seed_override_template()
    if not OVERRIDE_PATH.exists():
        return

    count = 0
    for patch in json.loads(OVERRIDE_PATH.read_text(encoding="utf-8")):
        timestamp = _parse_iso(str(patch.get("date", "")))
        if timestamp and patch.get("name"):
            name = str(patch["name"])
            merged[name] = {"name": name, "start_ts": timestamp, "source": "override"}
            count += 1
    print(f"  override: {count} patches from {OVERRIDE_PATH}")


def build_patches(out: Path = PARQUET_DIR, check_only: bool = False) -> None:
    """Merge three sources into a patch table this project owns.

    Args:
        out: Destination directory for ``patches.parquet``.
        check_only: Report the merged table without writing it.

    Raises:
        SystemExit: If no source yielded a single patch.

    Note:
        STRATZ's ``gameVersionId`` is correct through 7.40b and frozen after, so
        it cannot be the patch key. Precedence runs override > OpenDota >
        STRATZ, applied by merging in that order into one dict keyed by name.
    """
    out.mkdir(parents=True, exist_ok=True)
    merged: dict[str, JSONMapping] = {}

    _merge_stratz_patches(merged, out)
    _merge_opendota_patches(merged)
    _merge_override_patches(merged)

    patches = sorted(merged.values(), key=lambda row: row["start_ts"])
    if not patches:
        raise SystemExit("no patches from any source")

    print(
        f"\n{len(patches)} patches, {fmt_date(patches[0]['start_ts'])} "
        f"-> {fmt_date(patches[-1]['start_ts'])}\n"
    )
    for patch in patches[-6:]:
        print(f"  {patch['name']:<10} {fmt_date(patch['start_ts'])}   ({patch['source']})")

    gap_days = (time.time() - patches[-1]["start_ts"]) / SECONDS_PER_DAY
    if gap_days > STALE_PATCH_DAYS:
        print(f"\n  WARNING: newest known patch is {gap_days:.0f} days old. Everything")
        print(f"  since is pooling into {patches[-1]['name']} -- the same collapse")
        print(f"  STRATZ has. Add missing releases to {OVERRIDE_PATH}.")

    if check_only:
        return
    _write(duckdb.connect(), patches, out, "patches")


# --- item taxonomy -------------------------------------------------------


def _build_item_rows(raw: JSONMapping) -> list[JSONMapping]:
    """Turn OpenDota's item constants into rows with a resolved recipe graph.

    Args:
        raw: OpenDota ``constants/items``, keyed by item short name.

    Returns:
        One row per item, carrying both directions of the recipe graph:
        ``components`` (what it is built from) and ``built_into`` (what it
        builds into). Items without an id are skipped, as are component names
        that do not resolve.
    """
    name_to_id = {
        short: int(record["id"]) for short, record in raw.items() if record.get("id") is not None
    }
    built_into: dict[int, list[int]] = defaultdict(list)
    rows: list[JSONMapping] = []

    for short, record in raw.items():
        if record.get("id") is None:
            continue
        item_id = int(record["id"])
        components = [
            name_to_id[name] for name in (record.get("components") or []) if name in name_to_id
        ]
        for component in components:
            built_into[component].append(item_id)
        rows.append(
            {
                "item_id": item_id,
                "short_name": short,
                "display_name": record.get("dname") or short,
                "cost": record.get("cost"),
                "quality": record.get("qual"),
                "is_recipe": short.startswith("recipe_"),
                "is_consumable": record.get("qual") == "consumable",
                "components": components,
                "tier": record.get("tier"),
            }
        )

    # Filled in a second pass: built_into is only complete once every item's
    # components have been walked.
    for row in rows:
        row["built_into"] = sorted(built_into.get(row["item_id"], []))
    return rows


def build_item_meta(out: Path = PARQUET_DIR) -> None:
    """Resolve the item recipe graph and write it to Parquet.

    Args:
        out: Destination directory for ``items_meta.parquet``.

    Note:
        The purchase log mixes consumables rebought all game, components later
        absorbed into upgrades, recipes, and terminal items. Counting raw
        purchase events would rank TP scrolls as the most important item in
        Dota. Collapsing components into completions needs recipe data, which
        STRATZ does not carry -- hence OpenDota for this one table.
    """
    out.mkdir(parents=True, exist_ok=True)
    rows = _build_item_rows(opendota_get("constants/items"))

    con = duckdb.connect()
    _write(con, rows, out, "items_meta")

    consumables = sum(1 for row in rows if row["is_consumable"])
    terminal = sum(1 for row in rows if row["components"] and not row["built_into"])
    print(f"  {consumables} consumables, {terminal} terminal items")

    stratz_items = out / "items.parquet"
    if stratz_items.exists():
        _report_catalogue_gaps(con, out, stratz_items)


def _report_catalogue_gaps(
    con: duckdb.DuckDBPyConnection,
    out: Path,
    stratz_items: Path,
) -> None:
    """Compare the two item catalogues in both directions.

    Args:
        con: Open DuckDB connection.
        out: Directory holding ``items_meta.parquet``.
        stratz_items: Path to the STRATZ-sourced ``items.parquet``.

    Note:
        The catalogues are complementary, not redundant: STRATZ keeps ids for
        items Valve has since removed, and OpenDota lists current items STRATZ
        has not added. Reporting only the STRATZ-minus-OpenDota direction hid a
        real gap -- STRATZ's ``constants`` omits twelve purchasable 7.41 items
        (Essence Distiller, Consecrated Wraps, Crella's Crozier, Hydra's
        Breath, Chasm Stone, Splintmail, Shawl, Wizard Hat and their recipes)
        that appear in live match inventories. Anything resolving an id through
        ``items.parquet`` alone silently misses them, so both directions are
        named here.

        These ids reach us through match data regardless: see
        :data:`~dota_harvest.diagnostics.UNLOGGED_ITEMS` for the separate
        STRATZ defect that keeps them out of the purchase log.
    """
    meta = f"read_parquet('{out}/items_meta.parquet')"
    stratz = f"read_parquet('{stratz_items}')"

    only_stratz = scalar(
        con, f"SELECT COUNT(*) FROM {stratz} s WHERE s.id NOT IN (SELECT item_id FROM {meta})"
    )
    print(
        f"  {only_stratz} STRATZ item ids absent from OpenDota constants "
        f"(removed items and neutrals)"
    )

    # Only purchasable items matter here: cosmetics and internal placeholders
    # drift constantly and would bury a real omission in noise.
    rows = con.execute(
        f"SELECT item_id, display_name, cost FROM {meta} "
        f"WHERE cost > 0 AND item_id NOT IN (SELECT id FROM {stratz}) "
        f"ORDER BY item_id"
    ).fetchall()
    print(f"  {len(rows)} purchasable OpenDota items absent from STRATZ constants")
    for item_id, name, cost in rows:
        print(f"    {item_id:>5}  {name} ({cost}g)")
