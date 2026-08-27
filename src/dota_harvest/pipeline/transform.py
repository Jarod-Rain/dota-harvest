"""Stage 3: raw JSONL to patch-partitioned Parquet.

Network-free and fully re-runnable: everything here reads the raw landing zone
and the reference tables, so the output can be rebuilt at any time without
touching an API. Output is staged and swapped in atomically, so a failure
partway through leaves the previous dataset intact.
"""

from __future__ import annotations

import gzip
import shutil
import sys
from pathlib import Path
from typing import Any, Final

import duckdb

from dota_harvest.core.config import PARQUET_DIR, RAW_DIR
from dota_harvest.core.types import FieldTree

#: Leading bytes of a gzip stream.
GZIP_MAGIC: Final[bytes] = b"\x1f\x8b"

#: Read size when verifying a shard decompresses cleanly.
GZIP_PROBE_CHUNK: Final[int] = 1 << 20

#: Largest single JSON record DuckDB will accept, in bytes. Parsed matches with
#: full purchase logs run well past the default.
MAX_OBJECT_SIZE: Final[int] = 20_000_000

#: Warn when the newest match sits more than this many days past the newest
#: known patch -- a sign the patch table is missing releases.
STALE_PATCH_DAYS: Final[int] = 45

SECONDS_PER_DAY: Final[int] = 86_400

#: Subdirectories the transform produces, in write order.
OUTPUT_TABLES: Final[tuple[str, ...]] = ("players", "purchases")


def scalar(con: duckdb.DuckDBPyConnection, sql: str) -> Any:
    """Execute a query and return the first column of its first row.

    Args:
        con: Open DuckDB connection.
        sql: A query expected to yield at least one row, such as an aggregate.

    Returns:
        The first column of the first row.

    Raises:
        RuntimeError: If the query returned no rows at all.

    Note:
        ``fetchone()`` is typed ``tuple | None`` because a query can return no
        rows, but an aggregate always returns exactly one. This asserts that
        invariant in one place instead of scattering ``# type: ignore`` at every
        call site.
    """
    row = con.execute(sql).fetchone()
    if row is None:
        raise RuntimeError(f"expected a row, got none: {sql}")
    return row[0]


# --- raw schema ----------------------------------------------------------
# The STRATZ match JSON we consume, declared explicitly rather than inferred.
#
# read_json_auto(union_by_name=true) builds the schema from whatever fields
# happen to appear in the files it sees. That is fragile here because the raw
# landing zone is heterogeneous: public matches omit the league/team ids and
# every per-player combat stat, and any stat category STRATZ did not parse is
# omitted per player. When the sample lacks a field entirely -- e.g. a
# public-only collection -- the column simply does not exist, and every
# reference to it fails to bind:
#
#   Binder Error: Values list "m" does not have a column named "leagueId"
#
# Pinning the schema makes the transform independent of the sample: read_json
# fills any absent key (top-level or nested) with NULL instead of dropping the
# column. Types were verified consistent across the collection; see DATA.md.
#
# The cost of pinning is that it turns a loud failure into a silent one: a field
# renamed upstream, or misspelled here, yields an all-NULL column rather than a
# bind error. That is the same silent drift that produced the original bug, so
# this table must stay in step with MATCH_FIELDS in clients.py --
# `dota-harvest check` diffs the two (and both against the stored data) rather
# than leaving the invariant to memory.
_STATS_STRUCT = (
    "STRUCT("
    '  itemPurchases STRUCT(itemId BIGINT, "time" BIGINT)[],'
    "  campStack BIGINT[],"
    '  runes STRUCT(rune VARCHAR, "time" BIGINT)[],'
    '  wardDestruction STRUCT("time" BIGINT, isWard BOOLEAN)[],'
    '  killEvents STRUCT("time" BIGINT)[]'
    ")"
)
_PLAYER_STRUCT = (
    "STRUCT("
    "  steamAccountId BIGINT, heroId BIGINT, isRadiant BOOLEAN, isVictory BOOLEAN,"
    '  lane VARCHAR, role VARCHAR, "position" VARCHAR,'
    "  networth BIGINT, level BIGINT, kills BIGINT, deaths BIGINT, assists BIGINT,"
    "  numLastHits BIGINT, numDenies BIGINT, goldPerMinute BIGINT, experiencePerMinute BIGINT,"
    "  heroDamage BIGINT, towerDamage BIGINT, heroHealing BIGINT, imp BIGINT, award VARCHAR,"
    "  item0Id BIGINT, item1Id BIGINT, item2Id BIGINT, item3Id BIGINT, item4Id BIGINT,"
    "  item5Id BIGINT, backpack0Id BIGINT, backpack1Id BIGINT, backpack2Id BIGINT,"
    "  neutral0Id BIGINT,"
    f" stats {_STATS_STRUCT}"
    ")"
)
RAW_COLUMNS = {
    "id": "BIGINT",
    "didRadiantWin": "BOOLEAN",
    "durationSeconds": "BIGINT",
    "startDateTime": "BIGINT",
    "endDateTime": "BIGINT",
    "gameVersionId": "BIGINT",
    "lobbyType": "VARCHAR",
    "gameMode": "VARCHAR",
    "rank": "BIGINT",
    "parsedDateTime": "BIGINT",
    "leagueId": "BIGINT",
    "seriesId": "BIGINT",
    "radiantTeamId": "BIGINT",
    "direTeamId": "BIGINT",
    "firstBloodTime": "BIGINT",
    "towerStatusRadiant": "BIGINT",
    "towerStatusDire": "BIGINT",
    "players": f"{_PLAYER_STRUCT}[]",
}


def _columns_clause() -> str:
    """Render :data:`RAW_COLUMNS` as a DuckDB ``columns=`` struct literal.

    Returns:
        A brace-delimited mapping of column name to SQL type, ready to embed in
        a ``read_json`` call.
    """
    inner = ", ".join(f"'{name}': '{sql_type}'" for name, sql_type in RAW_COLUMNS.items())
    return "{" + inner + "}"


def _split_fields(body: str) -> list[str]:
    """Split a ``STRUCT`` body on its top-level commas.

    Args:
        body: Text between the outermost parentheses of a ``STRUCT(...)`` type.

    Returns:
        One string per field declaration. Commas inside nested ``STRUCT``s are
        left alone, so ``a BIGINT, b STRUCT(c BIGINT, d BIGINT)`` yields two
        entries rather than three.
    """
    parts: list[str] = []
    depth = 0
    current = ""
    for char in body:
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        if char == "," and depth == 0:
            parts.append(current)
            current = ""
        else:
            current += char
    if current.strip():
        parts.append(current)
    return parts


def pinned_fields(columns: dict[str, str] | None = None) -> FieldTree:
    """Parse the pinned schema into a nested field tree.

    Args:
        columns: Column-name to SQL-type mapping. Defaults to
            :data:`RAW_COLUMNS`.

    Returns:
        Field names nested by depth, shaped exactly like
        :func:`~dota_harvest.api.clients.selected_fields` so the two can be
        compared directly. List markers and quoting are stripped.

    Note:
        Derived from the same type strings handed to the reader, so the drift
        check cannot pass while the actual schema says something else.
    """

    def parse(sql_type: str) -> FieldTree:
        sql_type = sql_type.strip()
        while sql_type.endswith("[]"):
            sql_type = sql_type[:-2].strip()
        if not sql_type.upper().startswith("STRUCT("):
            return {}
        body = sql_type[sql_type.index("(") + 1 : sql_type.rindex(")")]
        tree: FieldTree = {}
        for field in _split_fields(body):
            field = field.strip()
            if not field:
                continue
            name, _, rest = field.partition(" ")
            tree[name.strip().strip('"')] = parse(rest)
        return tree

    return {name: parse(sql_type) for name, sql_type in (columns or RAW_COLUMNS).items()}


def _assert_readable_gzip(path: Path) -> None:
    """Verify a file is a complete, decompressible gzip stream.

    Args:
        path: File to check.

    Raises:
        OSError: The file lacks a gzip header or is truncated. ``BadGzipFile``
            and zlib errors both subclass ``OSError``.

    Note:
        DuckDB rejects anything without a valid gzip header, including the
        zero-byte file a hard-killed fetch leaves behind (which Python's ``gzip``
        would otherwise read as empty). The magic bytes are checked first, then
        the whole stream is read to catch mid-file truncation.
    """
    with path.open("rb") as handle:
        if handle.read(2) != GZIP_MAGIC:
            raise OSError("not a gzip stream (bad magic)")
    with gzip.open(path, "rb") as handle:
        while handle.read(GZIP_PROBE_CHUNK):
            pass


def valid_raw_files(raw: Path) -> list[str]:
    """List raw shards that DuckDB will be able to read.

    Args:
        raw: Directory holding ``*.jsonl.gz`` shards.

    Returns:
        Paths of every readable shard, sorted. Unreadable ones are reported on
        stdout and omitted.

    Note:
        A fetch killed mid-write leaves a truncated or zero-byte file behind.
        DuckDB aborts the entire scan on the first such file ("Input is not a
        GZIP stream"), which would strand every good match over one bad
        download. Each file is stream-decompressed up front -- cheap next to the
        JSON parse and Parquet write that follow -- and any that will not read
        cleanly is dropped.
    """
    good: list[str] = []
    skipped: list[str] = []
    for path in sorted(raw.glob("*.jsonl.gz")):
        try:
            _assert_readable_gzip(path)
        except (OSError, EOFError) as exc:
            skipped.append(f"{path.name}: {exc}")
        else:
            good.append(str(path))
    if skipped:
        print(f"  skipping {len(skipped)} unreadable file(s):")
        for entry in skipped:
            print(f"    {entry}")
    return good


PLAYERS_SQL = """
COPY (
  SELECT
    m.id                                    AS match_id,
    pt.name                                 AS patch,
    m.gameVersionId                         AS stratz_version_id,
    m.startDateTime                         AS start_time,
    m.durationSeconds                       AS duration_s,
    m.didRadiantWin                         AS radiant_win,
    m.lobbyType                             AS lobby_type,
    m.gameMode                              AS game_mode,
    m.rank                                  AS avg_rank,
    m.parsedDateTime IS NOT NULL            AS is_parsed,
    m.endDateTime                           AS end_time,
    m.leagueId                              AS league_id,
    m.seriesId                              AS series_id,
    m.radiantTeamId                         AS radiant_team_id,
    m.direTeamId                            AS dire_team_id,
    m.firstBloodTime                        AS first_blood_time_s,
    m.towerStatusRadiant                    AS tower_status_radiant,
    m.towerStatusDire                       AS tower_status_dire,
    p.steamAccountId                        AS account_id,
    p.heroId                                AS hero_id,
    p.isRadiant                             AS is_radiant,
    p.isVictory                             AS is_victory,
    p.lane, p.role, p.position,
    p.networth, p.level, p.kills, p.deaths, p.assists,
    p.numLastHits    AS last_hits,
    p.numDenies      AS denies,
    p.goldPerMinute  AS gpm,
    p.experiencePerMinute AS xpm,
    p.heroDamage     AS hero_damage,
    p.towerDamage    AS tower_damage,
    p.heroHealing    AS hero_healing,
    p.imp,
    p.award,
    -- Fantasy scoring inputs. Nested under stats and only present on parsed
    -- matches, so these are null wherever the category was not collected.
    -- campStack is a per-minute cumulative array; the total camps stacked is
    -- its final value, not its length (which is just the game's minute count).
    CASE WHEN len(p.stats.campStack) > 0
         THEN p.stats.campStack[len(p.stats.campStack)] END       AS camps_stacked,
    CASE WHEN p.stats.runes IS NOT NULL
         THEN len(p.stats.runes) END                              AS runes_taken,
    CASE WHEN p.stats.wardDestruction IS NOT NULL
         THEN len(list_filter(p.stats.wardDestruction,
                              x -> x.isWard)) END                  AS obs_wards_killed,
    CASE WHEN p.stats.killEvents IS NOT NULL
         THEN len(p.stats.killEvents) END                          AS kill_events,
    [p.item0Id, p.item1Id, p.item2Id,
     p.item3Id, p.item4Id, p.item5Id]       AS inventory,
    [p.backpack0Id, p.backpack1Id, p.backpack2Id] AS backpack,
    p.neutral0Id                            AS neutral_item
  FROM raw m
  ASOF JOIN patches pt ON m.startDateTime >= pt.start_ts,
  UNNEST(m.players) AS t(p)
) TO '{out}/players'
  (FORMAT PARQUET, PARTITION_BY (patch), COMPRESSION ZSTD, OVERWRITE_OR_IGNORE 1);
"""

PURCHASES_SQL = """
COPY (
  SELECT
    m.id        AS match_id,
    pt.name     AS patch,
    p.heroId    AS hero_id,
    p.isRadiant AS is_radiant,
    p.isVictory AS is_victory,
    ip.itemId   AS item_id,
    ip.time     AS purchase_time_s
  FROM raw m
  ASOF JOIN patches pt ON m.startDateTime >= pt.start_ts,
       UNNEST(m.players) AS t(p),
       UNNEST(p.stats.itemPurchases) AS u(ip)
  WHERE p.stats.itemPurchases IS NOT NULL
) TO '{out}/purchases'
  (FORMAT PARQUET, PARTITION_BY (patch), COMPRESSION ZSTD, OVERWRITE_OR_IGNORE 1);
"""

# Where STRATZ was still minting ids, our date-derived patch must agree with
# theirs. It is the only period with ground truth to validate the date table.
CROSSCHECK_SQL = """
SELECT gv.name, pt.name, COUNT(*)
FROM raw m
ASOF JOIN patches pt ON m.startDateTime >= pt.start_ts
LEFT JOIN game_versions gv ON gv.id = m.gameVersionId
WHERE m.gameVersionId < 182
GROUP BY 1, 2 HAVING gv.name IS DISTINCT FROM pt.name
ORDER BY 3 DESC LIMIT 10
"""


def _quote_sql_list(paths: list[str]) -> str:
    """Render file paths as a DuckDB list literal.

    Args:
        paths: Filesystem paths to embed.

    Returns:
        A bracketed, single-quoted list with embedded quotes escaped, so a path
        containing an apostrophe cannot terminate the literal early.
    """
    quoted = ", ".join("'" + path.replace("'", "''") + "'" for path in paths)
    return f"[{quoted}]"


def _register_views(con: duckdb.DuckDBPyConnection, files: list[str], out: Path) -> Path:
    """Register the ``raw``, ``patches``, and ``game_versions`` views.

    Args:
        con: Open DuckDB connection.
        files: Readable raw shards to scan.
        out: Parquet output directory holding the reference tables.

    Returns:
        Path to the patch table, which the caller reuses when reporting.

    Raises:
        SystemExit: If the patch table is missing, since every query joins it.
    """
    patches = out / "patches.parquet"
    if not patches.exists():
        sys.exit(f"{patches} not found. Run `dota-harvest reference` first.")

    con.execute(
        f"CREATE OR REPLACE VIEW raw AS SELECT * FROM read_json("
        f"{_quote_sql_list(files)}, format='newline_delimited', "
        f"columns={_columns_clause()}, maximum_object_size={MAX_OBJECT_SIZE})"
    )
    con.execute(f"CREATE OR REPLACE VIEW patches AS SELECT * FROM read_parquet('{patches}')")

    versions = out / "game_versions.parquet"
    if versions.exists():
        con.execute(
            f"CREATE OR REPLACE VIEW game_versions AS SELECT * FROM read_parquet('{versions}')"
        )
    return patches


def _assert_tables_written(stage: Path) -> None:
    """Confirm every expected table appeared in the staging directory.

    Args:
        stage: Staging directory the COPY statements wrote into.

    Raises:
        RuntimeError: If any table is absent. DuckDB writes no directory at all
            when a query matches zero rows, so this catches an empty result
            before it can be swapped over a good dataset.
    """
    missing = [table for table in OUTPUT_TABLES if not (stage / table).exists()]
    if missing:
        raise RuntimeError(f"{', '.join(missing)} produced no output")


def _write_tables(con: duckdb.DuckDBPyConnection, out: Path) -> None:
    """Build the Parquet tables in staging, then swap them in atomically.

    Args:
        con: Open DuckDB connection with the views registered.
        out: Destination directory.

    Raises:
        RuntimeError: If either table produced no output directory.

    Note:
        Deleting the previous output up front means any failure in the SQL --
        a schema drift, a bad patch table, a full disk -- destroys a good
        dataset and leaves nothing in its place. Writing into a fresh tree also
        preserves the original reason for clearing: a renamed partition key
        would otherwise leave stale directories DuckDB refuses to scan.
    """
    stage = out / "_staging"
    if stage.exists():
        shutil.rmtree(stage)
    stage.mkdir(parents=True)

    try:
        for table, statement in (("players", PLAYERS_SQL), ("purchases", PURCHASES_SQL)):
            print(f"writing {table} ...")
            con.execute(statement.format(out=stage))
        _assert_tables_written(stage)

        # Swap in only now that every table exists: a partial rename would be
        # just as destructive as deleting up front.
        for table in OUTPUT_TABLES:
            final = out / table
            if final.exists():
                shutil.rmtree(final)
            (stage / table).rename(final)
    except BaseException:
        shutil.rmtree(stage, ignore_errors=True)
        raise
    shutil.rmtree(stage, ignore_errors=True)


def _report_coverage(con: duckdb.DuckDBPyConnection, out: Path, patches: Path) -> None:
    """Print per-patch match counts and warn when the patch table looks stale.

    Args:
        con: Open DuckDB connection.
        out: Directory holding the written tables.
        patches: Path to the patch table.
    """
    players = f"read_parquet('{out}/players/**/*.parquet', hive_partitioning=true)"

    print("\nmatches per patch:")
    rows = con.execute(
        f"SELECT patch, COUNT(DISTINCT match_id), "
        f"COUNT(DISTINCT CASE WHEN is_parsed THEN match_id END) "
        f"FROM {players} GROUP BY patch ORDER BY MIN(start_time)"
    ).fetchall()
    for patch, count, parsed in rows:
        pct = 100 * parsed / count if count else 0
        print(f"  {str(patch):<10} {count:>9,} matches, {parsed:>9,} parsed ({pct:.1f}%)")

    # Unlike the counts above, this one genuinely can return no rows -- an empty
    # players table produces no groups -- so it stays a plain fetchone().
    newest = con.execute(
        f"SELECT pl.patch, MAX(pl.start_time), pt.start_ts FROM {players} pl "
        f"JOIN read_parquet('{patches}') pt ON pt.name = pl.patch "
        f"GROUP BY 1, 3 ORDER BY 2 DESC LIMIT 1"
    ).fetchone()
    if newest and (lag := (newest[1] - newest[2]) / SECONDS_PER_DAY) > STALE_PATCH_DAYS:
        print(
            f"\n  WARNING: newest matches sit {lag:.0f} days after {newest[0]} released. "
            f"The patch table is probably missing releases."
        )


def _report_crosscheck(con: duckdb.DuckDBPyConnection) -> None:
    """Compare date-derived patches against STRATZ's own labels.

    Args:
        con: Open DuckDB connection with ``game_versions`` registered.

    Note:
        Only meaningful before id 182, where STRATZ was still minting version
        ids. That window is the sole ground truth available to validate the
        date-derived patch table.
    """
    rows = con.execute(CROSSCHECK_SQL).fetchall()
    print("\ncross-check vs STRATZ (pre-182, where they were correct):")
    if not rows:
        print("  no disagreements")
        return
    for stratz_name, derived_name, count in rows:
        print(f"  STRATZ says {stratz_name}, we derive {derived_name}: {count:,} matches")


def run(raw: Path = RAW_DIR, out: Path = PARQUET_DIR) -> None:
    """Convert the raw landing zone into patch-partitioned Parquet.

    Args:
        raw: Directory of gzipped JSONL shards from
            :mod:`~dota_harvest.pipeline.fetch`.
        out: Destination for the Parquet tables. Must already contain the
            reference tables built by :mod:`~dota_harvest.pipeline.reference`.

    Raises:
        SystemExit: If the patch table or the raw landing zone is missing, or if
            no shard is readable.

    Note:
        Re-runnable and network-free. The previous output survives any failure,
        so a broken run costs nothing but the time it took.
    """
    out.mkdir(parents=True, exist_ok=True)

    if not raw.exists():
        sys.exit(f"{raw} not found. Run `dota-harvest fetch` first.")
    files = valid_raw_files(raw)
    if not files:
        sys.exit(f"no readable *.jsonl.gz files in {raw}. Run `dota-harvest fetch` first.")

    con = duckdb.connect()
    patches = _register_views(con, files, out)

    print(f"{scalar(con, 'SELECT COUNT(*) FROM raw'):,} matches in raw")
    dropped = scalar(
        con,
        "SELECT COUNT(*) FROM raw m WHERE m.startDateTime < (SELECT MIN(start_ts) FROM patches)",
    )
    if dropped:
        print(f"  {dropped:,} matches predate the earliest known patch; ASOF JOIN drops them")

    _write_tables(con, out)
    _report_coverage(con, out, patches)
    if (out / "game_versions.parquet").exists():
        _report_crosscheck(con)
