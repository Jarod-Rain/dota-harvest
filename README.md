# dota-harvest

Collect Dota 2 match data from STRATZ and OpenDota into patch-partitioned Parquet, ready for analysis.

## What you get

```
data/parquet/
├── players/patch=*/       one row per (match, player) — inventory, KDA, lane, result
├── purchases/patch=*/     one row per purchase event — item id and timestamp
├── kill_events/patch=*/   one row per kill — the hero and when they got it
├── items_meta.parquet     item taxonomy including the recipe graph
├── heroes.parquet
└── patches.parquet
```

Read it with anything that speaks Parquet:

```python
import duckdb
duckdb.sql("""
    SELECT hero_id, COUNT(*) AS n, AVG(is_victory::INT) AS winrate
    FROM read_parquet('data/parquet/players/**/*.parquet', hive_partitioning=true)
    WHERE patch = '7.41e'
    GROUP BY 1 ORDER BY n DESC
""")
```

## Setup

```bash
git clone https://github.com/Jarod-Rain/dota-harvest && cd dota-harvest
python -m venv .venv && source .venv/bin/activate
pip install -e .
cp .env.example .env      # add your STRATZ_TOKEN
```

A STRATZ token is required. You can get one for free, from <https://stratz.com/api>. An OpenDota key is optional but raises rate limits; without one, discovery is slow and occasionally 429s.

## Usage

```bash
dota-harvest check                 # validate token and GraphQL field names
dota-harvest reference             # constants, patch table, item taxonomy
dota-harvest discover --pages 50   # build the match-id sampling frame
dota-harvest fetch --limit 5000    # pull match detail into raw/
dota-harvest transform             # raw JSONL -> partitioned Parquet
dota-harvest status                # what has been collected
```

Run `check` first, always. It isolates auth failures from schema drift, which otherwise look identical. Both surface as an empty response.

### Collecting at scale

Discovery walks are independent and need distinct labels, or they share a cursor and silently resume in each other's position:

```bash
dota-harvest discover --label current            --until 2026-07-31 --pages 2000
dota-harvest discover --label prev --sample 0.5  --until 2025-12-15 --pages 4000
dota-harvest fetch --limit 1000000
```

`fetch` is resumable; pending ids live in a SQLite manifest, so interrupting it costs only the current batch. Run it under tmux for long jobs; it reports an achieved matches/hour rate as it goes.

`--min-rank` uses OpenDota's tier format: first digit is the medal (7=Divine, 8=Immortal), second is the star. Default is 75 (Divine 5+). Lower it for a broader sample.

### Diagnostics

Vendor behaviour changes. These isolate failures that are otherwise silent:

```bash
dota-harvest probe versions     # is gameVersionId meaningful again?
dota-harvest probe retention    # how far back does /publicMatches go now?
```

## Package layout

Four layers, each depending only on the ones above it:

```
src/dota_harvest/
├── core/           config, path resolution, SQLite manifest, shared types
├── api/            HTTP transport (retry, quota) and the STRATZ/OpenDota clients
├── pipeline/       discover → fetch → transform, plus reference and pro
├── cli/            argument parsing and dispatch; no domain logic
└── diagnostics.py  live API probes and the schema-drift check
```

The pipeline runs in that order: `discover` builds a sampling frame of match ids, `fetch` downloads detail into `raw/`, and `transform` turns the landing zone into Parquet.

## Development

```bash
pip install -e ".[dev]"
ruff check src/ tests/ && ruff format --check src/ tests/
pytest
```

The test suite is network-free, so it runs without credentials.

One invariant is worth knowing before editing either side of it: the fields in `MATCH_FIELDS` (`api/clients.py`) and the pinned schema in `RAW_COLUMNS` (`pipeline/transform.py`) must agree exactly. A field in the query but not the schema is downloaded and silently dropped; a field in the schema but not the query becomes an all-NULL column. `dota-harvest check` diffs both against each other and against the shards on disk, and `tests/test_schema_drift.py` enforces
it in CI.

## Design notes

[DATA.md](DATA.md) documents what the APIs actually do, with dates and evidence for each. Read it before changing constants in `core/config.py`.

Three things worth knowing up front:

- **`raw/` is the only irreplaceable directory.** Everything else rebuilds from it in minutes; it costs days of downloading. `transform` is network-free and fully re-runnable, so schema changes never mean re-downloading.
- **A purchase log is not a build.** It mixes consumables, absorbed components, recipes, and terminal items, and timestamps can be negative (pre-horn). See DATA.md §4 before treating purchase counts as item popularity.
- **`is_parsed` is not a coverage filter.** It reports whether STRATZ parsed the replay, not whether this project collected the fields.

## License

MIT
