# Data notes

What the STRATZ and OpenDota APIs actually do, as opposed to what they
document, and what this tool produces as a result.

Findings are dated because most are claims about vendor behaviour that can
change without notice. If you re-verify one, update it here **with the new
date** — the dates are what make this trustworthy a year from now.

---

## 1. The APIs

### STRATZ — GraphQL, match detail

Endpoint `https://api.stratz.com/graphql`. Bearer token from <https://stratz.com/api>.

**Requires `User-Agent: STRATZ_API`.** Without it every request 403s with what looks like a Cloudflare IP ban. Set in `config.STRATZ_UA`.

**`matches(ids: [...])` is admin-gated.** It exists in the schema but returns `"User is not an admin."` for ordinary tokens. Workaround: alias N singular`match(id:)` lookups into one HTTP request (`clients.build_batch_query`). A batch of 10 produces a ~6 KB query.

Aliased queries fail *per alias* — a bad id yields null at that alias plus an entry in `errors` while its siblings return normally. So a non-empty `errors` must not be read as a dead batch; `clients.parse_batch_response` splits the response into found and errored ids.

**Ingest lags OpenDota by roughly a day.** Matches younger than ~48h often return null with no error — indistinguishable from a bad field name unless you know to check. `discover` skips them via `MIN_MATCH_AGE_HOURS`.

**`gameVersionId` is frozen at 182 (7.40b, 2025-12-23).** *Verified 2026-08* by sampling matches across two years:

| Match date | `gameVersionId` | Patch  |
|------------|-----------------|--------|
| 2024-12    | 178             | 7.37e  |
| 2025-04    | 179             | 7.38   |
| 2025-08    | 180             | 7.39   |
| 2025-12    | 181             | 7.40   |
| 2026-03    | 182             | 7.40b  |
| 2026-06    | 182             | 7.40b  |
| 2026-08    | 182             | 7.40b  |

Historical tagging is sound; STRATZ simply stopped minting new ids.
Consequence: the field cannot be the patch key. See §3.

Re-check with `dota-harvest probe versions`. If ids above 182 start appearing, this section is stale and `STRATZ_TRUSTED_UNTIL_TS` should move.

**Parse coverage is excellent.** ~95% of matches carry full `itemPurchases` logs, and this holds at the far edge of the collection window — a public ranked match from 2025-08-08 still had all 48 purchase events. Unlike OpenDota, STRATZ parses on ingest rather than on demand, so timings come free with the match.

### OpenDota — REST, discovery and reference data

Endpoint `https://api.opendota.com/api`. Key optional; 60 req/min and 50k calls/month on the free tier.

**Free tier: 3,000 calls/day and 60/minute**

**`/publicMatches` retains ~365 days.** *Verified 2026-08*: data returned at 365 days, empty at 380. Supports `min_rank` and pages backwards via `less_than_match_id`, 100 ids per call. Re-check with `dota-harvest probe retention`.

Rank tier format: first digit is the medal (7=Divine, 8=Immortal), second is the star. `--min-rank 75` means Divine 5 and above.

**`/proMatches` goes back years** — at least to 2019. Used as a time anchor for locating match ids near a target date.

**`/constants/items` carries the recipe graph.** STRATZ's item constants do not. This is the only reason OpenDota is in the reference path at all.

**Empty results are ambiguous.** An empty page means "past the retention floor", not "search further back". Getting this backwards silently burns the rate limit walking into the void.

### Why discovery and detail use different vendors

Inverted from the obvious arrangement: OpenDota discovers, STRATZ fetches
detail.

`/publicMatches` returns 100 ids per call with a server-side rank filter, making the sampling frame nearly free. STRATZ's aliased batches return 10 full matches per call — roughly 10× cheaper per match than OpenDota's per-match endpoint, which would otherwise exhaust the 50k/month quota in a weekend.

Walking `less_than_match_id` backwards also yields a **time-ordered census** rather than crawling leaderboard players' match histories. Leaderboard crawling oversamples a few thousand accounts and their narrow hero pools, which biases any downstream analysis of what builds are common in a way that is very hard to detect after the fact.

---

## 2. Output layout

Everything under `$DOTA_DATA_DIR`. **Relative values resolve against the project root** rather than the shell's working directory. That keeps a checked-in `DOTA_DATA_DIR=../dota-data` meaning the same thing on every machine and from any directory, with no absolute paths committed. Absolute values are used as-is.

Unset, it defaults to `<project_root>/data`, falling back to the XDG data directory when the package is installed outside any checkout. `dota-harvest paths` prints what everything resolved to.

`.env` is loaded from the project root for the same reason: the default search walks up from the working directory and would find a different file, or none, depending on where the command was run.

```
data/
├── manifest.sqlite            match id ledger + discovery cursors
├── patches_override.json      hand-maintained patch releases
├── raw/stratz/*.jsonl.gz      immutable landing zone, one JSON match per line
└── parquet/
    ├── players/patch=*/       one row per (match, player)
    ├── purchases/patch=*/     one row per (match, player, item, time)
    ├── patches.parquet        owned patch release-date table
    ├── game_versions.parquet  STRATZ versions (historical reference only)
    ├── items.parquet          STRATZ item names — the ids match data uses
    ├── items_meta.parquet     OpenDota item taxonomy + recipe graph
    └── heroes.parquet
```

The `parquet/` directory is the contract. Consumers should read it and ignore everything else.

### `players/`

One row per player per match. `inventory` and `backpack` are lists of item ids; join to `items.parquet` for names. `is_parsed` indicates whether a purchase log exists. `stratz_version_id` is retained only as a cross-check against `patch`.

### `purchases/`

One row per purchase event. **Not a build** — see §4.

Kept separate from `players/` rather than widened into it because only parsed matches have purchase logs, so the two tables have different support. Merging would make "no timing data" indistinguishable from "bought nothing".

### `manifest.sqlite`

`matches(match_id, source, label, start_time, avg_rank_tier, lobby_type, game_mode, status, attempts, raw_file, last_error, discovered_at)`

Status lifecycle: `discovered` → `fetched` | `missing` | `failed`.

`missing` and `failed` are deliberately distinct. An id returning neither data nor an error is not in STRATZ's index — a coverage fact. An id that errored is a transient fault worth retrying (up to `attempts < 3`). Collapsing them makes both the retry logic and the coverage statistics meaningless.

`cursors(key, value)` where key is `"{source}:{label}"`. Forward collection and historical backfill are independent walks over the same source; sharing a cursor means whichever ran last dictates where the other resumes — silently, since both keep appearing to work. Always pass a distinct `--label` per walk.

### `raw/` — the landing zone

Gzipped JSONL, written verbatim, never parsed at ingest. `transform` reads only from here and is network-free, so it can be re-run from scratch whenever the output schema changes. Both Parquet output directories are cleared on each run precisely because they are disposable.

**`raw/` is the only irreplaceable directory.** Everything else rebuilds from it in minutes; it costs days of downloading. Back it up once you have volume.

---

## 3. Patch is derived from dates, not from the API

Since `gameVersionId` froze, patch is computed by ASOF JOIN of `start_time` against `patches.parquet`, which merges three sources in precedence order:

1. `$DOTA_DATA_DIR/patches_override.json` — hand-maintained, wins over all 
2. STRATZ `game_versions`, **restricted to its trusted range** (≤ 2025-12-24)
3. OpenDota's `patch.json`, for the range after that

STRATZ outranks OpenDota inside its trusted range for two reasons. It carries lettered patches (7.40b, 7.37e) where OpenDota's list has **major patches only**. And it is more reliable on dates, the two sources disagree on 7.38 (1 day), 7.40 (1 day), and **7.39 by seven days** (STRATZ 2025-05-30, OpenDota 2025-05-22). OpenDota's entries carry precise sub-minute timestamps that look like detection times rather than release times.

`reference` prints any disagreement it finds. Since lettered patches never appear in either automated source after 2025-12, the override file is the only way to get them.

**Constants come from GitHub, not the API.** `dotaconstants` is the static repo behind OpenDota's `/constants/*` endpoints, read directly from `raw.githubusercontent.com`. This costs nothing against the 3,000/day budget, which matters because reference data gets re-pulled often while iterating. Set `DOTACONSTANTS_REF` to a commit sha to pin. The API remains a fallback.

**Validation.** For matches before version 182, STRATZ's tagging was correct, so the derived patch has ground truth to check against. `transform` reports any disagreement. This is the only period where the release-date table can be validated at all — worth running against a few thousand historical matches before trusting it on current data. A wrong release timestamp misassigns matches near that boundary and nothing else will notice.

---

## 4. The purchase log is not a build

A single player's `itemPurchases` mixes at least four different things:

- **consumables** rebought all game (TP scrolls, tangos, wards)
- **components** later absorbed into an upgrade (Blade of Alacrity → Yasha)
- **recipes**, which have no standalone existence
- **terminal items** that persist to the final inventory

Purchase timestamps can be **negative** — items bought during the pre-horn phase. Any timing feature needs horn time as its defined zero.

Items also do not persist across patches. An item added in 7.40 shows zero purchases across a 7.39 slice, but that zero is *structural absence*, not evidence nobody wanted it. Any cross-patch aggregation needs a per-(patch, item) availability mask, derivable from this table by counting distinct matches per item per patch.

---

## 5. Pro and tournament collection

Public matches need a sampling frame built by walking time backwards. Pro matches do not: the population is small and enumerable, addressed by tournament or roster.

```bash
dota-harvest leagues "International 2026"     # find the league id
dota-harvest league <id> --with-history       # tournament + every team's year
dota-harvest teams "Falcons"                  # find a team id
dota-harvest team <id> --days-back 180
```

`/leagues/{id}/matches` returns a league's full match list in one call, and `/teams/{id}/matches` returns a team's whole history in one. A 16-team event plus each roster's history is ~18 API calls. The expensive half is fetching detail from STRATZ afterwards.

**The 48-hour age filter does not apply here.** `discover` skips recent matches because STRATZ trails OpenDota on public ingest, but a tournament in progress is exactly when you want today's games. The league and team paths pull everything; anything STRATZ has not indexed lands in `missing` and is retried next run.

**During a live tournament, re-run the league command periodically.** Match ids are inserted with `INSERT OR IGNORE`, so repeat runs are cheap and only add what is new.

### Fantasy-relevant fields

The match query carries the stats fantasy scoring uses: last hits, denies, GPM, XPM, hero/tower damage, healing, camps stacked, runes, observer wards killed, plus `leagueId`, `seriesId`, and both team ids. The nested ones live under `stats` and are therefore **null for unparsed matches**.
