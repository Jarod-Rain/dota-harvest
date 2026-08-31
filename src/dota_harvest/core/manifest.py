"""SQLite manifest tracking every match id the project knows about.

Cheap, transactional, and survives crashes -- which matters when a fetch run
lasts a day. It also keeps 'never seen' distinct from 'seen and unavailable', a
distinction that is invisible if you infer state by scanning output files.

The manifest holds three tables: ``matches`` (one row per match id, carrying its
lifecycle status), ``cursors`` (one row per discovery walk, recording where to
resume), and ``walks`` (one row per discovery walk, holding the parameters it
was created with and whether it has finished).
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Sequence
from datetime import UTC, datetime
from enum import StrEnum
from typing import Final

from dota_harvest.core.config import MANIFEST_PATH


class MatchStatus(StrEnum):
    """Lifecycle of a match id in the manifest.

    A row starts at :attr:`DISCOVERED` and moves to exactly one terminal state:

    ``DISCOVERED -> FETCHED``
        The detail request succeeded and the response was written to ``raw/``.
    ``DISCOVERED -> MISSING``
        STRATZ returned neither data nor an error, meaning it has no record of
        the match. A coverage fact, not a transient fault.
    ``DISCOVERED -> FAILED``
        STRATZ returned an error. Retryable while ``attempts`` stays below
        :data:`MAX_FETCH_ATTEMPTS`.

    Keeping ``MISSING`` and ``FAILED`` apart is what lets the retry logic and
    the coverage statistics each mean something.
    """

    DISCOVERED = "discovered"
    FETCHED = "fetched"
    MISSING = "missing"
    FAILED = "failed"


#: Give up on a match id after this many failed detail requests.
MAX_FETCH_ATTEMPTS: Final[int] = 3

#: Statuses that a fetch run should still attempt.
RETRYABLE_STATUSES: Final[tuple[MatchStatus, ...]] = (
    MatchStatus.DISCOVERED,
    MatchStatus.FAILED,
)

SCHEMA: Final[str] = """
CREATE TABLE IF NOT EXISTS matches (
    match_id      INTEGER PRIMARY KEY,
    source        TEXT    NOT NULL,   -- 'public' | 'pro'
    label         TEXT,               -- which discovery walk found it
    start_time    INTEGER,
    avg_rank_tier INTEGER,
    lobby_type    INTEGER,
    game_mode     INTEGER,
    status        TEXT    NOT NULL DEFAULT 'discovered',
    attempts      INTEGER NOT NULL DEFAULT 0,
    raw_file      TEXT,
    last_error    TEXT,
    discovered_at INTEGER
);
CREATE INDEX IF NOT EXISTS idx_status ON matches(status);
CREATE INDEX IF NOT EXISTS idx_start  ON matches(start_time);

CREATE TABLE IF NOT EXISTS cursors (
    key   TEXT PRIMARY KEY,           -- "{source}:{label}"
    value INTEGER
);

-- One row per discovery walk, holding the parameters it was created with.
-- Those are locked at creation: a walk's matches are only a coherent sample if
-- every page was collected under the same filters, so resuming reads them back
-- rather than trusting the operator to retype them identically.
CREATE TABLE IF NOT EXISTS walks (
    key        TEXT PRIMARY KEY,      -- "{source}:{label}"
    source     TEXT NOT NULL,
    label      TEXT NOT NULL,
    params     TEXT NOT NULL,         -- JSON, the locked run parameters
    state      TEXT NOT NULL,         -- 'open' | 'done'
    reason     TEXT,                  -- why it last stopped
    pages_done INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER,
    updated_at INTEGER
);
"""


def _migrate(conn: sqlite3.Connection) -> None:
    """Bring an older manifest up to the current schema.

    Args:
        conn: Open connection to the manifest database.

    Note:
        ``CREATE TABLE IF NOT EXISTS`` silently does nothing to a table that
        already exists, so a schema change leaves old databases intact and the
        mismatch only surfaces at query time. The ``raw/`` landing zone means a
        manifest could always be rebuilt, but discovery cursors and coverage
        history are worth keeping.
    """
    columns = {row[1] for row in conn.execute("PRAGMA table_info(matches)")}
    if columns and "label" not in columns:
        conn.execute("ALTER TABLE matches ADD COLUMN label TEXT")
        conn.execute("UPDATE matches SET label='main' WHERE label IS NULL")
        print("  manifest: added matches.label")

    cursor_columns = {row[1] for row in conn.execute("PRAGMA table_info(cursors)")}
    if cursor_columns and "key" not in cursor_columns:
        # The old layout keyed cursors by source alone. Those walks were all the
        # default label, so they map onto "{source}:main".
        rows = conn.execute("SELECT source, value FROM cursors").fetchall()
        conn.execute("DROP TABLE cursors")
        conn.execute("CREATE TABLE cursors (key TEXT PRIMARY KEY, value INTEGER)")
        conn.executemany(
            "INSERT INTO cursors(key, value) VALUES(?,?)",
            [(f"{source}:main", value) for source, value in rows],
        )
        print(f"  manifest: migrated {len(rows)} cursor(s) to labelled keys")

    conn.commit()


def connect() -> sqlite3.Connection:
    """Open the manifest, creating and migrating it as needed.

    Returns:
        A connection with the current schema applied. The parent directory is
        created if absent, so a fresh checkout works without setup.
    """
    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(MANIFEST_PATH)
    _migrate(conn)
    conn.executescript(SCHEMA)
    return conn


def get_cursor(conn: sqlite3.Connection, key: str) -> int | None:
    """Read where a discovery walk left off.

    Args:
        conn: Open manifest connection.
        key: Walk identifier, formatted ``"{source}:{label}"``.

    Returns:
        The lowest match id seen by that walk, or ``None`` if it has not run.
    """
    row = conn.execute("SELECT value FROM cursors WHERE key=?", (key,)).fetchone()
    return row[0] if row else None


def set_cursor(conn: sqlite3.Connection, key: str, value: int) -> None:
    """Record where a discovery walk has reached, committing immediately.

    Args:
        conn: Open manifest connection.
        key: Walk identifier, formatted ``"{source}:{label}"``.
        value: Lowest match id seen so far by this walk.

    Note:
        Committed per page rather than per run so an interrupted walk resumes
        from the last completed page instead of restarting.
    """
    conn.execute(
        "INSERT INTO cursors(key, value) VALUES(?,?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value),
    )
    conn.commit()


class WalkState(StrEnum):
    """Whether a discovery walk has more ground to cover.

    ``OPEN``
        The run stopped before exhausting its range -- quota spent, daily
        reserve reached, or the page budget used up. Resumable.
    ``DONE``
        The walk reached its ``--until`` floor or the archive ran out. Nothing
        remains to collect under these parameters.
    """

    OPEN = "open"
    DONE = "done"


#: Why a walk last stopped. The first two mean the walk is finished; the rest
#: leave it resumable.
STOP_REACHED_FLOOR: Final[str] = "reached --until floor"
STOP_ARCHIVE_EXHAUSTED: Final[str] = "no more results"
STOP_PAGE_BUDGET: Final[str] = "ran out of --pages"
STOP_QUOTA: Final[str] = "daily API quota exhausted"
STOP_RESERVE: Final[str] = "hit --reserve"
STOP_INTERRUPTED: Final[str] = "interrupted"

#: Stop reasons that mean the walk has nothing left to collect.
TERMINAL_REASONS: Final[frozenset[str]] = frozenset({STOP_REACHED_FLOOR, STOP_ARCHIVE_EXHAUSTED})

#: Parameters that bound how far a run travels rather than which matches it
#: keeps. A resume may change these without making the walk's rows
#: heterogeneous, so they are not locked; everything else is.
RANGE_PARAMS: Final[frozenset[str]] = frozenset({"pages", "until_ts"})


def walk_key(source: str, label: str) -> str:
    """Build the canonical identifier for a walk."""
    return f"{source}:{label}"


def start_walk(
    conn: sqlite3.Connection,
    source: str,
    label: str,
    params: dict[str, object],
) -> dict[str, object]:
    """Register a walk's parameters, or return the ones already stored.

    Args:
        conn: Open manifest connection.
        source: Which endpoint the walk draws from.
        label: The walk's label.
        params: Parameters this run was invoked with.

    Returns:
        The parameters the walk is bound to. For a walk seen before, these are
        the stored filters, with any key in :data:`RANGE_PARAMS` taken from
        ``params`` instead.

    Note:
        Filters are locked at creation. A walk's rows are only a coherent sample
        if every page passed through the same ones, so a later run under
        different settings would silently produce a mixed population that
        nothing downstream could separate.

        :data:`RANGE_PARAMS` are exempt because they bound how far a run
        travels rather than which matches qualify. Their new values are written
        back, so the stored record keeps describing the walk as it actually ran.
    """
    key = walk_key(source, label)
    row = conn.execute("SELECT params FROM walks WHERE key = ?", (key,)).fetchone()
    now = int(datetime.now(UTC).timestamp())

    if row is not None:
        stored = json.loads(row[0])
        merged = {**stored, **{name: params[name] for name in RANGE_PARAMS if name in params}}
        if merged != stored:
            conn.execute(
                "UPDATE walks SET params = ?, updated_at = ? WHERE key = ?",
                (json.dumps(merged, sort_keys=True), now, key),
            )
            conn.commit()
        return merged

    conn.execute(
        "INSERT INTO walks (key, source, label, params, state, created_at, updated_at) "
        "VALUES (?,?,?,?,?,?,?)",
        (key, source, label, json.dumps(params, sort_keys=True), WalkState.OPEN.value, now, now),
    )
    conn.commit()
    return dict(params)


def finish_walk(
    conn: sqlite3.Connection,
    source: str,
    label: str,
    reason: str,
    pages_done: int,
) -> None:
    """Record how a walk's run ended.

    Args:
        conn: Open manifest connection.
        source: Which endpoint the walk draws from.
        label: The walk's label.
        reason: One of the ``STOP_*`` constants.
        pages_done: Pages completed across the walk's lifetime, cumulative.

    Note:
        ``pages_done`` accumulates rather than replacing, so a walk resumed
        several times reports total progress rather than the last run's.
    """
    state = WalkState.DONE if reason in TERMINAL_REASONS else WalkState.OPEN
    conn.execute(
        "UPDATE walks SET state = ?, reason = ?, pages_done = pages_done + ?, updated_at = ? "
        "WHERE key = ?",
        (
            state.value,
            reason,
            pages_done,
            int(datetime.now(UTC).timestamp()),
            walk_key(source, label),
        ),
    )
    conn.commit()


def get_walk(conn: sqlite3.Connection, selector: str) -> dict[str, object] | None:
    """Look up one walk's stored record.

    Args:
        conn: Open manifest connection.
        selector: ``"label"`` or ``"source:label"``.

    Returns:
        The walk's record, or ``None`` when no walk matches.

    Raises:
        ValueError: If the selector is malformed, or if a bare label is
            ambiguous across sources -- resuming the wrong one would append to
            an unrelated sample.
    """
    source, label = parse_walk(selector)
    if source is None:
        rows = conn.execute("SELECT * FROM walks WHERE label = ?", (label,)).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM walks WHERE source = ? AND label = ?", (source, label)
        ).fetchall()

    if not rows:
        return None
    if len(rows) > 1:
        keys = ", ".join(row[0] for row in rows)
        raise ValueError(f"{label!r} is ambiguous; qualify it as one of: {keys}")

    columns = [column[0] for column in conn.execute("SELECT * FROM walks LIMIT 0").description]
    record = dict(zip(columns, rows[0], strict=True))
    record["params"] = json.loads(record["params"])
    return record


def list_walks(conn: sqlite3.Connection) -> list[dict[str, object]]:
    """List every registered walk, most recently updated first.

    Args:
        conn: Open manifest connection.

    Returns:
        One record per walk, with ``params`` decoded.
    """
    columns = [column[0] for column in conn.execute("SELECT * FROM walks LIMIT 0").description]
    records = []
    for row in conn.execute("SELECT * FROM walks ORDER BY updated_at DESC"):
        record = dict(zip(columns, row, strict=True))
        record["params"] = json.loads(record["params"])
        records.append(record)
    return records


def parse_walk(selector: str) -> tuple[str | None, str]:
    """Split a walk selector into its optional source and its label.

    Args:
        selector: Either ``"label"`` or ``"source:label"``, mirroring the
            cursor key format.

    Returns:
        A ``(source, label)`` pair. ``source`` is ``None`` for a bare label,
        meaning "this label under any source".

    Raises:
        ValueError: If either half is empty, which would silently select
            everything or nothing.

    Note:
        Labels are only unique per source -- the cursor key is ``source:label``
        -- so a bare label can name more than one walk. Accepting both forms
        keeps the common case short while leaving a way to disambiguate.
    """
    source, separator, label = selector.rpartition(":")
    if not separator:
        source, label = None, selector
    if not label.strip() or (source is not None and not source.strip()):
        raise ValueError(f"malformed walk selector: {selector!r}")
    return (source.strip() if source else None), label.strip()


def _walk_clause(selectors: Sequence[str]) -> tuple[str, list[str]]:
    """Build a SQL predicate matching any of several walk selectors.

    Args:
        selectors: Walk selectors in either accepted form.

    Returns:
        A ``(sql, params)`` pair. The SQL is a parenthesised ``OR`` chain
        suitable for embedding in a ``WHERE`` clause; params are bound, never
        interpolated.
    """
    terms: list[str] = []
    params: list[str] = []
    for selector in selectors:
        source, label = parse_walk(selector)
        if source is None:
            terms.append("label = ?")
            params.append(label)
        else:
            terms.append("(source = ? AND label = ?)")
            params.extend((source, label))
    return "(" + " OR ".join(terms) + ")", params


def known_walks(conn: sqlite3.Connection) -> list[tuple[str, str, int]]:
    """List every walk present in the manifest.

    Args:
        conn: Open manifest connection.

    Returns:
        One ``(source, label, count)`` triple per walk, largest first. Used to
        suggest alternatives when a selector matches nothing.
    """
    return [
        (row[0], row[1], row[2])
        for row in conn.execute(
            "SELECT source, label, COUNT(*) FROM matches "
            "GROUP BY source, label ORDER BY COUNT(*) DESC"
        )
    ]


def pending_ids(
    conn: sqlite3.Connection,
    limit: int,
    walks: Sequence[str] | None = None,
) -> list[int]:
    """Select match ids still awaiting a successful detail fetch.

    Args:
        conn: Open manifest connection.
        limit: Maximum number of ids to return.
        walks: Restrict to these walk selectors (``"label"`` or
            ``"source:label"``). ``None`` considers every walk.

    Returns:
        Up to ``limit`` ids in :data:`RETRYABLE_STATUSES` that have not yet
        exhausted :data:`MAX_FETCH_ATTEMPTS`, newest match first so a partial
        run collects the most recent data.

    Raises:
        ValueError: If a walk selector is malformed.
    """
    status_slots = ",".join("?" * len(RETRYABLE_STATUSES))
    params: list[object] = [status.value for status in RETRYABLE_STATUSES]
    params.append(MAX_FETCH_ATTEMPTS)

    walk_sql = ""
    if walks:
        clause, walk_params = _walk_clause(walks)
        walk_sql = f" AND {clause}"
        params.extend(walk_params)
    params.append(limit)

    rows = conn.execute(
        f"SELECT match_id FROM matches "  # noqa: S608 - placeholders, not values
        f"WHERE status IN ({status_slots}) AND attempts < ?{walk_sql} "
        f"ORDER BY start_time DESC LIMIT ?",
        params,
    ).fetchall()
    return [row[0] for row in rows]


def remove_walks(
    conn: sqlite3.Connection,
    walks: Sequence[str],
    *,
    include_fetched: bool = False,
) -> dict[str, int]:
    """Delete a walk's match rows and its discovery cursor.

    Args:
        conn: Open manifest connection.
        walks: Walk selectors to remove.
        include_fetched: Also delete rows whose detail was already downloaded.

    Returns:
        A tally keyed by what happened: one entry per deleted status, plus
        ``"kept_fetched"`` for rows preserved and ``"cursors"`` for cursors
        dropped.

    Raises:
        ValueError: If a walk selector is malformed.

    Note:
        Fetched rows are preserved by default because their responses are
        already on disk in ``raw/``. Deleting the row would orphan that data:
        the manifest is what maps a stored response back to a match id, so a
        transform would still read it while nothing recorded where it came from.
    """
    clause, params = _walk_clause(walks)

    tally: dict[str, int] = {}
    for status, count in conn.execute(
        f"SELECT status, COUNT(*) FROM matches WHERE {clause} GROUP BY status",  # noqa: S608
        params,
    ).fetchall():
        tally[status] = count

    kept = 0
    if not include_fetched:
        kept = tally.pop(MatchStatus.FETCHED.value, 0)
        deleted = conn.execute(
            f"DELETE FROM matches WHERE {clause} AND status != ?",  # noqa: S608
            [*params, MatchStatus.FETCHED.value],
        ).rowcount
    else:
        deleted = conn.execute(
            f"DELETE FROM matches WHERE {clause}",  # noqa: S608
            params,
        ).rowcount

    # Drop cursors last: a walk with no rows left should not resume mid-history.
    cursors = 0
    for source, label in (parse_walk(selector) for selector in walks):
        if source is None:
            cursors += conn.execute(
                "DELETE FROM cursors WHERE key LIKE '%:' || ?", (label,)
            ).rowcount
            conn.execute("DELETE FROM walks WHERE label = ?", (label,))
        else:
            cursors += conn.execute(
                "DELETE FROM cursors WHERE key = ?", (walk_key(source, label),)
            ).rowcount
            conn.execute("DELETE FROM walks WHERE key = ?", (walk_key(source, label),))
    conn.commit()

    tally["deleted"] = max(deleted, 0)
    tally["kept_fetched"] = kept
    tally["cursors"] = max(cursors, 0)
    return tally


def parse_date(value: str) -> int:
    """Convert a ``YYYY-MM-DD`` string to a UTC Unix timestamp.

    Args:
        value: Date in ``YYYY-MM-DD`` form.

    Returns:
        Seconds since the epoch at UTC midnight on that date.

    Raises:
        ValueError: If ``value`` is not in ``YYYY-MM-DD`` form.
    """
    return int(datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=UTC).timestamp())


def fmt_date(timestamp: int | float) -> str:
    """Format a Unix timestamp as a ``YYYY-MM-DD`` UTC date.

    Args:
        timestamp: Seconds since the epoch.

    Returns:
        The UTC calendar date, for display in progress output.
    """
    return datetime.fromtimestamp(timestamp, UTC).strftime("%Y-%m-%d")
