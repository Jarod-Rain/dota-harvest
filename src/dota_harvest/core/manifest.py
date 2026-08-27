"""SQLite manifest tracking every match id the project knows about.

Cheap, transactional, and survives crashes -- which matters when a fetch run
lasts a day. It also keeps 'never seen' distinct from 'seen and unavailable', a
distinction that is invisible if you infer state by scanning output files.

The manifest holds two tables: ``matches`` (one row per match id, carrying its
lifecycle status) and ``cursors`` (one row per discovery walk, recording where
to resume).
"""

from __future__ import annotations

import sqlite3
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


def pending_ids(conn: sqlite3.Connection, limit: int) -> list[int]:
    """Select match ids still awaiting a successful detail fetch.

    Args:
        conn: Open manifest connection.
        limit: Maximum number of ids to return.

    Returns:
        Up to ``limit`` ids in :data:`RETRYABLE_STATUSES` that have not yet
        exhausted :data:`MAX_FETCH_ATTEMPTS`, newest match first so a partial
        run collects the most recent data.
    """
    placeholders = ",".join("?" * len(RETRYABLE_STATUSES))
    rows = conn.execute(
        f"SELECT match_id FROM matches "  # noqa: S608 - placeholders, not values
        f"WHERE status IN ({placeholders}) AND attempts < ? "
        f"ORDER BY start_time DESC LIMIT ?",
        (*[s.value for s in RETRYABLE_STATUSES], MAX_FETCH_ATTEMPTS, limit),
    ).fetchall()
    return [row[0] for row in rows]


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
