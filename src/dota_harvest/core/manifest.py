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
        else:
            cursors += conn.execute(
                "DELETE FROM cursors WHERE key = ?", (f"{source}:{label}",)
            ).rowcount
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
