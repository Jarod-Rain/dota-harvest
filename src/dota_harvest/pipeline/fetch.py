"""Stage 2: pull match detail from STRATZ into the raw landing zone.

Raw responses are written verbatim as gzipped JSONL and never parsed here.
Feature extraction changes many times over a project; re-downloading two million
matches because a schema changed is the failure this prevents.
"""

from __future__ import annotations

import gzip
import json
import sqlite3
import sys
import time
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Final

from dota_harvest.api.clients import build_batch_query, parse_batch_response, stratz_query
from dota_harvest.api.http import QuotaExhaustedError
from dota_harvest.core.config import RAW_DIR, STRATZ_BATCH, STRATZ_SLEEP
from dota_harvest.core.manifest import MatchStatus, connect, pending_ids

#: Stop after this many consecutive batches return nothing. A run of empties
#: means something systemic -- a bad token, a schema break, too large a batch --
#: not a string of coincidences.
MAX_DEAD_STREAK: Final[int] = 3

#: Emit a progress line every N batches.
PROGRESS_EVERY: Final[int] = 25

SECONDS_PER_HOUR: Final[int] = 3600


def chunks(seq: list[int], size: int) -> Iterator[list[int]]:
    """Split a list into consecutive fixed-size chunks.

    Args:
        seq: Items to split.
        size: Maximum length of each chunk.

    Yields:
        Successive slices; the final chunk may be shorter.
    """
    for start in range(0, len(seq), size):
        yield seq[start : start + size]


def _mark(
    conn: sqlite3.Connection,
    ids: list[int],
    status: MatchStatus | str,
    error: str | None = None,
    errors: dict[int, str] | None = None,
) -> None:
    """Record the outcome of a fetch attempt for a set of match ids.

    Args:
        conn: Open manifest connection.
        ids: Match ids to update. An empty list is a no-op that still commits.
        status: New lifecycle status; a plain string is accepted too.
        error: One message shared by every id.
        errors: Per-id messages, taking precedence over ``error``.

    Note:
        ``status`` is bound rather than interpolated: the values are internal
        literals today, but this is the one statement that writes run state, and
        an injection-shaped f-string there is not worth the convenience.
    """
    # str() rather than .value: MatchStatus is a StrEnum, so both render to the
    # same text, and accepting a plain string keeps callers from having to
    # import the enum just to record a status.
    conn.executemany(
        "UPDATE matches SET status=?, attempts=attempts+1, last_error=? WHERE match_id=?",
        [(str(status), errors.get(mid) if errors else error, mid) for mid in ids],
    )
    conn.commit()


def _mark_fetched(conn: sqlite3.Connection, ids: list[int], raw_file: str) -> None:
    """Record match ids whose detail was successfully written to disk.

    Args:
        conn: Open manifest connection.
        ids: Match ids present in the response.
        raw_file: Name of the shard the responses were appended to, so a row can
            always be traced back to the file holding it.
    """
    conn.executemany(
        "UPDATE matches SET status=?, attempts=attempts+1, raw_file=? WHERE match_id=?",
        [(str(MatchStatus.FETCHED), raw_file, mid) for mid in ids],
    )
    conn.commit()


def _new_shard_path() -> Path:
    """Generate a unique filename for this run's output shard.

    Returns:
        A path under :data:`~dota_harvest.core.config.RAW_DIR` carrying a
        timestamp and a random suffix, so concurrent runs cannot collide.
    """
    return RAW_DIR / f"part-{int(time.time())}-{uuid.uuid4().hex[:8]}.jsonl.gz"


def _report(written: int, started: float, destination: Path) -> None:
    """Print the end-of-run summary.

    Args:
        written: Matches written this run.
        started: Monotonic-ish start time from :func:`time.time`.
        destination: Shard the matches were written to.
    """
    elapsed = time.time() - started
    rate = written / max(elapsed, 1) * SECONDS_PER_HOUR
    print(f"wrote {written:,} matches in {elapsed / 60:.1f} min ({rate:,.0f}/hr) -> {destination}")


def run(limit: int = 1000, batch: int = STRATZ_BATCH, sleep: float = STRATZ_SLEEP) -> int:
    """Fetch detail for pending match ids and append it to a new raw shard.

    Each batch becomes one aliased GraphQL request. Every id in the batch lands
    in exactly one terminal state: written to disk and marked fetched, marked
    failed with its own error message, or marked missing when STRATZ returns
    neither data nor an error.

    Args:
        limit: Maximum pending ids to attempt this run.
        batch: Aliased match lookups per request.
        sleep: Seconds to pause between requests.

    Returns:
        The number of matches written. Zero means the output shard was removed.

    Note:
        Safe to interrupt and re-run. Ids are only marked once their response is
        on disk, so anything unattempted stays pending.
    """
    conn = connect()
    ids = pending_ids(conn, limit)
    if not ids:
        print("nothing pending; run discover first")
        return 0

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    destination = _new_shard_path()
    print(f"{len(ids)} pending, batch={batch} -> {destination}")

    written = 0
    dead_streak = 0
    started = time.time()

    with gzip.open(destination, "wt", encoding="utf-8") as handle:
        for index, group in enumerate(chunks(ids, batch)):
            try:
                payload = stratz_query(build_batch_query(group))
            except QuotaExhaustedError as exc:
                # Pending ids stay pending; this batch was never attempted.
                print(f"\n{exc}\nStopping; re-run to resume.", file=sys.stderr)
                break
            except Exception as exc:  # noqa: BLE001 - any failure is per-batch
                _mark(conn, group, MatchStatus.FAILED, str(exc)[:300])
                print(f"  batch failed: {exc}", file=sys.stderr)
                time.sleep(sleep)
                continue

            found, errors = parse_batch_response(payload, group)
            for match in found.values():
                handle.write(json.dumps(match, separators=(",", ":")) + "\n")
            written += len(found)

            if found:
                dead_streak = 0
                _mark_fetched(conn, list(found), destination.name)
            else:
                dead_streak += 1

            # Each id carries its own message: a batch can fail for several
            # unrelated reasons at once, and collapsing them to whichever came
            # first makes last_error useless for diagnosing the rest.
            _mark(
                conn,
                [mid for mid in group if mid not in found and mid in errors],
                MatchStatus.FAILED,
                errors=errors,
            )
            # An id returning neither data nor an error is simply not in
            # STRATZ's index -- a coverage fact, not a transient fault. Keeping
            # it apart from 'failed' is what makes the retry logic and the
            # coverage statistics both mean something.
            _mark(
                conn,
                [mid for mid in group if mid not in found and mid not in errors],
                MatchStatus.MISSING,
            )

            if dead_streak >= MAX_DEAD_STREAK:
                print(
                    f"\n{MAX_DEAD_STREAK} consecutive empty batches; stopping. "
                    "Run `dota-harvest check`, and try a smaller --batch.",
                    file=sys.stderr,
                )
                break

            if (index + 1) % PROGRESS_EVERY == 0:
                rate = written / max(time.time() - started, 1) * SECONDS_PER_HOUR
                print(f"  {written:,} written ({rate:,.0f}/hr)")
            time.sleep(sleep)

    # A run that writes nothing -- quota exhausted on the first batch, or a dead
    # streak -- still creates an (empty) gzip. Leaving it behind litters raw/
    # with 0-record files that add nothing and only slow the next transform's
    # file scan, so drop it.
    if written == 0:
        destination.unlink(missing_ok=True)
        print("wrote 0 matches; removed empty output file")
        return 0

    _report(written, started, destination)
    return written
