"""Paths, endpoints, and tuning constants.

Every magic number that came out of empirical probing lives here with the
finding that produced it, so the reasoning survives when the number changes.
See ``DATA.md`` for the evidence behind each.

Path resolution happens once at import time. Constants are annotated with
``Final`` to make the intent explicit and let type checkers flag accidental
rebinding.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Final

# --- paths ---------------------------------------------------------------

#: Filenames that mark the root of a checkout.
PROJECT_MARKERS: Final[tuple[str, ...]] = ("pyproject.toml", ".git")


def find_project_root() -> Path | None:
    """Locate the project root by walking up from the package and the cwd.

    Returns:
        The nearest ancestor directory containing any of :data:`PROJECT_MARKERS`,
        or ``None`` when the package is installed outside a checkout.

    Note:
        The package location is checked first, then the working directory. The
        package location wins when running from a checkout; it finds nothing
        when pip-installed into site-packages, which is exactly when the cwd
        search is the right answer.
    """
    for start in (Path(__file__).resolve().parent, Path.cwd().resolve()):
        for candidate in (start, *start.parents):
            if any((candidate / marker).exists() for marker in PROJECT_MARKERS):
                return candidate
    return None


def _default_data_dir(root: Path | None) -> Path:
    """Choose a data directory when ``DOTA_DATA_DIR`` is unset.

    Args:
        root: Project root, or ``None`` if the package runs outside a checkout.

    Returns:
        ``<root>/data`` inside a checkout, else an XDG-compliant user data
        directory. The XDG fallback avoids scattering multi-gigabyte
        collections wherever the user happened to be standing.
    """
    if root is not None:
        return root / "data"
    base = os.environ.get("XDG_DATA_HOME") or (Path.home() / ".local" / "share")
    return Path(base) / "dota-harvest"


def _resolve_data_dir(configured: str | None, root: Path | None) -> Path:
    """Resolve the configured data directory to an absolute path.

    Args:
        configured: Raw ``DOTA_DATA_DIR`` value, if set.
        root: Project root, used to anchor relative values.

    Returns:
        An absolute, symlink-resolved data directory.

    Note:
        Relative values resolve against the project root, NOT the working
        directory. That is what makes a checked-in ``DOTA_DATA_DIR=../dota-data``
        mean the same thing on every machine and from every directory --
        resolving against cwd would silently point somewhere different depending
        on where you invoked from, and an absolute path would not survive being
        cloned elsewhere.
    """
    if not configured:
        return _default_data_dir(root).resolve()
    path = Path(configured).expanduser()
    if path.is_absolute() or root is None:
        return path.resolve()
    return (root / path).resolve()


def _load_dotenv() -> None:
    """Load ``.env`` from the project root, if python-dotenv is installed.

    Note:
        Reads the project root explicitly rather than letting python-dotenv walk
        up from the cwd, which would pick up a different ``.env`` (or none)
        depending on where the command was run. A missing dependency is not an
        error: the environment may already carry the credentials.
    """
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    if PROJECT_ROOT is not None:
        load_dotenv(PROJECT_ROOT / ".env")
    else:
        load_dotenv()


PROJECT_ROOT: Final[Path | None] = find_project_root()

_load_dotenv()

DATA_DIR: Final[Path] = _resolve_data_dir(os.environ.get("DOTA_DATA_DIR"), PROJECT_ROOT)
RAW_DIR: Final[Path] = DATA_DIR / "raw" / "stratz"
PARQUET_DIR: Final[Path] = DATA_DIR / "parquet"
MANIFEST_PATH: Final[Path] = DATA_DIR / "manifest.sqlite"

# Patch overrides live with the data, not with the source. Deriving this from
# __file__ works when running out of a checkout and breaks silently once the
# package is pip-installed into site-packages -- it would point at a read-only
# directory inside the venv that the user has no reason to look in.
OVERRIDE_PATH: Final[Path] = Path(
    os.environ.get("DOTA_PATCHES_OVERRIDE", DATA_DIR / "patches_override.json")
).expanduser()

# --- endpoints -----------------------------------------------------------

STRATZ_URL: Final[str] = "https://api.stratz.com/graphql"
OPENDOTA_URL: Final[str] = "https://api.opendota.com/api"

# STRATZ 403s any request without this exact User-Agent. Documented only on the
# English version of their API page; the failure looks like an IP ban.
STRATZ_UA: Final[str] = "STRATZ_API"

# --- rate limiting -------------------------------------------------------

# STRATZ default tier is widely cited as 2000 req/hour. UNVERIFIED against
# current docs -- measure before relying on it for multi-day jobs.
STRATZ_SLEEP: Final[float] = 2.0

#: Aliased match lookups per GraphQL request.
STRATZ_BATCH: Final[int] = 6

#: 60 req/min on the free tier, with headroom.
OPENDOTA_SLEEP: Final[float] = 1.3

# Free tier is also capped at 3000 calls/day (docs.opendota.com). The daily
# counter arrives as X-Rate-Limit-Remaining-Day on every response; discover
# stops on it rather than retrying, since a daily window cannot be waited out.
OPENDOTA_DAILY_CALLS: Final[int] = 3000

# --- empirical findings --------------------------------------------------

# STRATZ stopped minting gameVersionIds after 7.40b; everything since is 182.
# Verified 2026-08: matches from Mar/Jun/Aug 2026 all report 182, while
# 2024-2025 matches resolve correctly to 178/179/180/181.
STRATZ_VERSION_FROZEN_AFTER: Final[int] = 182

#: 2025-12-24, release of 7.40b -- the last patch STRATZ labelled correctly.
STRATZ_TRUSTED_UNTIL_TS: Final[int] = 1766534400

# OpenDota /publicMatches is a rolling ~365-day window. /proMatches goes back
# to at least 2019. Verified 2026-08: data at 365d, empty at 380d.
PUBLICMATCHES_RETENTION_DAYS: Final[int] = 365

#: STRATZ ingest trails OpenDota; matches younger than this often 404.
MIN_MATCH_AGE_HOURS: Final[int] = 48


def describe_paths() -> str:
    """Summarise where every configured path resolved to.

    Returns:
        An indented, human-readable block listing the project root, data
        directory (annotated with whether it came from ``DOTA_DATA_DIR`` or the
        default), and each derived path.
    """
    source = "DOTA_DATA_DIR" if os.environ.get("DOTA_DATA_DIR") else "default"
    root = str(PROJECT_ROOT) if PROJECT_ROOT else "(none found — using XDG data dir)"
    return "\n".join(
        [
            f"  project root   {root}",
            f"  data dir       {DATA_DIR}   [{source}]",
            f"  raw            {RAW_DIR}",
            f"  parquet        {PARQUET_DIR}",
            f"  manifest       {MANIFEST_PATH}",
            f"  patch override {OVERRIDE_PATH}",
        ]
    )
