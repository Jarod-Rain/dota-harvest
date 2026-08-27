"""Configuration and persistent state shared by every other layer.

This layer knows nothing about HTTP or Dota: it resolves where data lives
(:mod:`~dota_harvest.core.config`) and tracks which match ids have been seen and
what happened to them (:mod:`~dota_harvest.core.manifest`).
"""

from dota_harvest.core.config import DATA_DIR, PARQUET_DIR, RAW_DIR, describe_paths
from dota_harvest.core.manifest import MatchStatus, connect, fmt_date, parse_date
from dota_harvest.core.types import FieldTree, JSONMapping

__all__ = [
    "DATA_DIR",
    "PARQUET_DIR",
    "RAW_DIR",
    "FieldTree",
    "JSONMapping",
    "MatchStatus",
    "connect",
    "describe_paths",
    "fmt_date",
    "parse_date",
]
