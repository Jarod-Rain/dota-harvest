"""Shared type aliases.

Defined once here so the same shape is used everywhere a nested field tree or a
decoded JSON object crosses a module boundary -- notably the schema-drift check,
which compares trees produced by two different layers.
"""

from __future__ import annotations

from typing import Any, TypeAlias

#: A decoded JSON object.
JSONMapping: TypeAlias = dict[str, Any]

#: A nested field tree; leaves map to an empty mapping.
FieldTree: TypeAlias = dict[str, "FieldTree"]

__all__ = ["FieldTree", "JSONMapping"]
