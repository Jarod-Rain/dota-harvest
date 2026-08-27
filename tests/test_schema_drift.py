"""The invariant that the query, the pinned schema, and the data agree.

This is the regression guard for the bug that motivated the drift check: fields
were downloaded but silently dropped by the reader, and nothing noticed because
each side was inspected on its own.
"""

from __future__ import annotations

from dota_harvest.api.clients import selected_fields
from dota_harvest.diagnostics import _flatten, _observed_fields
from dota_harvest.pipeline.transform import RAW_COLUMNS, pinned_fields


def test_query_and_pinned_schema_agree_exactly():
    """Every downloaded field is readable, and nothing pinned goes unrequested."""
    queried = _flatten(selected_fields())
    pinned = _flatten(pinned_fields())
    assert queried - pinned == set(), "downloaded but not readable by the transform"
    assert pinned - queried == set(), "pinned but never requested; would be all-NULL"


def test_pinned_fields_parses_nested_structs():
    """Nested STRUCTs and list markers resolve to a plain name tree."""
    tree = pinned_fields({"players": "STRUCT(a BIGINT, b STRUCT(c BIGINT))[]"})
    assert tree == {"players": {"a": {}, "b": {"c": {}}}}


def test_pinned_fields_strips_quoted_names():
    """Reserved words are quoted in SQL but must compare unquoted."""
    assert pinned_fields({"m": 'STRUCT("time" BIGINT)'}) == {"m": {"time": {}}}


def test_selected_fields_handles_inline_braces_and_comments():
    """A one-line selection set parses the same as a multi-line block."""
    tree = selected_fields("""
        id
        # a comment
        players { heroId stats { campStack } }
    """)
    assert tree == {"id": {}, "players": {"heroId": {}, "stats": {"campStack": {}}}}


def test_observed_fields_unions_across_list_elements():
    """A field present on only one element still counts as observed."""
    match = {"players": [{"a": 1}, {"b": 2}]}
    assert _observed_fields(match) == {"players", "players.a", "players.b"}


def test_raw_columns_declares_players_as_a_list():
    """The players column must stay a list for UNNEST to work."""
    assert RAW_COLUMNS["players"].endswith("[]")
