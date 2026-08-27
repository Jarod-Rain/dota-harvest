"""Aliased batch responses, where partial success is the normal case."""

from __future__ import annotations

from dota_harvest.api.clients import build_batch_query, parse_batch_response


def test_aliases_every_id_in_the_batch():
    query = build_batch_query([111, 222])
    assert query.count("match(id:") == 2
    assert "m0: match(id: 111)" in query
    assert "m1: match(id: 222)" in query


def test_splits_partial_success_by_id():
    """One bad id must not discard its siblings' data."""
    payload = {"data": {"m0": {"id": 10}}, "errors": [{"path": ["m1"], "message": "boom"}]}
    found, errors = parse_batch_response(payload, [10, 20])
    assert found == {10: {"id": 10}}
    assert errors == {20: "boom"}


def test_pathless_error_applies_to_every_id():
    """No path means the whole query was rejected, not one alias."""
    payload = {"data": None, "errors": [{"message": "User is not an admin."}]}
    found, errors = parse_batch_response(payload, [1, 2])
    assert found == {}
    assert errors == {1: "User is not an admin.", 2: "User is not an admin."}


def test_each_id_keeps_its_own_error():
    """Collapsing to the first message makes last_error useless for the rest."""
    payload = {
        "data": {},
        "errors": [{"path": ["m0"], "message": "first"}, {"path": ["m1"], "message": "second"}],
    }
    _, errors = parse_batch_response(payload, [10, 20])
    assert errors == {10: "first", 20: "second"}


def test_id_with_neither_data_nor_error_is_absent_from_both():
    """That is 'missing' -- a coverage fact, distinct from a failure."""
    found, errors = parse_batch_response({"data": {"m0": None}}, [10])
    assert found == {} and errors == {}


def test_unmappable_alias_does_not_fail_the_batch():
    payload = {"data": {"m0": {"id": 10}}, "errors": [{"path": ["mZZ"], "message": "?"}]}
    found, _ = parse_batch_response(payload, [10])
    assert found == {10: {"id": 10}}


def test_error_messages_are_truncated():
    payload = {"data": {}, "errors": [{"path": ["m0"], "message": "x" * 5000}]}
    _, errors = parse_batch_response(payload, [10])
    assert len(errors[10]) == 300
