"""Bounding DuckDB's memory so a large transform cannot be OOM-killed.

A 328,923-match corpus killed the transform at 15 GiB RSS with swap full. The
purchases table is a double UNNEST over every match, and DuckDB was buffering
the whole ordered result before writing it. These tests pin the three settings
that make the write stream instead.
"""

from __future__ import annotations

import duckdb
import pytest

from dota_harvest.pipeline.transform import (
    MEMORY_HEADROOM,
    MIN_MEMORY_LIMIT_BYTES,
    _configure_memory,
    available_memory_bytes,
)


@pytest.fixture
def con():
    connection = duckdb.connect()
    connection.execute("SET enable_progress_bar=false")
    yield connection
    connection.close()


def _setting(connection, name):
    return connection.execute(f"SELECT current_setting('{name}')").fetchone()[0]


def test_available_memory_is_read_from_the_kernel():
    """MemAvailable, not MemTotal: the budget must exclude other processes."""
    available = available_memory_bytes()
    assert available is None or available > 0


def test_insertion_order_is_released(con, tmp_path):
    """The expensive default: it buffers a partitioned COPY in full.

    Nothing downstream depends on row order within a partition, so streaming
    costs nothing and is the difference between 15 GiB and a bounded write.
    """
    _configure_memory(con, tmp_path)
    assert _setting(con, "preserve_insertion_order") is False


def test_the_spill_directory_is_absolute_and_exists(con, tmp_path):
    """DuckDB's relative '.tmp' default is not a usable spill target."""
    _configure_memory(con, tmp_path)
    spill = _setting(con, "temp_directory")
    assert spill == str(tmp_path / ".tmp")
    assert (tmp_path / ".tmp").is_dir()


def test_the_budget_leaves_headroom(con, tmp_path, monkeypatch):
    """Sizing to all of available RAM leaves nothing for Python or the readers."""
    monkeypatch.setattr(
        "dota_harvest.pipeline.transform.available_memory_bytes", lambda: 20 * 2**30
    )
    _configure_memory(con, tmp_path)

    limit = _setting(con, "memory_limit")
    assert limit != "20.0 GiB"
    assert MEMORY_HEADROOM < 1.0


def test_a_cramped_machine_gets_the_floor(con, tmp_path, monkeypatch):
    """Below the floor DuckDB spills so constantly a transform never finishes."""
    monkeypatch.setattr(
        "dota_harvest.pipeline.transform.available_memory_bytes", lambda: 256 * 2**20
    )
    _configure_memory(con, tmp_path)

    limit = _setting(con, "memory_limit")
    assert limit.endswith("GiB")
    assert float(limit.split()[0]) >= MIN_MEMORY_LIMIT_BYTES / 2**30 - 0.1


def test_an_unreadable_meminfo_leaves_the_default_limit(con, tmp_path, monkeypatch):
    """No reading is better than a wrong one; the other two settings still apply."""
    monkeypatch.setattr("dota_harvest.pipeline.transform.available_memory_bytes", lambda: None)
    before = _setting(con, "memory_limit")
    _configure_memory(con, tmp_path)

    assert _setting(con, "memory_limit") == before
    assert _setting(con, "preserve_insertion_order") is False
