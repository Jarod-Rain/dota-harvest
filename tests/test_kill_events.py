"""The kill_events table: one row per kill, attributable to a hero.

players.kill_events keeps only a count, which cannot say when a hero got their
kills. This table keeps the timeline, keyed like purchases so both event streams
join to players on (match_id, hero_id).
"""

from __future__ import annotations

import json

import duckdb
import pytest

from dota_harvest.pipeline.transform import (
    KILL_EVENTS_SQL,
    OUTPUT_TABLES,
    PURCHASES_SQL,
    _columns_clause,
    _quote_sql_list,
)

#: Two matches: one parsed with kills on both sides, one with none at all.
MATCHES = [
    {
        "id": 1,
        "startDateTime": 1_700_000_000,
        "players": [
            {
                "heroId": 14,
                "isRadiant": True,
                "isVictory": True,
                "stats": {
                    "killEvents": [{"time": -79}, {"time": 249}, {"time": 600}],
                    "itemPurchases": [{"itemId": 40, "time": 120}],
                },
            },
            {
                "heroId": 23,
                "isRadiant": False,
                "isVictory": False,
                "stats": {"killEvents": [{"time": 300}]},
            },
        ],
    },
    {
        "id": 2,
        "startDateTime": 1_700_000_500,
        "players": [{"heroId": 8, "isRadiant": True, "isVictory": False, "stats": {}}],
    },
]


@pytest.fixture
def con(tmp_path):
    """A connection with `raw` and `patches` views over a tiny fixture."""
    shard = tmp_path / "part-fixture.jsonl"
    shard.write_text("\n".join(json.dumps(match) for match in MATCHES))

    connection = duckdb.connect()
    connection.execute("SET enable_progress_bar=false")
    connection.execute(
        f"CREATE VIEW raw AS SELECT * FROM read_json({_quote_sql_list([str(shard)])}, "
        f"format='newline_delimited', columns={_columns_clause()})"
    )
    connection.execute(
        "CREATE VIEW patches AS SELECT 'testpatch' AS name, 1699999999::BIGINT AS start_ts"
    )
    yield connection
    connection.close()


def _build(connection, tmp_path, statement=KILL_EVENTS_SQL, table="kill_events"):
    """Run one COPY statement and return its rows."""
    (tmp_path / "out").mkdir(exist_ok=True)
    connection.execute(statement.format(out=tmp_path / "out"))
    return connection.execute(
        f"SELECT * FROM read_parquet('{tmp_path}/out/{table}/**/*.parquet', "
        f"hive_partitioning=true) ORDER BY match_id, hero_id, kill_time_s"
    ).fetchall()


def test_the_table_is_one_of_the_declared_outputs():
    """_assert_tables_written and the staging swap both iterate this tuple."""
    assert "kill_events" in OUTPUT_TABLES


def test_one_row_per_kill_event(con, tmp_path):
    """The count must equal the number of array elements, not of players."""
    assert len(_build(con, tmp_path)) == 4


def test_every_kill_carries_its_hero(con, tmp_path):
    """The whole point: a kill is attributable to one hero, not to a side."""
    rows = _build(con, tmp_path)
    assert [row[1] for row in rows] == [14, 14, 14, 23]


def test_kill_times_survive_unrounded(con, tmp_path):
    rows = _build(con, tmp_path)
    assert [row[4] for row in rows if row[1] == 14] == [-79, 249, 600]


def test_a_pre_horn_kill_is_kept(con, tmp_path):
    """Negative timestamps are real -- courier and creep-block kills."""
    assert any(row[4] < 0 for row in _build(con, tmp_path))


def test_a_player_with_no_kill_log_contributes_nothing(con, tmp_path):
    """Match 2 has no killEvents at all, so it must not appear."""
    assert {row[0] for row in _build(con, tmp_path)} == {1}


def test_the_side_and_result_ride_along(con, tmp_path):
    """Enough context to filter without joining players for the common case."""
    radiant = [row for row in _build(con, tmp_path) if row[1] == 14]
    assert all(row[2] is True and row[3] is True for row in radiant)


def test_the_columns_match_the_purchases_shape(con, tmp_path):
    """Both event tables must key alike, or they cannot be joined together."""
    (tmp_path / "out").mkdir(exist_ok=True)
    con.execute(KILL_EVENTS_SQL.format(out=tmp_path / "out"))
    con.execute(PURCHASES_SQL.format(out=tmp_path / "out"))

    def columns(table):
        return [
            row[0]
            for row in con.execute(
                f"DESCRIBE SELECT * FROM read_parquet("
                f"'{tmp_path}/out/{table}/**/*.parquet', hive_partitioning=true)"
            ).fetchall()
        ]

    shared = ["match_id", "hero_id", "is_radiant", "is_victory", "patch"]
    assert all(name in columns("kill_events") for name in shared)
    assert all(name in columns("purchases") for name in shared)


def test_the_partition_key_is_the_patch(con, tmp_path):
    """Partitioning must match the other tables so scans prune alike."""
    assert all(row[5] == "testpatch" for row in _build(con, tmp_path))
