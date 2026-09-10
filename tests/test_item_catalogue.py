"""The two item catalogues and the gap report that compares them.

STRATZ and OpenDota disagree about which items exist, in both directions:
STRATZ keeps ids for items Valve removed, and OpenDota lists current items
STRATZ has not added. The gap report used to name only the first direction, so
twelve purchasable 7.41 items missing from STRATZ went unmentioned -- the same
ids STRATZ reports in inventories but never in a purchase log. These tests pin
both directions, that item list, and the recipe graph the taxonomy is built on.
"""

from __future__ import annotations

import duckdb
import pytest

from dota_harvest.diagnostics import UNLOGGED_ITEMS, check_unlogged_items
from dota_harvest.pipeline.reference import _build_item_rows, _report_catalogue_gaps

#: A miniature catalogue: one build, its recipe, a consumable, and a neutral.
RAW_ITEMS = {
    "blink": {"id": 1, "dname": "Blink Dagger", "cost": 2250, "qual": "artifact"},
    "ultimate_orb": {"id": 2, "dname": "Ultimate Orb", "cost": 2050, "qual": "secret_shop"},
    "recipe_manta": {"id": 3, "dname": "Manta Style Recipe", "cost": 500},
    "manta": {
        "id": 4,
        "dname": "Manta Style",
        "cost": 4600,
        "qual": "artifact",
        "components": ["ultimate_orb", "recipe_manta"],
    },
    "tango": {"id": 5, "dname": "Tango", "cost": 90, "qual": "consumable"},
    "no_id_item": {"dname": "Placeholder"},
}


def _rows_by_id():
    return {row["item_id"]: row for row in _build_item_rows(RAW_ITEMS)}


# --- the recipe graph ----------------------------------------------------


def test_items_without_an_id_are_skipped():
    """A record with no id cannot be joined to a purchase row."""
    assert len(_build_item_rows(RAW_ITEMS)) == 5


def test_components_resolve_to_ids():
    assert _rows_by_id()[4]["components"] == [2, 3]


def test_built_into_is_the_reverse_edge():
    """Both directions are needed: one to collapse builds, one to walk upgrades."""
    rows = _rows_by_id()
    assert rows[2]["built_into"] == [4]
    assert rows[3]["built_into"] == [4]


def test_a_terminal_item_builds_into_nothing():
    assert _rows_by_id()[4]["built_into"] == []


def test_recipes_are_flagged_by_name():
    rows = _rows_by_id()
    assert rows[3]["is_recipe"] is True
    assert rows[4]["is_recipe"] is False


def test_consumables_are_flagged_by_quality():
    """is_consumable drives the biggest cut in the downstream action space."""
    rows = _rows_by_id()
    assert rows[5]["is_consumable"] is True
    assert rows[1]["is_consumable"] is False


def test_an_unresolvable_component_is_dropped_not_fatal():
    """A component naming an item that carries no id must not fail the build."""
    raw = {
        "widget": {"id": 9, "dname": "Widget", "cost": 100, "components": ["ghost", "nothing"]},
        "ghost": {"dname": "Ghost"},
    }
    assert _build_item_rows(raw)[0]["components"] == []


# --- the two-way gap report ----------------------------------------------


@pytest.fixture
def catalogues(tmp_path):
    """items_meta with a new item STRATZ lacks; items.parquet with a removed one."""
    con = duckdb.connect()
    con.execute("SET enable_progress_bar=false")
    con.execute(
        "CREATE TABLE meta AS SELECT * FROM (VALUES "
        "(1, 'Blink Dagger', 2250), "
        "(1858, 'Hydra''s Breath', 5900), "
        "(9999, 'Cosmetic', 0)) AS t(item_id, display_name, cost)"
    )
    con.execute(f"COPY meta TO '{tmp_path}/items_meta.parquet' (FORMAT PARQUET)")
    con.execute(
        "CREATE TABLE st AS SELECT * FROM (VALUES "
        "(1, 'Blink Dagger'), (213, 'Tranquil Boots Recipe')) AS t(id, displayName)"
    )
    con.execute(f"COPY st TO '{tmp_path}/items.parquet' (FORMAT PARQUET)")
    return con, tmp_path


def test_the_report_names_items_stratz_is_missing(catalogues, capsys):
    """The bug: this direction was never reported, so new items stayed hidden."""
    con, tmp_path = catalogues
    _report_catalogue_gaps(con, tmp_path, tmp_path / "items.parquet")

    out = capsys.readouterr().out
    assert "1 purchasable OpenDota items absent from STRATZ" in out
    assert "Hydra's Breath" in out


def test_the_report_still_names_the_other_direction(catalogues, capsys):
    con, tmp_path = catalogues
    _report_catalogue_gaps(con, tmp_path, tmp_path / "items.parquet")
    assert "1 STRATZ item ids absent from OpenDota" in capsys.readouterr().out


def test_unpurchasable_items_are_not_reported(catalogues, capsys):
    """Cosmetics drift constantly and would bury a real omission in noise."""
    con, tmp_path = catalogues
    _report_catalogue_gaps(con, tmp_path, tmp_path / "items.parquet")
    assert "Cosmetic" not in capsys.readouterr().out


# --- items STRATZ never logs a purchase for -------------------------------


def test_unlogged_items_are_the_ids_missing_from_stratz_constants():
    """The two gaps have the same root: an id STRATZ's catalogue omits.

    STRATZ returns these in match inventories but never in itemPurchases, and
    also omits them from `constants { items }`. Measured at 0% logged against
    91% for every other held item, on fully parsed matches.
    """
    assert 1852 in UNLOGGED_ITEMS  # Essence Distiller
    assert 1872 in UNLOGGED_ITEMS  # Chasm Stone
    assert len(UNLOGGED_ITEMS) == 12


def test_unlogged_items_are_sorted_and_unique():
    """The list is cited in docs and compared against; drift would confuse."""
    assert list(UNLOGGED_ITEMS) == sorted(set(UNLOGGED_ITEMS))


def test_the_report_is_silent_without_built_tables(tmp_path, capsys):
    """`check` must not crash before the first transform."""
    check_unlogged_items(tmp_path)
    assert "run `dota-harvest transform`" in capsys.readouterr().out
