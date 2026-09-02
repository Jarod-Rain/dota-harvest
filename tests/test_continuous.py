"""Continuous collection, unlimited page budgets, and quota-reset sleeping.

A continuous run is unattended and long-lived, so the arithmetic deciding when
it wakes is the part worth pinning: an off-by-a-day sleep would stall a
multi-day backfill silently.
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime

import pytest

from dota_harvest.cli.main import (
    RESUMABLE_OVERRIDES,
    TUNING_FLAGS,
    UNLIMITED,
    _resolve_bound,
    _show_bound,
    date_floor,
    page_budget,
)
from dota_harvest.core.manifest import RANGE_PARAMS, parse_date
from dota_harvest.pipeline.discover import (
    MAX_QUOTA_SLEEP_S,
    QUOTA_RESET_MARGIN_S,
    UNLIMITED_PAGES,
    seconds_until_quota_reset,
)


def _at(iso: str) -> float:
    return datetime.fromisoformat(iso).replace(tzinfo=UTC).timestamp()


def _wake(iso: str) -> datetime:
    moment = _at(iso)
    return datetime.fromtimestamp(moment + seconds_until_quota_reset(moment), UTC)


# --- quota reset arithmetic ---------------------------------------------


@pytest.mark.parametrize(
    "when",
    ["2026-08-30T00:01:00", "2026-08-30T12:00:00", "2026-08-30T23:59:00"],
)
def test_always_wakes_just_after_the_next_utc_midnight(when):
    """Whenever the budget runs out, the wake time is the same boundary."""
    wake = _wake(when)
    assert wake.date() == datetime.fromisoformat("2026-08-31").date()
    assert wake.hour == 0
    assert wake.minute == QUOTA_RESET_MARGIN_S // 60


def test_sleep_includes_the_reset_margin():
    """A wake exactly at midnight could race a reset applied slightly late."""
    assert seconds_until_quota_reset(_at("2026-08-30T23:59:59")) >= QUOTA_RESET_MARGIN_S


def test_a_full_day_is_never_exceeded():
    """The daily window is 24h; a longer wait would mean a broken clock."""
    for when in ["2026-08-30T00:00:00", "2026-08-30T00:00:01", "2026-01-01T00:00:00"]:
        assert seconds_until_quota_reset(_at(when)) <= MAX_QUOTA_SLEEP_S


def test_sleep_is_never_negative():
    assert seconds_until_quota_reset(_at("2026-08-30T23:59:59.999")) >= 0


def test_midday_sleeps_about_half_a_day():
    hours = seconds_until_quota_reset(_at("2026-08-30T12:00:00")) / 3600
    assert 12 <= hours <= 12.5


# --- unlimited pages -----------------------------------------------------


def test_unlimited_pages_is_none():
    """The loop tests `pages is UNLIMITED_PAGES`, so the sentinel must be None."""
    assert UNLIMITED_PAGES is None


def test_unlimited_budget_never_bounds_the_loop():
    """Mirrors the loop's own condition: no page count ends an unlimited run."""
    for page in (0, 1, 10_000):
        assert UNLIMITED_PAGES is None or page < UNLIMITED_PAGES


# --- which flags a resume accepts ---------------------------------------


def test_reserve_is_overridable_on_resume():
    """It paces the run without changing which matches qualify."""
    assert "--reserve" in RESUMABLE_OVERRIDES
    assert "--reserve" not in TUNING_FLAGS


def test_continuous_is_overridable_on_resume():
    assert "--continuous" in RESUMABLE_OVERRIDES


def test_reserve_is_a_range_param():
    """The manifest must let a resume write the new value back."""
    assert "reserve" in RANGE_PARAMS


def test_filters_remain_locked():
    """The flags that decide the population must stay refused on resume."""
    for flag in ("--min-rank", "--sample", "--all-lobbies", "--source", "--min-age-hours"):
        assert flag in TUNING_FLAGS
        assert flag not in RESUMABLE_OVERRIDES


def test_the_two_flag_sets_stay_disjoint():
    assert not set(TUNING_FLAGS) & set(RESUMABLE_OVERRIDES)


# --- clearing a stored bound --------------------------------------------


@pytest.mark.parametrize("word", ["unlimited", "none", "inf", "all", "UNLIMITED", " Inf "])
def test_unlimited_words_parse_to_the_sentinel(word):
    """Case and surrounding space must not matter to an operator typing it."""
    assert page_budget(word) is UNLIMITED
    assert date_floor(word) is UNLIMITED


def test_a_page_count_parses_normally():
    assert page_budget("25") == 25


def test_a_date_parses_normally():
    assert date_floor("2026-08-25") == parse_date("2026-08-25")


@pytest.mark.parametrize("bad", ["0", "-5", "abc", ""])
def test_page_budget_rejects_nonsense(bad):
    with pytest.raises(argparse.ArgumentTypeError):
        page_budget(bad)


def test_zero_pages_points_at_the_right_flag():
    """`--pages 0` reads as 'no limit' but would collect nothing."""
    with pytest.raises(argparse.ArgumentTypeError, match="unlimited"):
        page_budget("0")


@pytest.mark.parametrize("bad", ["not-a-date", "2026-13-45", ""])
def test_date_floor_rejects_nonsense(bad):
    with pytest.raises(argparse.ArgumentTypeError):
        date_floor(bad)


def test_omitting_a_flag_keeps_the_stored_bound():
    """None means 'not supplied', which must leave the walk's value alone."""
    assert _resolve_bound(20, None) == 20
    assert _resolve_bound(None, None) is None


def test_the_sentinel_clears_the_stored_bound():
    """The gap this closes: previously no value could express 'remove it'."""
    assert _resolve_bound(20, UNLIMITED) is None
    assert _resolve_bound(1_700_000_000, UNLIMITED) is None


def test_a_concrete_value_replaces_the_stored_bound():
    assert _resolve_bound(20, 50) == 50


def test_none_and_the_sentinel_resolve_oppositely():
    """They are both 'falsy-ish' but mean opposite things, so keep them apart."""
    assert _resolve_bound(20, None) != _resolve_bound(20, UNLIMITED)


def test_an_absent_bound_displays_as_unlimited():
    """`pages=None` in status would read as a bug rather than a setting."""
    assert _show_bound("pages", None) == "unlimited"
    assert _show_bound("until", None) == "unlimited"


def test_a_present_bound_displays_its_value():
    assert _show_bound("pages", 20) == "20"
    assert _show_bound("until", parse_date("2026-08-25")) == "2026-08-25"
