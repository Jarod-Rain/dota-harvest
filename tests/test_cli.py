"""Argument validation that protects the dataset from a plausible typo."""

from __future__ import annotations

import argparse

import pytest

from dota_harvest.cli.main import RESUMABLE_OVERRIDES, TUNING_FLAGS, UNLIMITED, fraction


@pytest.mark.parametrize("value", ["1", "0.5", "0.01", "1.0"])
def test_accepts_valid_fractions(value):
    assert 0 < fraction(value) <= 1


@pytest.mark.parametrize("value", ["50", "0", "-0.5", "1.5", "abc"])
def test_rejects_out_of_range_and_non_numeric(value):
    """`--sample 50` once read as 'keep everything' and silently produced wrong data."""
    with pytest.raises(argparse.ArgumentTypeError):
        fraction(value)


def test_percentage_gets_a_corrective_hint():
    with pytest.raises(argparse.ArgumentTypeError, match="did you mean 0.5"):
        fraction("50")


def _resume_conflicts(argv):
    """Which tuning flags main() would reject alongside --resume."""
    return sorted(
        flag
        for flag in TUNING_FLAGS
        if any(arg == flag or arg.startswith(f"{flag}=") for arg in argv)
    )


def test_resume_alone_has_no_conflicts():
    assert _resume_conflicts(["discover", "--resume", "demo"]) == []


@pytest.mark.parametrize(
    "flag", ["--min-rank", "--sample", "--label", "--source", "--min-age-hours"]
)
def test_resume_rejects_each_filter_flag(flag):
    """Filters are locked at creation, so supplying one here is a mistake."""
    assert _resume_conflicts(["discover", "--resume", "demo", flag, "9"]) == [flag]


@pytest.mark.parametrize("flag", RESUMABLE_OVERRIDES)
def test_resume_allows_range_overrides(flag):
    """--pages and --until bound how far a run goes, not which matches qualify."""
    assert _resume_conflicts(["discover", "--resume", "demo", flag, "9"]) == []


def test_resume_rejects_equals_form():
    """`--min-rank=80` must be caught as readily as `--min-rank 80`."""
    assert _resume_conflicts(["discover", "--resume", "demo", "--min-rank=80"]) == ["--min-rank"]


def test_resume_rejects_store_true_flags():
    assert _resume_conflicts(["discover", "--resume", "demo", "--no-seek"]) == ["--no-seek"]


def test_resume_reports_every_conflict_at_once():
    argv = ["discover", "--resume", "demo", "--sample", "0.5", "--min-rank", "80"]
    assert _resume_conflicts(argv) == ["--min-rank", "--sample"]


def test_range_overrides_are_not_tuning_flags():
    """The two sets must stay disjoint, or a flag would be both allowed and refused."""
    assert not set(RESUMABLE_OVERRIDES) & set(TUNING_FLAGS)


def test_a_walk_value_named_like_a_flag_is_not_a_conflict():
    """The selector itself must not be mistaken for a tuning flag."""
    assert _resume_conflicts(["discover", "--resume", "public:demo"]) == []


@pytest.mark.parametrize("command", ["status", "paths", "transform", "check", "remove"])
def test_the_continuous_bound_check_tolerates_other_subcommands(command):
    """Other subcommands must survive the --continuous bound check.

    It reads --until/--pages, which only `discover` defines, so reading them
    unguarded raised AttributeError on every other subcommand.
    """
    args = argparse.Namespace(cmd=command)  # no until/pages/resume/continuous
    unbounded = getattr(args, "until", None) in (None, UNLIMITED) and getattr(
        args, "pages", None
    ) in (None, UNLIMITED)
    assert unbounded is True
    assert getattr(args, "continuous", False) is False


def test_continuous_needs_a_bound_only_on_a_fresh_walk():
    """A resume inherits the stored --until, so it needs no bound up front."""
    resuming = argparse.Namespace(continuous=True, resume="demo", until=None, pages=None)
    fresh = argparse.Namespace(continuous=True, resume=None, until=None, pages=None)

    def needs_bound(a):
        unbounded = getattr(a, "until", None) in (None, UNLIMITED) and getattr(
            a, "pages", None
        ) in (None, UNLIMITED)
        return getattr(a, "continuous", False) and not getattr(a, "resume", None) and unbounded

    assert needs_bound(fresh) is True
    assert needs_bound(resuming) is False
