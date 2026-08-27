"""Argument validation that protects the dataset from a plausible typo."""

from __future__ import annotations

import argparse

import pytest

from dota_harvest.cli.main import fraction


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
