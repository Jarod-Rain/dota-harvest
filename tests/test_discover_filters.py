"""Client-side sampling-frame filters.

These decide what enters the dataset, so a silent change here biases every
downstream model rather than raising anything.
"""

from __future__ import annotations

import time

from dota_harvest.pipeline.discover import RANKED_LOBBY_TYPE, _is_sampled, _should_keep

NOW = time.time()
OLD_ENOUGH = NOW - 10 * 86_400


def _row(**overrides):
    row = {
        "match_id": 8_930_260_000,
        "start_time": int(OLD_ENOUGH),
        "avg_rank_tier": 80,
        "lobby_type": RANKED_LOBBY_TYPE,
    }
    row.update(overrides)
    return row


def _keep(row, **overrides):
    kwargs = {
        "source": "public",
        "min_rank": 75,
        "sample": 1.0,
        "ranked_only": True,
        "age_cutoff": NOW - 48 * 3600,
        "until_ts": None,
    }
    kwargs.update(overrides)
    return _should_keep(row, **kwargs)


def test_keeps_a_qualifying_public_match():
    assert _keep(_row())


def test_drops_matches_younger_than_the_age_cutoff():
    """STRATZ has not indexed these yet; they would land in 'missing'."""
    assert not _keep(_row(start_time=int(NOW)))


def test_drops_matches_below_the_rank_floor():
    assert not _keep(_row(avg_rank_tier=60))


def test_drops_matches_with_unknown_rank():
    """A null tier cannot be shown to clear the floor, so it does not."""
    assert not _keep(_row(avg_rank_tier=None))


def test_drops_unranked_lobbies_by_default():
    assert not _keep(_row(lobby_type=0))


def test_keeps_unranked_lobbies_when_asked():
    assert _keep(_row(lobby_type=0), ranked_only=False)


def test_ignores_rank_and_lobby_for_pro_matches():
    """Pro matches have no MMR bracket, so those filters must not apply."""
    assert _keep(_row(avg_rank_tier=None, lobby_type=None), source="pro")


def test_drops_matches_older_than_the_floor():
    assert not _keep(_row(), until_ts=int(NOW - 5 * 86_400))


def test_sampling_is_deterministic_for_a_given_id():
    """A re-run over the same range must keep the same matches."""
    assert _is_sampled(8_930_260_988, 0.1) == _is_sampled(8_930_260_988, 0.1)


def test_full_sample_keeps_everything():
    assert all(_is_sampled(base + n, 1.0) for base in (0, 8_930_260_000) for n in range(50))


def test_sampling_keeps_roughly_the_requested_fraction():
    kept = sum(_is_sampled(mid, 0.1) for mid in range(8_930_260_000, 8_930_270_000))
    assert 900 <= kept <= 1100
