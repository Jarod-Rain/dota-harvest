"""Client-side sampling-frame filters.

These decide what enters the dataset, so a silent change here biases every
downstream model rather than raising anything.
"""

from __future__ import annotations

import time

from dota_harvest.pipeline import discover
from dota_harvest.pipeline.discover import (
    DEFAULT_ID_RATE,
    RANKED_LOBBY_TYPE,
    TOO_NEW,
    _drop_reason,
    _id_rate,
    _is_sampled,
    _newest_start,
    _should_keep,
)

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


def _criteria(**overrides):
    criteria = {
        "source": "public",
        "min_rank": 75,
        "sample": 1.0,
        "ranked_only": True,
        "age_cutoff": NOW - 48 * 3600,
        "until_ts": None,
    }
    criteria.update(overrides)
    return criteria


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


def test_drop_reason_is_none_when_kept():
    assert _drop_reason(_row(), **_criteria()) is None


def test_reports_too_new_for_unindexed_matches():
    """The reason that made the original walk keep 0 with no explanation."""
    assert _drop_reason(_row(start_time=int(NOW)), **_criteria()) == TOO_NEW


def test_reports_the_until_floor():
    criteria = _criteria(until_ts=int(NOW - 5 * 86_400))
    assert _drop_reason(_row(), **criteria) == "below --until"


def test_reports_rank_and_lobby_separately():
    assert _drop_reason(_row(avg_rank_tier=10), **_criteria(min_rank=75)) == "below --min-rank"
    assert _drop_reason(_row(lobby_type=0), **_criteria()) == "not ranked"


def test_age_is_checked_before_every_other_reason():
    """A too-new match must not be reported as, say, unranked instead."""
    row = _row(start_time=int(NOW), lobby_type=0, avg_rank_tier=1)
    assert _drop_reason(row, **_criteria(min_rank=80)) == TOO_NEW


def test_id_rate_measures_ids_per_second():
    rows = [
        {"match_id": 1_000_000, "start_time": 1_000},
        {"match_id": 1_003_000, "start_time": 1_100},
    ]
    assert _id_rate(rows) == 30.0


def test_id_rate_falls_back_when_page_spans_no_time():
    """Every row sharing one timestamp would otherwise divide by zero."""
    rows = [{"match_id": 1, "start_time": 500}, {"match_id": 2, "start_time": 500}]
    assert _id_rate(rows) == DEFAULT_ID_RATE


def test_id_rate_falls_back_on_a_single_row():
    assert _id_rate([{"match_id": 1, "start_time": 500}]) == DEFAULT_ID_RATE


def test_id_rate_ignores_rows_without_a_timestamp():
    rows = [
        {"match_id": 1_000_000, "start_time": 1_000},
        {"match_id": 9_999_999},
        {"match_id": 1_003_000, "start_time": 1_100},
    ]
    assert _id_rate(rows) == 30.0


def test_newest_start_picks_the_maximum():
    assert _newest_start([{"start_time": 5}, {"start_time": 9}, {"start_time": 7}]) == 9


def test_newest_start_is_none_without_timestamps():
    assert _newest_start([{"match_id": 1}]) is None


def _page(start_id, start_ts, count=100, id_step=300, ts_step=10):
    """A synthetic descending page, mimicking OpenDota's newest-first order."""
    return [
        {"match_id": start_id - i * id_step, "start_time": start_ts - i * ts_step}
        for i in range(count)
    ]


def test_seek_returns_none_when_newest_is_already_collectable(monkeypatch):
    """Nothing to skip; the caller should just start from the newest page."""
    monkeypatch.setattr(discover, "discover_page", lambda *a, **k: _page(9_000_000_000, 1_000_000))
    entry, probes = discover.seek_to_age_cutoff("public", 11, 2_000_000)
    assert entry is None
    assert probes == 1


def test_seek_returns_none_on_an_empty_archive(monkeypatch):
    monkeypatch.setattr(discover, "discover_page", lambda *a, **k: [])
    assert discover.seek_to_age_cutoff("public", 11, 1_000)[0] is None


def _sparse_archive(true_rate, newest_id, newest_ts, page_rate):
    """An archive whose pages understate the true id rate, as the live API does.

    A rank-filtered page is a sparse sample of the id space: consecutive rows
    skip many ids, so the rate measured *within* a page is lower than the rate
    at which ids actually climb *across* the archive. ``page_rate`` controls
    that understatement.
    """

    def page(source, less_than, min_rank):
        anchor = newest_id if less_than is None else less_than
        anchor_ts = newest_ts - (newest_id - anchor) / true_rate
        # ids/second within the page is page_rate, below the archive's true_rate
        return [
            {
                "match_id": int(anchor - i * page_rate * 10),
                "start_time": int(anchor_ts - i * 10),
            }
            for i in range(100)
        ]

    return page


def test_seek_converges_when_pages_understate_the_id_rate(monkeypatch):
    """The regression this fix exists for.

    Live, a page reported ~14 ids/s while the archive actually advanced ~23
    ids/s. Seeding from the page rate and never correcting makes every step
    undershoot: the seek closes the gap geometrically and runs out of probes
    while still hours short of the window, so the walk keeps 0 forever.
    """
    true_rate, page_rate = 25.0, 10.0
    newest_id, newest_ts = 9_000_000_000, 2_000_000
    cutoff = newest_ts - 48 * 3600

    monkeypatch.setattr(
        discover,
        "discover_page",
        _sparse_archive(true_rate, newest_id, newest_ts, page_rate),
    )
    entry, probes = discover.seek_to_age_cutoff("public", 11, cutoff)

    assert entry is not None, "seek failed to reach the collectable window"
    assert probes <= discover.SEEK_MAX_PROBES + 1
    landed = newest_ts - (newest_id - entry) / true_rate
    assert 0 <= cutoff - landed <= discover.SEEK_TOLERANCE_HOURS * 3600


def test_seek_never_exceeds_its_probe_budget(monkeypatch):
    """A pathological archive must not spend unbounded API calls."""
    calls = []

    def stubborn(source, less_than, min_rank):
        calls.append(less_than)
        # Always reports the same time, so the seek can never converge.
        return _page(9_000_000_000 if less_than is None else less_than, 2_000_000)

    monkeypatch.setattr(discover, "discover_page", stubborn)
    entry, probes = discover.seek_to_age_cutoff("public", 11, 2_000_000 - 48 * 3600)
    assert entry is None
    assert len(calls) <= discover.SEEK_MAX_PROBES + 1
    assert probes <= discover.SEEK_MAX_PROBES + 1
