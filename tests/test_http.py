"""Rate-limit accounting and retry arithmetic.

Both are hard to observe in production -- a wrong quota reading looks like a
successful run that stopped early -- so they are pinned here instead.
"""

from __future__ import annotations

import email.utils
import time

import pytest
import requests

from dota_harvest.api import http
from dota_harvest.api.http import (
    MAX_HONOURED_WAIT,
    QuotaExhaustedError,
    _raise_if_quota_exhausted,
    host_of,
    quota_for,
    remaining_from_headers,
    retry_after_seconds,
)


def _response(**headers) -> requests.Response:
    resp = requests.Response()
    resp.headers.update(headers)
    return resp


@pytest.fixture(autouse=True)
def _clear_quota():
    http.QUOTA.clear()
    yield
    http.QUOTA.clear()


def test_reads_remaining_counters_by_window():
    resp = _response(**{"X-Rate-Limit-Remaining-Minute": "55", "X-Rate-Limit-Remaining-Day": "40"})
    assert remaining_from_headers(resp) == {"minute": 55, "day": 40}


def test_ignores_unparseable_counters():
    assert remaining_from_headers(_response(**{"X-Rate-Limit-Remaining-Day": "n/a"})) == {}


def test_quota_is_isolated_per_host():
    """STRATZ and OpenDota have unrelated budgets; one must not mask the other."""
    http.QUOTA["api.opendota.com"] = {"day": 40}
    http.QUOTA["api.stratz.com"] = {"day": 1999}
    assert quota_for("https://api.opendota.com/api")["day"] == 40
    assert quota_for("https://api.stratz.com/graphql")["day"] == 1999


def test_quota_accepts_a_bare_host():
    http.QUOTA["api.opendota.com"] = {"day": 7}
    assert quota_for("api.opendota.com")["day"] == 7


def test_quota_for_unknown_host_is_empty_not_zero():
    """An absent counter means unknown; treating it as 0 would stop a good run."""
    assert quota_for("https://example.com/x") == {}


def test_host_of_extracts_netloc():
    assert host_of("https://api.stratz.com/graphql") == "api.stratz.com"


@pytest.mark.parametrize("window", ["day", "month"])
def test_exhausted_long_window_raises(window):
    """A window measured in days cannot be waited out inside a retry loop."""
    with pytest.raises(QuotaExhaustedError):
        _raise_if_quota_exhausted({window: 0})


def test_negative_counter_also_raises():
    """OpenDota's daily counter goes negative once exceeded."""
    with pytest.raises(QuotaExhaustedError):
        _raise_if_quota_exhausted({"day": -5})


def test_short_windows_do_not_raise():
    """A per-minute limit is worth sleeping through, so it must not raise."""
    _raise_if_quota_exhausted({"minute": 0})


def test_unknown_counters_do_not_raise():
    _raise_if_quota_exhausted({})


def test_retry_after_parses_delay_seconds():
    assert retry_after_seconds(_response(**{"Retry-After": "30"})) == 30.0


def test_retry_after_parses_http_date():
    """The date form raises ValueError in float(), which once escaped the handler."""
    when = email.utils.formatdate(time.time() + 120, usegmt=True)
    parsed = retry_after_seconds(_response(**{"Retry-After": when}))
    assert parsed is not None
    assert 60 <= parsed <= 180


def test_retry_after_absent_is_none():
    assert retry_after_seconds(_response()) is None


def test_retry_after_garbage_is_none():
    assert retry_after_seconds(_response(**{"Retry-After": "soon"})) is None


def test_retry_after_never_negative():
    past = email.utils.formatdate(time.time() - 600, usegmt=True)
    assert retry_after_seconds(_response(**{"Retry-After": past})) == 0.0


def test_honoured_wait_is_capped():
    """Blind trust in Retry-After is how an unattended job sleeps until morning."""
    advised = retry_after_seconds(_response(**{"Retry-After": "99999"}))
    assert advised is not None
    assert min(advised, MAX_HONOURED_WAIT) == MAX_HONOURED_WAIT
