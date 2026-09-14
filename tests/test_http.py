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
    MAX_DAILY_WAIT,
    QuotaExhaustedError,
    _daily_reset_is_waitable,
    _handle_rate_limited,
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


def test_a_wait_beyond_the_ceiling_is_refused_not_capped():
    """Capping and retrying anyway sent guaranteed-429s at a 12-hour block.

    The old form clamped a 43,982s Retry-After to the ceiling and retried, so
    eight tries covered a fraction of the reset and every one was refused. A
    wait this long belongs to the caller, which checkpoints and resumes later.
    """
    resp = _response(**{"Retry-After": "99999"})
    with pytest.raises(QuotaExhaustedError, match="beyond the"):
        _handle_rate_limited(resp, 0, True)


def test_a_wait_inside_the_ceiling_is_honoured_verbatim():
    """No clamping below the ceiling: the server's own advice is the delay."""
    delay, _ = _handle_rate_limited(_response(**{"Retry-After": "30"}), 0, True)
    assert delay == 30.0


def test_the_refusal_names_the_counters_it_saw():
    """The message is the operator's only record of why the run stopped.

    A spent *day* is slept through instead, so the refusal path needs a long
    wait on some other window -- here an exhausted hour with the day healthy.
    """
    resp = _response(
        **{
            "Retry-After": "43982",
            "x-ratelimit-remaining-day": "1231",
            "x-ratelimit-remaining-hour": "0",
        }
    )
    with pytest.raises(QuotaExhaustedError, match="'hour': 0"):
        _handle_rate_limited(resp, 0, True)


# --- vendor header spellings ---------------------------------------------


def test_stratz_counters_are_read_without_the_inner_hyphen():
    """The root cause: STRATZ writes "ratelimit", OpenDota "rate-limit".

    Matching only the hyphenated form made every STRATZ counter invisible, so
    x-ratelimit-remaining-day=0 read as unknown and the daily guard, which
    treats an absent counter as "not exhausted", never fired.
    """
    resp = _response(
        **{
            "x-ratelimit-remaining-day": "0",
            "x-ratelimit-remaining-hour": "1231",
            "x-ratelimit-remaining-minute": "150",
            "x-ratelimit-remaining-second": "8",
        }
    )
    assert remaining_from_headers(resp) == {
        "day": 0,
        "hour": 1231,
        "minute": 150,
        "second": 8,
    }


def test_opendota_spelling_still_parses():
    """Widening the match must not drop the vendor that already worked."""
    resp = _response(**{"X-Rate-Limit-Remaining-Minute": "55", "X-Rate-Limit-Remaining-Day": "40"})
    assert remaining_from_headers(resp) == {"minute": 55, "day": 40}


def test_a_spent_stratz_day_now_reaches_the_daily_guard():
    """End to end: the headers from the failing run must stop the loop."""
    resp = _response(
        **{
            "retry-after": "43982",
            "x-ratelimit-remaining-day": "0",
            "x-ratelimit-remaining-minute": "150",
        }
    )
    with pytest.raises(QuotaExhaustedError, match="dayly"):
        _raise_if_quota_exhausted(remaining_from_headers(resp))


def test_limit_headers_are_not_mistaken_for_remaining():
    """x-ratelimit-limit-day sits one word away from the counter that matters."""
    resp = _response(**{"x-ratelimit-limit-day": "15000", "ratelimit-limit": "15000"})
    assert remaining_from_headers(resp) == {}


# --- sleeping through the daily reset ------------------------------------


def test_a_spent_day_with_advice_is_slept_through_uncapped():
    """A fetch must survive the daily reset, not stop at it.

    STRATZ sends the exact reset alongside a spent daily counter, so the wait
    is known rather than guessed. Clamping it to MAX_HONOURED_WAIT and retrying
    just sent guaranteed-429s into a window that could not have moved.
    """
    resp = _response(**{"Retry-After": "43982", "x-ratelimit-remaining-day": "0"})
    delay, _ = _handle_rate_limited(resp, 0, True)
    assert delay == 43982.0


def test_the_daily_sleep_is_bounded_against_nonsense_advice():
    """The wait comes from the server, so only a bogus value needs guarding."""
    resp = _response(**{"Retry-After": "999999", "x-ratelimit-remaining-day": "0"})
    delay, _ = _handle_rate_limited(resp, 0, True)
    assert delay == MAX_DAILY_WAIT


def test_a_spent_day_without_advice_is_not_waitable():
    """OpenDota sends counters but no Retry-After; there is nothing to wait on."""
    assert _daily_reset_is_waitable(_response(**{"X-Rate-Limit-Remaining-Day": "-5"})) is False


def test_a_healthy_day_is_never_waitable():
    resp = _response(**{"Retry-After": "43982", "x-ratelimit-remaining-day": "1231"})
    assert _daily_reset_is_waitable(resp) is False


def test_a_zero_retry_after_is_not_waitable():
    """Advice of 0s says nothing about when the day refills."""
    resp = _response(**{"Retry-After": "0", "x-ratelimit-remaining-day": "0"})
    assert _daily_reset_is_waitable(resp) is False


def test_opendota_spent_day_still_raises_through_the_transport(monkeypatch):
    """Discover's --continuous depends on this raise to sleep to UTC midnight."""
    resp = _response(**{"X-Rate-Limit-Remaining-Day": "-5", "X-Rate-Limit-Remaining-Minute": "-1"})
    resp.status_code = 429
    monkeypatch.setattr(http.requests, "request", lambda *a, **k: resp)
    monkeypatch.setattr(http.time, "sleep", lambda _: None)

    with pytest.raises(QuotaExhaustedError):
        http.request_with_retry("GET", "https://api.opendota.com/api/publicMatches")


def test_the_run_resumes_after_the_daily_sleep(monkeypatch):
    """The wake-up must spend a real attempt, not fall through the ladder."""
    blocked = _response(**{"Retry-After": "43982", "x-ratelimit-remaining-day": "0"})
    blocked.status_code = 429
    ok = _response(**{"x-ratelimit-remaining-day": "14999"})
    ok.status_code = 200

    replies = [blocked, ok]
    slept: list[float] = []
    monkeypatch.setattr(http.requests, "request", lambda *a, **k: replies.pop(0))
    monkeypatch.setattr(http.time, "sleep", slept.append)

    resp = http.request_with_retry("POST", "https://api.stratz.com/graphql")

    assert resp.status_code == 200
    assert slept == [43982.0]
