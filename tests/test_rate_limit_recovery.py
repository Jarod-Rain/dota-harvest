"""Recovery from a short rate-limit window that outlasts the retry ladder.

A 254-page walk was killed by a sustained per-minute block: the ladder ran out,
`request_with_retry` raised a bare RuntimeError, and it flew past the handler in
`_walk_pages` that exists precisely to keep `--continuous` runs alive. These
tests pin the three defects that combined to produce that crash.
"""

from __future__ import annotations

import pytest
import requests

from dota_harvest.api import http
from dota_harvest.api.http import (
    WAITABLE_WINDOWS,
    QuotaExhaustedError,
    RateLimitedError,
    _spent_short_window,
    request_with_retry,
)
from dota_harvest.core.manifest import STOP_RATE_LIMITED, TERMINAL_REASONS


class FakeResponse:
    """Minimal stand-in for the parts of a response the retry loop reads."""

    def __init__(self, status_code=429, headers=None):
        """Build a response with the given status and headers."""
        self.status_code = status_code
        self.headers = headers or {}


@pytest.fixture(autouse=True)
def _no_sleeping(monkeypatch):
    """Keep the ladder's arithmetic without paying its wall-clock cost."""
    slept: list[float] = []
    monkeypatch.setattr(http.time, "sleep", slept.append)
    # QUOTA is module-global and survives between tests, so a counter left by
    # one case would decide another's outcome.
    saved = {host: dict(counters) for host, counters in http.QUOTA.items()}
    http.QUOTA.clear()
    yield slept
    http.QUOTA.clear()
    http.QUOTA.update(saved)


def _always_429(headers):
    def _request(method, url, **kwargs):  # noqa: ARG001
        return FakeResponse(429, headers)

    return _request


# --- the crash -----------------------------------------------------------


def test_a_spent_minute_window_raises_the_typed_error(monkeypatch):
    """The bug: this raised bare RuntimeError, which no caller could handle."""
    headers = {"X-Rate-Limit-Remaining-Day": "2735", "X-Rate-Limit-Remaining-Minute": "-1"}
    monkeypatch.setattr(http.requests, "request", _always_429(headers))

    with pytest.raises(RateLimitedError) as caught:
        request_with_retry("GET", "https://api.opendota.com/api/publicMatches", max_tries=3)

    assert caught.value.window == "minute"
    assert caught.value.retry_after > 0


def test_the_typed_error_is_not_confused_with_a_daily_quota(monkeypatch):
    """A spent day must still raise the other error, which stops the walk."""
    headers = {"X-Rate-Limit-Remaining-Day": "0", "X-Rate-Limit-Remaining-Minute": "-1"}
    monkeypatch.setattr(http.requests, "request", _always_429(headers))

    with pytest.raises(QuotaExhaustedError):
        request_with_retry("GET", "https://api.opendota.com/api/publicMatches", max_tries=3)


def test_a_server_error_still_raises_plain_runtime_error(monkeypatch):
    """Only 429s become RateLimitedError; a 500 storm is a different problem."""
    monkeypatch.setattr(http.requests, "request", lambda *a, **k: FakeResponse(503))  # noqa: ARG005

    with pytest.raises(RuntimeError) as caught:
        request_with_retry("GET", "https://api.opendota.com/api/publicMatches", max_tries=3)
    assert not isinstance(caught.value, RateLimitedError)


# --- the wasted final sleep ---------------------------------------------


def test_the_last_attempt_does_not_sleep_before_giving_up(monkeypatch, _no_sleeping):
    """The ladder slept once more than it had attempts left to use it."""
    headers = {"X-Rate-Limit-Remaining-Minute": "-1"}
    monkeypatch.setattr(http.requests, "request", _always_429(headers))

    with pytest.raises(RateLimitedError):
        request_with_retry("GET", "https://api.opendota.com/api/publicMatches", max_tries=4)

    assert len(_no_sleeping) == 3, "4 attempts have only 3 gaps between them"


def test_a_successful_retry_still_sleeps_between_attempts(monkeypatch, _no_sleeping):
    """Trimming the final sleep must not remove the ones that do work."""
    replies = [FakeResponse(429, {}), FakeResponse(429, {}), FakeResponse(200, {})]
    monkeypatch.setattr(http.requests, "request", lambda *a, **k: replies.pop(0))  # noqa: ARG005

    resp = request_with_retry("GET", "https://api.opendota.com/api/publicMatches", max_tries=4)

    assert resp.status_code == 200
    assert len(_no_sleeping) == 2


# --- window identification ----------------------------------------------


def test_retry_after_wins_over_the_default_window_length():
    resp = FakeResponse(429, {"Retry-After": "12"})
    window, wait = _spent_short_window({"minute": -1}, resp)
    assert (window, wait) == ("minute", 12.0)


def test_an_absent_retry_after_falls_back_to_the_window_length():
    window, wait = _spent_short_window({"minute": 0}, FakeResponse(429, {}))
    assert (window, wait) == ("minute", 60.0)


def test_an_unexplained_block_still_pauses_a_minute():
    """A 429 with no exhausted counter is still real; crashing on it is worse."""
    window, wait = _spent_short_window({"day": 500}, FakeResponse(429, {}))
    assert (window, wait) == ("minute", 60.0)


def test_the_longest_blocking_window_is_reported_first():
    """An hourly block needs an hour; reporting the minute would retry too soon."""
    window, _ = _spent_short_window({"hour": 0, "minute": -1}, FakeResponse(429, {}))
    assert window == "hour"


def test_waitable_and_unwaitable_windows_stay_disjoint():
    """A window in both sets would both raise and be slept off."""
    assert not set(WAITABLE_WINDOWS) & set(http.UNWAITABLE_WINDOWS)


# --- the walk keeps its place -------------------------------------------


def test_rate_limited_leaves_the_walk_resumable():
    """Being blocked is a pause, so the walk must not be marked finished."""
    assert STOP_RATE_LIMITED not in TERMINAL_REASONS


def test_a_network_error_is_unchanged(monkeypatch):
    """RequestException must still surface as itself on the final attempt."""

    def _boom(*args, **kwargs):  # noqa: ARG001
        raise requests.ConnectionError("read timeout")

    monkeypatch.setattr(http.requests, "request", _boom)

    with pytest.raises(requests.ConnectionError):
        request_with_retry("GET", "https://api.opendota.com/api/publicMatches", max_tries=2)
