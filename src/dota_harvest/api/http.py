"""HTTP transport with retry, backoff, and rate-limit handling.

Vendor-neutral: both API clients share this module, and it knows nothing about
GraphQL, REST, or Dota. Its job is to turn a request into a response, sleeping
through the rate limits that can be waited out and failing fast on the ones that
cannot.
"""

from __future__ import annotations

import email.utils
import random
import sys
import time
from typing import Any, Final
from urllib.parse import urlsplit

import requests

#: Never sleep longer than this on a server's advice. Blind trust in
#: ``Retry-After`` is how an unattended job sleeps until morning.
MAX_HONOURED_WAIT: Final[float] = 300.0

#: Ceiling for computed backoff, used only when the server declines to advise.
#: Per-minute windows are common, so a ladder topping out below 60s can never
#: clear one.
FALLBACK_CAP: Final[float] = 75.0

#: Default attempts before giving up on a single request.
DEFAULT_MAX_TRIES: Final[int] = 8

#: Rate-limit windows short enough that sleeping one off is worthwhile, longest
#: first so the reported wait matches the window that actually has to drain.
WAITABLE_WINDOWS: Final[tuple[str, ...]] = ("hour", "minute", "second")

#: How long each waitable window takes to refill, for the message on giving up.
WINDOW_SECONDS: Final[dict[str, int]] = {"second": 1, "minute": 60, "hour": 3600}

#: Seconds before a single request is abandoned.
REQUEST_TIMEOUT: Final[float] = 60.0

#: Rate-limit windows too long to wait out inside a retry loop.
UNWAITABLE_WINDOWS: Final[tuple[str, ...]] = ("day", "month")

#: Last quota counters seen, keyed by host and then by window ("day", "minute").
#:
#: Populated from ``X-Rate-Limit-Remaining-*`` headers on every reply, not just
#: 429s, so callers can stop before hitting the wall instead of after.
#:
#: Keyed by host because STRATZ and OpenDota both send these headers and have
#: unrelated budgets. A single flat dict let whichever API replied last define
#: "remaining", so a run touching both would test one vendor's reserve against
#: the other's counters -- and the daily-quota guard would fire, or fail to, on
#: the wrong number.
QUOTA: dict[str, dict[str, int]] = {}


class QuotaExhaustedError(RuntimeError):
    """Raised when a rate-limit window too long to wait out is exhausted.

    Distinct from an ordinary 429 because the correct response is different: a
    per-minute limit is worth sleeping through, a daily one is not. Retrying
    into a daily window just burns the retry budget and reports a confusing
    failure hours later, so this propagates to the caller, which checkpoints and
    exits cleanly.
    """


class RateLimitedError(RuntimeError):
    """Raised when a short rate-limit window outlasts the whole retry ladder.

    The retry loop used to exhaust its attempts on a sustained per-minute block
    and raise a bare :class:`RuntimeError`, which no caller could tell apart
    from a genuine bug. That crashed ``--continuous`` runs whose entire purpose
    was to wait such things out. Callers catch this to pause and resume.

    Attributes:
        window: The rate-limit window that was empty, e.g. ``"minute"``.
        retry_after: Seconds until that window is expected to refill.
    """

    def __init__(self, message: str, *, window: str, retry_after: float) -> None:
        """Record which window blocked the request and for how long.

        Args:
            message: Human-readable description of the failure.
            window: The exhausted rate-limit window, e.g. ``"minute"``.
            retry_after: Seconds before that window is expected to refill.
        """
        super().__init__(message)
        self.window = window
        self.retry_after = retry_after


def host_of(url: str) -> str:
    """Extract the network location used to key :data:`QUOTA`.

    Args:
        url: Absolute request URL.

    Returns:
        The host (with port, if present).
    """
    return urlsplit(url).netloc


def quota_for(url_or_host: str) -> dict[str, int]:
    """Look up the counters last seen from one API.

    Args:
        url_or_host: A full URL or a bare host name.

    Returns:
        Remaining-request counters by window, or an empty mapping if that host
        has not replied yet. Callers must treat an absent counter as unknown
        rather than as zero.
    """
    host = host_of(url_or_host) if "//" in url_or_host else url_or_host
    return QUOTA.get(host, {})


def remaining_from_headers(resp: requests.Response) -> dict[str, int]:
    """Read any ``X-Rate-Limit-Remaining-*`` counters the server volunteered.

    Args:
        resp: Any response, successful or not.

    Returns:
        Remaining requests keyed by window name, e.g. ``{"minute": 55}``.
        Unparseable values are skipped rather than raising.

    Note:
        OpenDota sends ``Remaining-Minute`` and ``Remaining-Day``. The daily
        counter is undocumented -- their published limits are 50k/month and
        60/minute -- and it goes negative once exceeded.
    """
    counters: dict[str, int] = {}
    for name, value in resp.headers.items():
        lowered = name.lower()
        if lowered.startswith("x-rate-limit-remaining-"):
            try:
                counters[lowered.rsplit("-", 1)[-1]] = int(value)
            except ValueError:
                continue
    return counters


def retry_after_seconds(resp: requests.Response) -> float | None:
    """Parse the ``Retry-After`` header into a delay in seconds.

    Args:
        resp: Response carrying the header, typically a 429.

    Returns:
        The advised delay, clamped at zero, or ``None`` when the header is
        absent or unparseable.

    Note:
        RFC 9110 permits either delay-seconds or an HTTP-date. The date form is
        the one that bites: ``float()`` raises ``ValueError`` on it, which would
        escape the 429 handler entirely and surface as a crash rather than a
        pause.
    """
    raw = resp.headers.get("Retry-After")
    if not raw:
        return None

    try:
        return max(float(raw.strip()), 0.0)
    except ValueError:
        pass

    try:
        when = email.utils.parsedate_to_datetime(raw)
    except (TypeError, ValueError):
        return None
    if when is None:
        return None
    return max(when.timestamp() - time.time(), 0.0)


def _rate_limit_headers(resp: requests.Response) -> str:
    """Render every rate-limit header on a response, for diagnosis.

    Args:
        resp: Response to inspect.

    Returns:
        Headers as ``name=value`` pairs sorted by name, or an empty string if
        the server sent none.
    """
    seen = {
        name: value
        for name, value in resp.headers.items()
        if "ratelimit" in name.lower().replace("-", "") or name.lower() == "retry-after"
    }
    return "  ".join(f"{name}={value}" for name, value in sorted(seen.items()))


def _backoff_delay(attempt: int, cap: float = FALLBACK_CAP) -> float:
    """Compute an exponential backoff delay with jitter.

    Args:
        attempt: Zero-based attempt number.
        cap: Upper bound before jitter is added.

    Returns:
        Seconds to sleep. The jitter keeps concurrent workers from retrying in
        lockstep.
    """
    return min(2.0**attempt, cap) + random.random()


def _raise_if_quota_exhausted(remaining: dict[str, int]) -> None:
    """Fail fast when a long rate-limit window has been used up.

    Args:
        remaining: Counters last seen from the host being called.

    Raises:
        QuotaExhaustedError: If any window in :data:`UNWAITABLE_WINDOWS` has no
            requests left. A window measured in days or months cannot be waited
            out inside a request retry loop, so the caller must checkpoint and
            resume later instead.
    """
    for window in UNWAITABLE_WINDOWS:
        if remaining.get(window, 1) <= 0:
            raise QuotaExhaustedError(
                f"{window}ly API quota exhausted (remaining={remaining[window]}). "
                f"Other counters: {remaining}."
            )


def _handle_rate_limited(
    resp: requests.Response,
    attempt: int,
    reported: bool,
) -> tuple[float, bool]:
    """Decide how long to sleep after a 429.

    Args:
        resp: The rate-limited response.
        attempt: Zero-based attempt number, used for the fallback ladder.
        reported: Whether this call has already logged the server's headers.

    Returns:
        A ``(delay, reported)`` pair. ``reported`` becomes ``True`` after the
        headers are logged once, so a long run does not spam -- but they are
        logged often enough to learn what the API actually advertises.
    """
    if not reported:
        headers = _rate_limit_headers(resp)
        print(f"  429 headers: {headers or '(none sent)'}", file=sys.stderr)
        reported = True

    advised = retry_after_seconds(resp)
    if advised is None:
        delay = _backoff_delay(attempt)
        source = "backoff"
    else:
        delay = min(advised, MAX_HONOURED_WAIT)
        source = (
            f"Retry-After capped from {advised:.0f}s"
            if advised > MAX_HONOURED_WAIT
            else "Retry-After"
        )

    print(f"  429 rate limited; sleeping {delay:.1f}s ({source})", file=sys.stderr)
    return delay, reported


def _spent_short_window(
    remaining: dict[str, int],
    resp: requests.Response,
) -> tuple[str, float]:
    """Identify which waitable window is blocking, and how long it needs.

    Args:
        remaining: Counters last seen from the host being called.
        resp: The final rate-limited response, consulted for ``Retry-After``.

    Returns:
        A ``(window, seconds)`` pair. The server's own advice wins when it sent
        any; otherwise the wait is the window's full refill period, since a
        counter sitting at or below zero says nothing about when it last reset.

    Note:
        Falls back to ``"minute"`` when no counter is exhausted. A 429 that
        survives the whole ladder is a real block whatever the headers claim,
        and pausing a minute beats crashing on an unexplained one.
    """
    for window in WAITABLE_WINDOWS:
        if remaining.get(window, 1) <= 0:
            advised = retry_after_seconds(resp)
            return window, advised if advised is not None else float(WINDOW_SECONDS[window])

    advised = retry_after_seconds(resp)
    return "minute", advised if advised is not None else float(WINDOW_SECONDS["minute"])


def request_with_retry(
    method: str,
    url: str,
    *,
    max_tries: int = DEFAULT_MAX_TRIES,
    **kwargs: Any,
) -> requests.Response:
    """Issue an HTTP request, retrying transient failures with backoff.

    Retries network errors, 429s, and 5xx responses; returns anything else to
    the caller, including 4xx, which retrying cannot fix. Quota counters are
    recorded from every response, so callers can stop before hitting a wall
    rather than after.

    Args:
        method: HTTP verb.
        url: Absolute request URL.
        max_tries: Attempts before giving up.
        **kwargs: Passed through to ``requests.request``.

    Returns:
        The first response that is neither rate-limited nor a server error.

    Raises:
        QuotaExhaustedError: A rate-limit window too long to wait out is spent.
        RateLimitedError: A short window outlasted the whole retry ladder.
        requests.RequestException: The final attempt failed at the network level.
        RuntimeError: Every attempt was consumed by retryable server errors.
    """
    reported = False
    last_response: requests.Response | None = None

    for attempt in range(max_tries):
        try:
            resp = requests.request(method, url, timeout=REQUEST_TIMEOUT, **kwargs)
        except requests.RequestException as exc:
            if attempt == max_tries - 1:
                raise
            delay = _backoff_delay(attempt, cap=float("inf"))
            print(f"  network error ({exc}); retry in {delay:.1f}s", file=sys.stderr)
            time.sleep(delay)
            continue

        host = host_of(url)
        if counters := remaining_from_headers(resp):
            QUOTA.setdefault(host, {}).update(counters)

        last_response = resp
        if resp.status_code == 429:
            _raise_if_quota_exhausted(QUOTA.get(host, {}))
            delay, reported = _handle_rate_limited(resp, attempt, reported)
            if attempt == max_tries - 1:
                # Sleeping here would only delay the raise below by a full
                # ladder step; the loop has no attempt left to spend on it.
                break
            time.sleep(delay)
            continue

        if resp.status_code >= 500:
            delay = _backoff_delay(attempt, cap=float("inf"))
            print(f"  {resp.status_code}; retry in {delay:.1f}s", file=sys.stderr)
            if attempt == max_tries - 1:
                break
            time.sleep(delay)
            continue

        return resp

    if last_response is not None and last_response.status_code == 429:
        window, wait = _spent_short_window(QUOTA.get(host_of(url), {}), last_response)
        raise RateLimitedError(
            f"{window}ly rate limit still blocked after {max_tries} tries: {method} {url}",
            window=window,
            retry_after=wait,
        )
    raise RuntimeError(f"gave up after {max_tries} tries: {method} {url}")
