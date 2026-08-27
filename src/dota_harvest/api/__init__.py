"""Transport and API clients for STRATZ and OpenDota.

:mod:`~dota_harvest.api.http` owns everything vendor-neutral -- retries,
backoff, ``Retry-After`` handling, and per-host quota accounting.
:mod:`~dota_harvest.api.clients` layers the two vendor protocols on top: STRATZ
speaks GraphQL, OpenDota speaks REST.
"""

from dota_harvest.api.clients import (
    build_batch_query,
    opendota_get,
    parse_batch_response,
    selected_fields,
    stratz_query,
)
from dota_harvest.api.http import QuotaExhaustedError, quota_for, request_with_retry

__all__ = [
    "QuotaExhaustedError",
    "build_batch_query",
    "opendota_get",
    "parse_batch_response",
    "quota_for",
    "request_with_retry",
    "selected_fields",
    "stratz_query",
]
