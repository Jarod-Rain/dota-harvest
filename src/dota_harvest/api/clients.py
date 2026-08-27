"""Vendor clients for STRATZ (GraphQL) and OpenDota (REST).

Both sit on :mod:`dota_harvest.api.http`, which owns retries and rate limiting.
What differs between them is protocol and shape: STRATZ answers one batched
GraphQL query per request and reports per-field errors alongside partial data,
while OpenDota answers plain REST endpoints one at a time.
"""

from __future__ import annotations

import os
import sys
import time
from typing import Any, Final

from dota_harvest.api.http import request_with_retry
from dota_harvest.core.config import (
    OPENDOTA_SLEEP,
    OPENDOTA_URL,
    STRATZ_UA,
    STRATZ_URL,
)
from dota_harvest.core.types import FieldTree, JSONMapping

#: Truncate GraphQL error messages to keep the manifest readable.
MAX_ERROR_CHARS: Final[int] = 300

# --- STRATZ --------------------------------------------------------------

#: The per-match selection set, shared by the batch query and the field check.
#:
#: This is the single source of truth for what gets downloaded. The transform's
#: pinned schema is diffed against it by ``dota-harvest check``; see
#: :func:`selected_fields`.
MATCH_FIELDS: Final[str] = """
    id
    didRadiantWin
    durationSeconds
    startDateTime
    endDateTime
    gameVersionId
    lobbyType
    gameMode
    rank
    parsedDateTime
    leagueId
    seriesId
    radiantTeamId
    direTeamId
    firstBloodTime
    towerStatusRadiant
    towerStatusDire
    players {
      steamAccountId
      heroId
      isRadiant
      isVictory
      lane
      role
      position
      networth
      level
      kills
      deaths
      assists
      numLastHits
      numDenies
      goldPerMinute
      experiencePerMinute
      heroDamage
      towerDamage
      heroHealing
      imp
      award
      item0Id
      item1Id
      item2Id
      item3Id
      item4Id
      item5Id
      backpack0Id
      backpack1Id
      backpack2Id
      neutral0Id
      stats {
        itemPurchases {
          itemId
          time
        }
        # Fantasy scoring categories. STRATZ field names here are the least
        # certain part of this query -- run `dota-harvest check` after any
        # edit; it reports unknown fields by name.
        campStack
        runes { rune time }
        wardDestruction { time isWard }
        killEvents { time }
      }
    }
"""


def selected_fields(selection: str = MATCH_FIELDS) -> FieldTree:
    """Parse a GraphQL selection set into a nested field tree.

    Args:
        selection: GraphQL selection-set text. Defaults to :data:`MATCH_FIELDS`.

    Returns:
        Field names nested by depth, with leaves mapping to an empty dict, e.g.
        ``{"players": {"stats": {"campStack": {}}}}``. Comments and blank lines
        are ignored, and a field list closed on the same line
        (``runes { rune time }``) parses the same as a multi-line block.

    Note:
        Derived from the query text rather than maintained as a second list, so
        it cannot drift from what is actually sent. ``dota-harvest check`` diffs
        this against the pinned transform schema and against the stored raw
        data; both comparisons are only meaningful if this side is derived, not
        restated.
    """
    root: FieldTree = {}
    stack = [root]
    for raw_line in selection.splitlines():
        line = raw_line.split("#", 1)[0].strip()
        if not line:
            continue
        # A closing brace can trail a field list, e.g. "runes { rune time }".
        while line:
            if line.startswith("}"):
                if len(stack) > 1:
                    stack.pop()
                line = line[1:].strip()
                continue
            token, _, rest = line.partition(" ")
            rest = rest.strip()
            if rest.startswith("{"):
                child: FieldTree = {}
                stack[-1][token] = child
                stack.append(child)
                line = rest[1:].strip()
            else:
                stack[-1].setdefault(token, {})
                line = rest
    return root


def stratz_headers() -> dict[str, str]:
    """Build the authorisation headers for a STRATZ request.

    Returns:
        Bearer token, the required User-Agent, and a JSON content type.

    Raises:
        SystemExit: If ``STRATZ_TOKEN`` is unset. Exiting here rather than
            raising keeps the message actionable: every caller is a CLI command,
            and a traceback would bury the one thing the user needs to do.
    """
    token = os.environ.get("STRATZ_TOKEN")
    if not token:
        sys.exit("STRATZ_TOKEN is not set. Get one at https://stratz.com/api")
    return {
        "Authorization": f"Bearer {token}",
        "User-Agent": STRATZ_UA,
        "Content-Type": "application/json",
    }


def stratz_query(query: str, variables: JSONMapping | None = None) -> JSONMapping:
    """Execute a GraphQL query against STRATZ.

    Args:
        query: GraphQL document to execute.
        variables: Optional variable bindings.

    Returns:
        The decoded response body, which may carry ``data``, ``errors``, or
        both. Partial success is normal for batch queries, so this deliberately
        does not treat ``errors`` as fatal -- see :func:`parse_batch_response`.

    Raises:
        SystemExit: On 403, which is nearly always a fixable local problem.
        requests.HTTPError: For any other unsuccessful status.
    """
    resp = request_with_retry(
        "POST",
        STRATZ_URL,
        headers=stratz_headers(),
        json={"query": query, "variables": variables or {}},
    )
    if resp.status_code == 403:
        sys.exit(
            "403 from STRATZ. Usual causes: expired token, or a missing "
            f"User-Agent header (must be exactly {STRATZ_UA!r})."
        )
    resp.raise_for_status()
    return resp.json()


def build_batch_query(ids: list[int]) -> str:
    """Build one GraphQL document with N aliased single-match lookups.

    Args:
        ids: Match ids to request together.

    Returns:
        A query aliasing each id as ``m0``, ``m1``, ... so one HTTP request
        returns many matches. :func:`parse_batch_response` maps the aliases back.

    Note:
        The bulk ``matches(ids: [...])`` field is admin-gated ("User is not an
        admin"), so this aliases the unprivileged singular ``match(id:)``. The
        selection set repeats per alias rather than sharing a fragment -- a
        fragment needs the concrete type name, one more thing that can drift,
        for no gain.
    """
    parts = [f"  m{index}: match(id: {mid}) {{{MATCH_FIELDS}  }}" for index, mid in enumerate(ids)]
    return "query BatchMatches {\n" + "\n".join(parts) + "\n}"


def parse_batch_response(
    payload: JSONMapping,
    ids: list[int],
) -> tuple[dict[int, JSONMapping], dict[int, str]]:
    """Split an aliased batch response into successes and per-id errors.

    Args:
        payload: Decoded response from :func:`stratz_query`.
        ids: The ids passed to :func:`build_batch_query`, in the same order --
            alias ``mN`` corresponds to ``ids[N]``.

    Returns:
        A ``(found, errors)`` pair mapping match id to its data and to its error
        message respectively. An id in neither is absent from STRATZ's index.

    Note:
        Aliased queries fail per alias: a bad id yields null there plus an entry
        in ``errors``, while siblings still return data. Treating the presence of
        ``errors`` as a whole-batch failure would discard good rows. An error
        without a path means the whole query was rejected, so it applies to every
        id.
    """
    data = payload.get("data") or {}
    errors: dict[int, str] = {}

    for error in payload.get("errors") or []:
        message = str(error.get("message"))[:MAX_ERROR_CHARS]
        path = error.get("path") or []
        if path and isinstance(path[0], str) and path[0].startswith("m"):
            try:
                errors[ids[int(path[0][1:])]] = message
            except (ValueError, IndexError):
                # An alias we cannot map back is not worth failing the batch for.
                continue
        else:
            for mid in ids:
                errors.setdefault(mid, message)

    found = {mid: data[f"m{index}"] for index, mid in enumerate(ids) if data.get(f"m{index}")}
    return found, errors


# --- OpenDota ------------------------------------------------------------


def opendota_params(extra: JSONMapping | None = None) -> JSONMapping:
    """Build query parameters for an OpenDota request.

    Args:
        extra: Endpoint-specific parameters.

    Returns:
        A copy of ``extra`` with ``api_key`` added when ``OPENDOTA_KEY`` is set.
        The key is optional: the free tier works without one, just more slowly.
    """
    params = dict(extra or {})
    key = os.environ.get("OPENDOTA_KEY")
    if key:
        params["api_key"] = key
    return params


def opendota_get(path: str, params: JSONMapping | None = None) -> Any:
    """Fetch and decode one OpenDota REST endpoint.

    Args:
        path: Endpoint path relative to the API root, e.g. ``"publicMatches"``.
        params: Optional query parameters; the API key is added automatically.

    Returns:
        The decoded JSON body. Most endpoints return a list of records.

    Raises:
        requests.HTTPError: If the response status is unsuccessful.

    Note:
        Sleeps :data:`~dota_harvest.core.config.OPENDOTA_SLEEP` after each call
        to stay inside the per-minute allowance. Pacing lives here so every
        caller inherits it rather than each remembering to throttle.
    """
    resp = request_with_retry("GET", f"{OPENDOTA_URL}/{path}", params=opendota_params(params))
    resp.raise_for_status()
    time.sleep(OPENDOTA_SLEEP)
    return resp.json()
