"""GET URL → PUT object. Không biết nguồn; planner gọi ``land(client, bucket, url, key)``.

Song song nằm ở :mod:`fetch.pool` — ``land`` vẫn là một thao tác đơn.
"""

from __future__ import annotations

import json
import time
from io import BytesIO
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from minio import Minio

from fetch.pool import FetchTask, PoolResult, run_fetch_pool

__all__ = [
    "FetchTask",
    "PoolResult",
    "bronze_bytes",
    "get_body",
    "land",
    "run_fetch_pool",
]

RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})
RETRIES = 5
RETRY_PAUSE_S = 60
HTTP_TIMEOUT_S = 60
USER_AGENT = "vn-climate-risk-monitor"


def get_body(url: str) -> bytes:
    request = Request(
        url,
        headers={
            "Accept": "application/json",
            "Accept-Encoding": "identity",
            "User-Agent": USER_AGENT,
        },
    )
    for attempt in range(RETRIES):
        try:
            with urlopen(request, timeout=HTTP_TIMEOUT_S) as response:
                return response.read()
        except HTTPError as error:
            if error.code not in RETRY_STATUSES or attempt == RETRIES - 1:
                raise
            time.sleep(RETRY_PAUSE_S)
    raise RuntimeError("unreachable")


def bronze_bytes(body: bytes) -> bytes:
    payload = json.loads(body)
    rows = payload if isinstance(payload, list) else [payload]
    for item in rows:
        if isinstance(item, dict) and item.get("error") is True:
            raise RuntimeError(str(item.get("reason") or "Open-Meteo error"))
    return json.dumps(rows).encode()


def land(client: Minio, bucket: str, url: str, key: str) -> None:
    body = bronze_bytes(get_body(url))
    client.put_object(
        bucket, key, BytesIO(body), len(body), content_type="application/json"
    )
