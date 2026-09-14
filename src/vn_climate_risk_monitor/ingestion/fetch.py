"""HTTP → MinIO primitives used by source-specific planners."""

from __future__ import annotations

import json
import threading
import time
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from io import BytesIO
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from minio import Minio

from vn_climate_risk_monitor.sources.open_meteo import FetchTask

RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})
RETRIES = 5
RETRY_PAUSE_S = 60
HTTP_TIMEOUT_S = 60
USER_AGENT = "vn-climate-risk-monitor"


@dataclass
class PoolResult:
    landed: int = 0
    failures: list[str] = field(default_factory=list)
    cancelled: int = 0

    @property
    def ok(self) -> bool:
        return not self.failures


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
    return body


def land(client: Minio, bucket: str, url: str, key: str) -> None:
    body = get_body(url)
    # Reject API error payloads before writing immutable Bronze bytes.
    bronze_bytes(body)
    client.put_object(
        bucket, key, BytesIO(body), len(body), content_type="application/json"
    )


def ensure_bucket(client: Minio, bucket: str) -> None:
    """Create the Bronze bucket before fetch workers start."""
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)


def run_fetch_pool(
    tasks: Sequence[FetchTask],
    *,
    land_one: Callable[[FetchTask], None],
    workers: int = 4,
    report: Callable[[str], None] = print,
) -> PoolResult:
    """Run independent fetches concurrently and stop scheduling useful work on error."""
    if workers < 1:
        raise ValueError("workers phải >= 1")

    result = PoolResult()
    stop = threading.Event()
    lock = threading.Lock()
    total = len(tasks)

    def work(task: FetchTask) -> None:
        if stop.is_set():
            with lock:
                result.cancelled += 1
            return
        try:
            land_one(task)
        except Exception as error:  # noqa: BLE001 - aggregate worker failures
            with lock:
                result.failures.append(f"{task.key}: {type(error).__name__}: {error}")
            stop.set()
            return
        with lock:
            result.landed += 1
            report(f"  [{result.landed}/{total}] {task.key}")

    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="fetch") as pool:
        list(pool.map(work, tasks))
    return result
