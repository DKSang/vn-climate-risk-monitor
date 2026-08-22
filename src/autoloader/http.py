"""Shared HTTP session policy for source collectors."""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from typing import Any

from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

RETRYABLE_STATUS_CODES = frozenset({408, 429, 500, 502, 503, 504})

_log = logging.getLogger(__name__)


class RetryCancelledError(RuntimeError):
    """Retry bị hủy giữa chừng vì cả lô đã dừng (ví dụ: hết hạn mức ngày)."""


_RETRY_ABORT = threading.Event()


def abort_all_retries() -> None:
    """Đánh thức mọi giấc ngủ retry đang chạy — gọi khi dừng cả lô."""
    _RETRY_ABORT.set()


class RetryWith429Cooldown(Retry):
    """Honor Retry-After and provide a source-safe fallback for bare 429s."""

    def __init__(
        self,
        *args: object,
        fallback_retry_after_429: float = 60.0,
        **kwargs: object,
    ) -> None:
        super().__init__(*args, **kwargs)
        if fallback_retry_after_429 < 0:
            raise ValueError("fallback_retry_after_429 must not be negative")
        self.fallback_retry_after_429 = fallback_retry_after_429

    def new(self, **kwargs: Any) -> RetryWith429Cooldown:
        retry = super().new(**kwargs)
        retry.fallback_retry_after_429 = self.fallback_retry_after_429
        return retry

    def get_retry_after(self, response: Any) -> float | None:
        retry_after = super().get_retry_after(response)
        if retry_after is None and response.status == 429:
            return self.fallback_retry_after_429
        return retry_after

    def sleep(self, response: Any = None) -> None:
        """Ngủ giữa các lần thử, nhưng dậy ngay nếu cả lô bị hủy."""
        if self.respect_retry_after_header and response is not None:
            slept = self.get_retry_after(response)
            if slept is not None:
                self._sleep_abortable(max(0.0, slept))
                return
        backoff = self.get_backoff_time()
        if backoff > 0:
            self._sleep_abortable(backoff)

    def _sleep_abortable(self, seconds: float) -> None:
        if seconds >= 5:
            _log.warning("Đợi %.0fs trước khi thử lại...", seconds)
        if _RETRY_ABORT.wait(seconds):
            raise RetryCancelledError("retry bị hủy vì cả lô đã dừng")


class EffectiveCallPacer:
    """Token-bucket pacing across both minute and hourly call budgets.

    Thread-safe: ``wait`` giữ lock trong lúc ngủ nên các luồng tải song song
    xếp hàng theo đúng ngân sách chung, không ai vượt hạn mức.
    """

    def __init__(
        self,
        *,
        calls_per_minute: int,
        calls_per_hour: int,
        monotonic: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        if calls_per_minute < 1 or calls_per_hour < 1:
            raise ValueError("Effective call pacing budgets must be positive")
        self._minute_capacity = float(calls_per_minute)
        self._hour_capacity = float(calls_per_hour)
        self._minute_rate = calls_per_minute / 60
        self._hour_rate = calls_per_hour / 3600
        self._monotonic = monotonic
        self._sleeper = sleeper
        self._minute_tokens = self._minute_capacity
        self._hour_tokens = self._hour_capacity
        self._updated_at = monotonic()
        self._lock = threading.Lock()

    def _refill(self, now: float) -> None:
        elapsed = max(0.0, now - self._updated_at)
        self._minute_tokens = min(
            self._minute_capacity,
            self._minute_tokens + elapsed * self._minute_rate,
        )
        self._hour_tokens = min(
            self._hour_capacity,
            self._hour_tokens + elapsed * self._hour_rate,
        )
        self._updated_at = now

    def wait(self, call_units: int) -> float:
        """Wait before one request and reserve time for its effective cost."""
        if call_units < 1:
            raise ValueError("call_units must be positive")
        if call_units > min(self._minute_capacity, self._hour_capacity):
            raise ValueError("One request exceeds an effective call pacing budget")
        with self._lock:
            now = self._monotonic()
            self._refill(now)
            wait_seconds = max(
                0.0,
                (call_units - self._minute_tokens) / self._minute_rate,
                (call_units - self._hour_tokens) / self._hour_rate,
            )
            if wait_seconds:
                self._sleeper(wait_seconds)
                now = self._monotonic()
                self._refill(now)
            self._minute_tokens -= call_units
            self._hour_tokens -= call_units
            return wait_seconds


def build_http_session(
    *,
    max_attempts: int = 5,
    fallback_retry_after_429: float = 60.0,
    pool_size: int = 10,
) -> Session:
    """Return one reusable session with bounded retries for idempotent GETs.

    ``max_attempts`` includes the initial request, while urllib3's ``total``
    value counts retries after that request. User-agent và backoff là hằng số:
    không caller nào cần đổi (kể cả test).
    """
    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")
    if pool_size < 1:
        raise ValueError("pool_size must be at least 1")

    retry = RetryWith429Cooldown(
        total=max_attempts - 1,
        connect=max_attempts - 1,
        read=max_attempts - 1,
        status=max_attempts - 1,
        other=0,
        allowed_methods=frozenset({"GET"}),
        status_forcelist=RETRYABLE_STATUS_CODES,
        backoff_factor=1.0,
        backoff_jitter=0.5,
        fallback_retry_after_429=fallback_retry_after_429,
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=pool_size,
        pool_maxsize=pool_size,
    )

    session = Session()
    session.headers.update(
        {
            "User-Agent": "vn-climate-risk-monitor",
            "Accept": "application/json",
            "Accept-Encoding": "identity",
        }
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session
