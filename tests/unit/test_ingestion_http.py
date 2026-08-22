import threading
from types import SimpleNamespace

import pytest
from urllib3.util.retry import Retry

from autoloader.http import (
    _RETRY_ABORT,
    RETRYABLE_STATUS_CODES,
    EffectiveCallPacer,
    RetryCancelledError,
    RetryWith429Cooldown,
    abort_all_retries,
    build_http_session,
)


def test_http_session_has_bounded_get_retry_policy() -> None:
    session = build_http_session(max_attempts=5, pool_size=3)

    adapter = session.get_adapter("https://example.com")
    retry = adapter.max_retries

    assert isinstance(retry, Retry)
    assert isinstance(retry, RetryWith429Cooldown)
    assert retry.total == 4
    assert retry.allowed_methods == frozenset({"GET"})
    assert retry.status_forcelist == RETRYABLE_STATUS_CODES
    assert retry.respect_retry_after_header is True
    assert adapter._pool_connections == 3
    assert adapter._pool_maxsize == 3
    assert session.headers["User-Agent"] == "vn-climate-risk-monitor"
    assert session.headers["Accept-Encoding"] == "identity"


def test_http_retry_uses_one_minute_fallback_for_429_without_header() -> None:
    session = build_http_session(fallback_retry_after_429=60)
    retry = session.get_adapter("https://example.com").max_retries
    response = SimpleNamespace(status=429, headers={})

    assert retry.get_retry_after(response) == 60
    assert retry.new().fallback_retry_after_429 == 60


def test_http_retry_prefers_source_retry_after_header() -> None:
    session = build_http_session(fallback_retry_after_429=60)
    retry = session.get_adapter("https://example.com").max_retries
    response = SimpleNamespace(status=429, headers={"Retry-After": "17"})

    assert retry.get_retry_after(response) == 17


def test_effective_call_pacer_allows_burst_then_waits_for_minute_refill() -> None:
    current = [100.0]
    sleeps: list[float] = []

    def monotonic() -> float:
        return current[0]

    def sleep(seconds: float) -> None:
        sleeps.append(seconds)
        current[0] += seconds

    pacer = EffectiveCallPacer(
        calls_per_minute=500,
        calls_per_hour=4500,
        monotonic=monotonic,
        sleeper=sleep,
    )

    assert pacer.wait(500) == 0
    assert pacer.wait(25) == 3
    assert sleeps == [3]


def test_effective_call_pacer_enforces_hourly_bucket_too() -> None:
    current = [100.0]

    def monotonic() -> float:
        return current[0]

    def sleep(seconds: float) -> None:
        current[0] += seconds

    pacer = EffectiveCallPacer(
        calls_per_minute=5000,
        calls_per_hour=4500,
        monotonic=monotonic,
        sleeper=sleep,
    )

    assert pacer.wait(4500) == 0
    assert pacer.wait(25) == 20


def test_effective_call_pacer_is_thread_safe() -> None:
    current = [0.0]

    def monotonic() -> float:
        return current[0]

    def sleep(seconds: float) -> None:
        current[0] += seconds

    pacer = EffectiveCallPacer(
        calls_per_minute=10,
        calls_per_hour=10,
        monotonic=monotonic,
        sleeper=sleep,
    )

    def spend() -> None:
        for _ in range(10):
            pacer.wait(1)

    threads = [threading.Thread(target=spend) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    # 80 đơn vị tiêu hết, khởi điểm 10, nạp lại 1/6 đơn vị/giây ảo. Token
    # không âm sau cùng chứng tỏ các luồng không tiêu vượt ngân sách chung.
    assert current[0] >= (80 - 10) * 6 - 1e-6
    assert pacer._minute_tokens >= -1e-9
    assert pacer._hour_tokens >= -1e-9


@pytest.mark.parametrize("max_attempts", [0, -1])
def test_http_session_rejects_invalid_max_attempts(max_attempts: int) -> None:
    with pytest.raises(ValueError, match="max_attempts"):
        build_http_session(max_attempts=max_attempts)


def test_retry_sleep_aborts_when_whole_batch_stops() -> None:
    retry = RetryWith429Cooldown(
        total=1,
        allowed_methods=frozenset({"GET"}),
        status_forcelist={429},
        respect_retry_after_header=True,
        fallback_retry_after_429=0.01,
    )
    response = SimpleNamespace(status=429, headers={})

    retry.sleep(response)  # chưa hủy: ngủ 10ms rồi dậy bình thường

    abort_all_retries()
    try:
        with pytest.raises(RetryCancelledError):
            retry.sleep(response)
    finally:
        _RETRY_ABORT.clear()
