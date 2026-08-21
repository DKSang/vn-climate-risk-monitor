import pytest
from urllib3.util.retry import Retry

from vn_climate_risk_monitor.ingestion.http import (
    RETRYABLE_STATUS_CODES,
    build_http_session,
)


def test_http_session_has_bounded_get_retry_policy() -> None:
    session = build_http_session(max_attempts=5, pool_size=3)

    adapter = session.get_adapter("https://example.com")
    retry = adapter.max_retries

    assert isinstance(retry, Retry)
    assert retry.total == 4
    assert retry.allowed_methods == frozenset({"GET"})
    assert retry.status_forcelist == RETRYABLE_STATUS_CODES
    assert retry.respect_retry_after_header is True
    assert adapter._pool_connections == 3
    assert adapter._pool_maxsize == 3
    assert session.headers["User-Agent"] == "vn-climate-risk-monitor"
    assert session.headers["Accept-Encoding"] == "identity"


@pytest.mark.parametrize("max_attempts", [0, -1])
def test_http_session_rejects_invalid_max_attempts(max_attempts: int) -> None:
    with pytest.raises(ValueError, match="max_attempts"):
        build_http_session(max_attempts=max_attempts)
