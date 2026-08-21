"""Shared HTTP session policy for source collectors."""

from __future__ import annotations

from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

RETRYABLE_STATUS_CODES = frozenset({408, 429, 500, 502, 503, 504})


def build_http_session(
    *,
    user_agent: str = "vn-climate-risk-monitor",
    max_attempts: int = 5,
    backoff_factor: float = 1.0,
    backoff_jitter: float = 0.5,
    pool_size: int = 10,
) -> Session:
    """Return one reusable session with bounded retries for idempotent GETs.

    ``max_attempts`` includes the initial request, while urllib3's ``total``
    value counts retries after that request.
    """
    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")
    if pool_size < 1:
        raise ValueError("pool_size must be at least 1")

    retry = Retry(
        total=max_attempts - 1,
        connect=max_attempts - 1,
        read=max_attempts - 1,
        status=max_attempts - 1,
        other=0,
        allowed_methods=frozenset({"GET"}),
        status_forcelist=RETRYABLE_STATUS_CODES,
        backoff_factor=backoff_factor,
        backoff_jitter=backoff_jitter,
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
            "User-Agent": user_agent,
            "Accept": "application/json",
            "Accept-Encoding": "identity",
        }
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session
