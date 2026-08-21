"""Deterministic schedule slots for idempotent batch pipelines."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta


def latest_hourly_schedule_slot(
    observed_at_utc: datetime,
    *,
    minute: int = 15,
) -> datetime:
    """Return the most recent hourly UTC slot at ``minute``.

    A scheduler retry within the same hour therefore resolves to the same
    logical run instead of creating a new run from wall-clock seconds.
    """
    if observed_at_utc.tzinfo is None or observed_at_utc.utcoffset() is None:
        raise ValueError("observed_at_utc must be timezone-aware")
    if not 0 <= minute <= 59:
        raise ValueError("minute must be between 0 and 59")

    observed_at_utc = observed_at_utc.astimezone(UTC)
    candidate = observed_at_utc.replace(minute=minute, second=0, microsecond=0)
    if candidate > observed_at_utc:
        candidate -= timedelta(hours=1)
    return candidate
