from datetime import UTC, datetime

import pytest

from vn_climate_risk_monitor.ingestion.scheduling import latest_hourly_schedule_slot


def test_latest_hourly_slot_uses_current_slot_after_schedule_minute() -> None:
    observed = datetime(2026, 8, 21, 10, 47, 12, tzinfo=UTC)

    assert latest_hourly_schedule_slot(observed, minute=15) == datetime(
        2026, 8, 21, 10, 15, tzinfo=UTC
    )


def test_latest_hourly_slot_uses_previous_hour_before_schedule_minute() -> None:
    observed = datetime(2026, 8, 21, 10, 14, 59, tzinfo=UTC)

    assert latest_hourly_schedule_slot(observed, minute=15) == datetime(
        2026, 8, 21, 9, 15, tzinfo=UTC
    )


def test_latest_hourly_slot_rejects_naive_time() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        latest_hourly_schedule_slot(datetime(2026, 8, 21, 10, 20))  # noqa: DTZ001
