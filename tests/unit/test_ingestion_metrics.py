from dataclasses import replace
from datetime import UTC, datetime, timedelta

from vn_climate_risk_monitor.ingestion.state import (
    PipelineHealth,
    PipelineMetrics,
    RunStatus,
)


def _metrics() -> PipelineMetrics:
    observed = datetime(2026, 8, 21, 12, 20, tzinfo=UTC)
    return PipelineMetrics(
        pipeline_name="open_meteo_forecast",
        dataset="forecast",
        scope="production",
        observed_at_utc=observed,
        latest_attempt_id=None,
        latest_run_status=RunStatus.SUCCEEDED,
        latest_scheduled_at_utc=observed - timedelta(minutes=5),
        latest_completed_at_utc=observed - timedelta(minutes=4),
        latest_success_at_utc=observed - timedelta(minutes=5),
        succeeded_runs_24h=1,
        failed_runs_24h=0,
        pending_files=0,
        processing_files=0,
        failed_files=0,
        retry_exhausted_files=0,
        expired_leases=0,
        committed_files_24h=6,
        rows_parsed_24h=9072,
        rows_inserted_24h=9072,
        rescued_rows_24h=0,
    )


def test_recent_complete_pipeline_is_healthy() -> None:
    assert _metrics().health(stale_after=timedelta(hours=2)) == PipelineHealth.HEALTHY


def test_backlog_or_staleness_degrades_pipeline() -> None:
    assert (
        replace(_metrics(), pending_files=1).health(stale_after=timedelta(hours=2))
        == PipelineHealth.DEGRADED
    )
    assert (
        replace(
            _metrics(), latest_success_at_utc=datetime(2026, 8, 21, 9, 0, tzinfo=UTC)
        ).health(stale_after=timedelta(hours=2))
        == PipelineHealth.DEGRADED
    )
    assert (
        replace(_metrics(), latest_run_status=RunStatus.RUNNING).health(
            stale_after=timedelta(hours=2)
        )
        == PipelineHealth.DEGRADED
    )


def test_failed_run_or_exhausted_retry_is_critical() -> None:
    assert (
        replace(_metrics(), latest_run_status=RunStatus.FAILED).health(
            stale_after=timedelta(hours=2)
        )
        == PipelineHealth.CRITICAL
    )
    assert (
        replace(_metrics(), retry_exhausted_files=1).health(
            stale_after=timedelta(hours=2)
        )
        == PipelineHealth.CRITICAL
    )
