"""Typed identities and records for incremental ingestion state."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any
from uuid import UUID, uuid5

LOGICAL_RUN_NAMESPACE = UUID("cb6040e9-4fba-4f57-a8f2-b0bf99c78238")


class RunStatus(StrEnum):
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class FileStatus(StrEnum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMMITTED = "COMMITTED"
    FAILED = "FAILED"


class PipelineHealth(StrEnum):
    HEALTHY = "HEALTHY"
    DEGRADED = "DEGRADED"
    CRITICAL = "CRITICAL"


def logical_schedule_key(scheduled_at_utc: datetime) -> str:
    """Return one canonical key for a timezone-aware logical schedule slot."""
    if scheduled_at_utc.tzinfo is None or scheduled_at_utc.utcoffset() is None:
        raise ValueError("scheduled_at_utc must be timezone-aware")
    return (
        scheduled_at_utc.astimezone(UTC)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def build_logical_run_id(
    *,
    pipeline_name: str,
    dataset: str,
    scope: str,
    logical_key: str,
) -> UUID:
    """Build a deterministic identity shared by all attempts of one logical run."""
    components = (pipeline_name, dataset, scope, logical_key)
    if any(not component.strip() for component in components):
        raise ValueError("logical run identity components must not be empty")
    return uuid5(LOGICAL_RUN_NAMESPACE, "\x1f".join(components))


@dataclass(frozen=True)
class RunAttempt:
    attempt_id: UUID
    logical_run_id: UUID
    attempt_number: int
    logical_key: str
    status: RunStatus


@dataclass(frozen=True)
class ClaimedObject:
    file_id: UUID
    attempt_id: UUID
    logical_run_id: UUID
    pipeline_name: str
    source_name: str
    dataset: str
    scope: str
    object_key: str
    batch_index: int
    size_bytes: int
    sha256: str
    content_type: str
    retry_count: int
    scheduled_at_utc: datetime
    collection_started_at_utc: datetime
    collection_completed_at_utc: datetime
    source_uri: str
    collector_version: str
    contract_version: str
    run_parameters: dict[str, Any]
    file_parameters: dict[str, Any]
    expected_item_count: int | None
    received_item_count: int | None


@dataclass(frozen=True)
class PipelineMetrics:
    """Small operational snapshot derived from the two control tables."""

    pipeline_name: str
    dataset: str
    scope: str
    observed_at_utc: datetime
    latest_attempt_id: UUID | None
    latest_run_status: RunStatus | None
    latest_scheduled_at_utc: datetime | None
    latest_completed_at_utc: datetime | None
    latest_success_at_utc: datetime | None
    succeeded_runs_24h: int
    failed_runs_24h: int
    pending_files: int
    processing_files: int
    failed_files: int
    retry_exhausted_files: int
    expired_leases: int
    committed_files_24h: int
    rows_parsed_24h: int
    rows_inserted_24h: int
    rescued_rows_24h: int

    def health(self, *, stale_after: timedelta) -> PipelineHealth:
        if stale_after <= timedelta(0):
            raise ValueError("stale_after must be positive")
        if self.retry_exhausted_files or self.latest_run_status == RunStatus.FAILED:
            return PipelineHealth.CRITICAL
        if self.latest_run_status == RunStatus.RUNNING:
            return PipelineHealth.DEGRADED
        if self.latest_success_at_utc is None:
            return PipelineHealth.DEGRADED
        if self.latest_success_at_utc < self.observed_at_utc - stale_after:
            return PipelineHealth.DEGRADED
        if self.pending_files or self.processing_files or self.failed_files:
            return PipelineHealth.DEGRADED
        return PipelineHealth.HEALTHY
