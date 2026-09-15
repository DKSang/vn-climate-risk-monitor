"""Incremental processing state machine with checkpointed source bounds."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

from vn_climate_risk_monitor.auto_process.config import ProcessConfig
from vn_climate_risk_monitor.auto_process.state import ensure_active_process_key


@dataclass(frozen=True)
class SourceBounds:
    """Cửa sổ đọc của một source trong lần chạy này."""

    source_ref: str
    change_column: str
    checkpoint_before: datetime | None
    lower_bound: datetime | None

    def as_json(self) -> dict[str, str | None]:
        return {
            "change_column": self.change_column,
            "checkpoint_before": (
                self.checkpoint_before.isoformat() if self.checkpoint_before else None
            ),
            "lower_bound": (self.lower_bound.isoformat() if self.lower_bound else None),
        }


@dataclass(frozen=True)
class Bounds:
    """Incremental source bounds captured for one processing run."""

    run_started_at: datetime
    sources: tuple[SourceBounds, ...]

    @property
    def is_incremental(self) -> bool:
        """Require checkpoints for every source before incremental mode."""
        return bool(self.sources) and all(
            source.lower_bound is not None for source in self.sources
        )

    def as_json(self) -> dict[str, dict[str, str | None]]:
        return {source.source_ref: source.as_json() for source in self.sources}


@dataclass(frozen=True)
class ProcessingResult:
    run_id: UUID
    process_key: str
    status: str
    bounds: Bounds


def restore_bounds(
    run_started_at: datetime, saved: Mapping[str, Any]
) -> Bounds:
    """Restore bounds persisted in PostgreSQL for a later Airflow task."""
    def moment(value: str | None) -> datetime | None:
        return datetime.fromisoformat(value) if value else None

    return Bounds(
        run_started_at=run_started_at,
        sources=tuple(
            SourceBounds(
                source_ref=source_ref,
                change_column=values["change_column"],
                checkpoint_before=moment(values.get("checkpoint_before")),
                lower_bound=moment(values.get("lower_bound")),
            )
            for source_ref, values in saved.items()
        ),
    )


def compute_bounds(
    config: ProcessConfig,
    checkpoints: dict[str, datetime | None],
    run_started_at: datetime,
    *,
    force_full_refresh: bool = False,
) -> Bounds:
    """Build lower bounds directly from successful checkpoints."""
    return Bounds(
        run_started_at=run_started_at,
        sources=tuple(
            SourceBounds(
                source_ref=source.ref,
                change_column=source.change_column,
                checkpoint_before=checkpoints.get(source.ref),
                lower_bound=(
                    checkpoints[source.ref]
                    if (
                        not force_full_refresh
                        and checkpoints.get(source.ref) is not None
                    )
                    else None
                ),
            )
            for source in config.sources
        ),
    )


def begin_process(
    *,
    config: ProcessConfig,
    repository: Any,
    force_full_refresh: bool = False,
    actor: str = "runner",
    reason: str | None = None,
    run_id: UUID | None = None,
) -> ProcessingResult:
    """Open one durable processing run without advancing checkpoints."""
    ensure_active_process_key(config.process_key)
    if force_full_refresh and not (reason and reason.strip()):
        raise ValueError("full refresh cần reason để audit")

    started_at = repository.control_now()
    checkpoints = repository.read_checkpoints(
        process_key=config.process_key,
        scope=config.scope,
        source_refs=config.source_refs,
    )
    first_run = all(checkpoints.get(ref) is None for ref in config.source_refs)
    bounds = compute_bounds(
        config,
        checkpoints,
        started_at,
        force_full_refresh=force_full_refresh,
    )
    created_run_id = repository.begin_run(
        process_key=config.process_key,
        scope=config.scope,
        target_ref=config.target,
        started_at=started_at,
        bounds=bounds.as_json(),
        actor=actor,
        reason=(reason.strip() if reason else "controlled first run" if first_run else None),
        **({"run_id": run_id} if run_id else {}),
    )
    return ProcessingResult(created_run_id, config.process_key, "RUNNING", bounds)


def complete_process(
    *,
    config: ProcessConfig,
    repository: Any,
    run: ProcessingResult,
    metrics: Mapping[str, int | None] | None = None,
) -> ProcessingResult:
    """Publish metrics and checkpoint only after every external phase passed."""
    repository.complete_run(
        run.run_id,
        process_key=config.process_key,
        scope=config.scope,
        source_refs=config.source_refs,
        checkpoint=run.bounds.run_started_at,
        completed_at=repository.control_now(),
        metrics=metrics,
    )
    return ProcessingResult(run.run_id, config.process_key, "SUCCEEDED", run.bounds)


def run_process(
    *,
    config: ProcessConfig,
    repository: Any,
    execute: Callable[[Bounds], Mapping[str, int | None] | None],
    force_full_refresh: bool = False,
    actor: str = "runner",
    reason: str | None = None,
) -> ProcessingResult:
    """Run one transform and advance checkpoints only after success."""
    run = begin_process(
        config=config,
        repository=repository,
        force_full_refresh=force_full_refresh,
        actor=actor,
        reason=reason,
    )
    try:
        metrics = execute(run.bounds)
    except BaseException as error:
        repository.fail_run(
            run.run_id,
            process_key=config.process_key,
            error=error,
            completed_at=repository.control_now(),
        )
        raise
    return complete_process(
        config=config,
        repository=repository,
        run=run,
        metrics=metrics,
    )
