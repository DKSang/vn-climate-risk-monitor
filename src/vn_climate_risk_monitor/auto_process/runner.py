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
    ensure_active_process_key(config.process_key)

    if force_full_refresh and not (reason and reason.strip()):
        raise ValueError("full refresh cần reason để audit")

    run_started_at = repository.control_now()
    checkpoints = repository.read_checkpoints(
        process_key=config.process_key,
        scope=config.scope,
        source_refs=config.source_refs,
    )
    first_run = all(checkpoints.get(source_ref) is None for source_ref in config.source_refs)
    bounds = compute_bounds(
        config,
        checkpoints,
        run_started_at,
        force_full_refresh=force_full_refresh,
    )

    run_id = repository.begin_run(
        process_key=config.process_key,
        scope=config.scope,
        target_ref=config.target,
        started_at=run_started_at,
        bounds=bounds.as_json(),
        actor=actor,
        reason=(
            reason.strip()
            if reason
            else "controlled first run"
            if first_run
            else None
        ),
    )
    try:
        metrics = execute(bounds)
    except BaseException as error:
        repository.fail_run(
            run_id,
            process_key=config.process_key,
            error=error,
            completed_at=repository.control_now(),
        )
        raise

    repository.complete_run(
        run_id,
        process_key=config.process_key,
        scope=config.scope,
        source_refs=config.source_refs,
        # Checkpoint at run start so mid-run arrivals stay eligible next run.
        checkpoint=run_started_at,
        completed_at=repository.control_now(),
        metrics=metrics,
    )
    return ProcessingResult(
        run_id=run_id,
        process_key=config.process_key,
        status="SUCCEEDED",
        bounds=bounds,
    )
