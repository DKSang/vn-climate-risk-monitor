"""State machine của một lần incremental processing.

    run_started_at = control_now()          ← LẤY TRƯỚC khi đọc bất cứ thứ gì
            ↓
    checkpoint_before = processing_state
    lower_bound = checkpoint_before − safety_lag
            ↓
    begin_run → RUNNING
            ↓
    execute(bounds)
            ↓
       ┌────┴────┐
     FAIL      SUCCESS
       ↓          ↓
    state     state = run_started_at
    KHÔNG đổi
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

from processing.config import ProcessConfig


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
            "lower_bound": (
                self.lower_bound.isoformat() if self.lower_bound else None
            ),
        }


@dataclass(frozen=True)
class Bounds:
    """CỐ Ý không có upper bound.

    Chặn trên bằng ``run_started_at`` nghe có vẻ cho batch deterministic, nhưng
    nó loại đúng phần overlap đang bảo vệ ta. ``_ingested_at`` là transaction
    START time, còn row chỉ visible lúc COMMIT: một row đóng dấu 10:59:58 có thể
    xuất hiện sau khi run 11:00:00 đã đọc xong. Với upper bound đóng, row đó nằm
    ngoài mọi cửa sổ tương lai và mất vĩnh viễn.

    Cách chữa duy nhất là chồng lấn có kiểm soát ở CHẶN DƯỚI (``safety_lag``) và
    dựa vào MERGE idempotent để hấp thụ phần lặp.
    """

    run_started_at: datetime
    sources: tuple[SourceBounds, ...]

    @property
    def is_incremental(self) -> bool:
        """Thiếu checkpoint ở BẤT KỲ source nào là full refresh.

        Source chưa có checkpoint nghĩa là chưa từng được xử lý; lọc incremental
        trên nó sẽ bỏ qua toàn bộ lịch sử.
        """
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
) -> Bounds:
    """checkpoint − safety_lag. Không có checkpoint = None = full refresh."""
    lag = config.checkpoint.safety_lag
    return Bounds(
        run_started_at=run_started_at,
        sources=tuple(
            SourceBounds(
                source_ref=source.ref,
                change_column=source.change_column,
                checkpoint_before=checkpoints.get(source.ref),
                lower_bound=(
                    checkpoints[source.ref] - lag
                    if checkpoints.get(source.ref) is not None
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
    execute: Callable[[Bounds], None],
) -> ProcessingResult:
    """Chạy transform một lần và chỉ advance checkpoint khi nó thành công."""
    run_started_at = repository.control_now()
    checkpoints = repository.read_checkpoints(
        process_key=config.process_key,
        scope=config.scope,
        source_refs=config.source_refs,
    )
    bounds = compute_bounds(config, checkpoints, run_started_at)

    run_id = repository.begin_run(
        process_key=config.process_key,
        scope=config.scope,
        target_ref=config.target,
        started_at=run_started_at,
        bounds=bounds.as_json(),
    )
    try:
        execute(bounds)
    except BaseException as error:
        repository.fail_run(
            run_id, error=error, completed_at=repository.control_now()
        )
        raise

    repository.complete_run(
        run_id,
        process_key=config.process_key,
        scope=config.scope,
        source_refs=config.source_refs,
        # KHÔNG phải completed_at, KHÔNG phải MAX(change_column). Row nào đến
        # trong lúc run chạy sẽ nằm sau mốc này và được lần sau nhặt lên.
        checkpoint=run_started_at,
        completed_at=repository.control_now(),
    )
    return ProcessingResult(
        run_id=run_id,
        process_key=config.process_key,
        status="SUCCEEDED",
        bounds=bounds,
    )
