"""Incremental processing state-machine tests with a fake repository."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import pytest

from vn_climate_risk_monitor.auto_process.config import (
    ProcessConfig,
    SourceBinding,
)
from vn_climate_risk_monitor.auto_process.runner import (
    begin_process,
    complete_process,
    compute_bounds,
    restore_bounds,
    run_process,
)


def at(hour: int, minute: int = 0, second: int = 0) -> datetime:
    return datetime(2026, 9, 3, hour, minute, second, tzinfo=UTC)


class FakeRepository:
    """Ghi nhớ trong RAM, đủ hình dạng cho runner."""

    def __init__(
        self,
        *,
        clock: list[datetime],
        checkpoints: dict[str, datetime] | None = None,
    ) -> None:
        self._clock = clock
        self._tick = 0
        self.checkpoints: dict[str, datetime | None] = dict(checkpoints or {})
        self.begun: list[dict[str, Any]] = []
        self.completed: list[dict[str, Any]] = []
        self.failed: list[dict[str, Any]] = []

    def control_now(self) -> datetime:
        moment = self._clock[min(self._tick, len(self._clock) - 1)]
        self._tick += 1
        return moment

    def read_checkpoints(
        self, *, process_key: str, scope: str, source_refs: tuple[str, ...]
    ) -> dict[str, datetime | None]:
        del process_key, scope
        return {ref: self.checkpoints.get(ref) for ref in source_refs}

    def begin_run(self, **kwargs: Any) -> UUID:
        run_id = kwargs.pop("run_id", None) or uuid4()
        self.begun.append({"run_id": run_id, **kwargs})
        return run_id

    def complete_run(self, run_id: UUID, **kwargs: Any) -> None:
        self.completed.append({"run_id": run_id, **kwargs})
        for source_ref in kwargs["source_refs"]:
            self.checkpoints[source_ref] = kwargs["checkpoint"]

    def fail_run(self, run_id: UUID, **kwargs: Any) -> None:
        self.failed.append({"run_id": run_id, **kwargs})


def build_config(
    *,
    sources: tuple[str, ...] = ("archive_hourly",),
) -> ProcessConfig:
    return ProcessConfig(
        process_key="archive",
        target="gold.fct_rainfall_historical_hourly",
        sources=tuple(SourceBinding(ref=ref) for ref in sources),
        scope="test",
    )


# ── checkpoint là START time ──────────────────────────────────────────────────
def test_checkpoint_is_run_start_not_run_end() -> None:
    """Checkpoint successful runs at start time."""
    repository = FakeRepository(clock=[at(10, 0), at(10, 10)])

    run_process(
        config=build_config(),
        repository=repository,
        execute=lambda bounds: None,
    )

    written = repository.completed[0]
    assert written["checkpoint"] == at(10, 0)
    assert written["completed_at"] == at(10, 10)
    assert written["checkpoint"] != written["completed_at"]


def test_split_process_does_not_checkpoint_until_complete() -> None:
    repository = FakeRepository(clock=[at(10, 0), at(10, 10)])
    config = build_config()

    running = begin_process(config=config, repository=repository)

    assert running.status == "RUNNING"
    assert repository.completed == []
    assert repository.checkpoints == {}

    complete_process(config=config, repository=repository, run=running)

    assert repository.completed[0]["checkpoint"] == at(10, 0)


def test_begin_process_uses_caller_supplied_stable_run_id() -> None:
    repository = FakeRepository(clock=[at(10, 0)])
    stable_id = uuid4()

    running = begin_process(
        config=build_config(), repository=repository, run_id=stable_id
    )

    assert running.run_id == stable_id
    assert repository.begun[0]["run_id"] == stable_id


def test_saved_bounds_can_be_restored_by_a_later_airflow_task() -> None:
    original = compute_bounds(
        build_config(), {"archive_hourly": at(9, 0)}, at(10, 0)
    )

    restored = restore_bounds(original.run_started_at, original.as_json())

    assert restored == original


def test_row_arriving_mid_run_is_picked_up_next_time() -> None:
    """Hồi quy trực tiếp cho bug của `MAX(_ingested_at)` sau khi dbt xong."""
    config = build_config()
    repository = FakeRepository(clock=[at(10, 0), at(10, 10)])
    run_process(config=config, repository=repository, execute=lambda b: None)

    row_ingested_at = at(10, 5)  # autoloader commit trong lúc run 1 đang chạy
    next_bounds = compute_bounds(
        config,
        repository.read_checkpoints(
            process_key=config.process_key,
            scope=config.scope,
            source_refs=config.source_refs,
        ),
        at(11, 0),
    )

    lower = next_bounds.sources[0].lower_bound
    assert lower is not None
    assert row_ingested_at > lower, "row giữa run phải nằm trong cửa sổ kế tiếp"


# ── chỉ advance khi thành công ────────────────────────────────────────────────
def test_failed_run_leaves_checkpoint_unchanged() -> None:
    repository = FakeRepository(
        clock=[at(11, 0), at(11, 7)], checkpoints={"archive_hourly": at(10, 0)}
    )

    def boom(_bounds: Any) -> None:
        raise RuntimeError("dbt exit 1")

    with pytest.raises(RuntimeError, match="dbt exit 1"):
        run_process(config=build_config(), repository=repository, execute=boom)

    assert repository.checkpoints["archive_hourly"] == at(10, 0)
    assert repository.completed == []
    assert repository.failed[0]["completed_at"] == at(11, 7)


def test_base_exception_also_marks_the_run_failed() -> None:
    """Termination must mark a RUNNING process as failed."""
    repository = FakeRepository(
        clock=[at(11, 0), at(11, 2)], checkpoints={"archive_hourly": at(10, 0)}
    )

    def terminated(_bounds: Any) -> None:
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        run_process(config=build_config(), repository=repository, execute=terminated)

    assert repository.failed[0]["error"].__class__ is KeyboardInterrupt
    assert repository.checkpoints["archive_hourly"] == at(10, 0)


def test_retry_after_failure_reprocesses_the_same_window() -> None:
    config = build_config()
    repository = FakeRepository(
        clock=[at(11, 0), at(11, 7)], checkpoints={"archive_hourly": at(10, 0)}
    )
    with pytest.raises(RuntimeError):
        run_process(
            config=config,
            repository=repository,
            execute=lambda b: (_ for _ in ()).throw(RuntimeError("boom")),
        )

    retry = compute_bounds(
        config,
        repository.read_checkpoints(
            process_key=config.process_key,
            scope=config.scope,
            source_refs=config.source_refs,
        ),
        at(11, 30),
    )

    assert retry.sources[0].checkpoint_before == at(10, 0)


# ── full refresh ──────────────────────────────────────────────────────────────
def test_first_run_has_no_lower_bound() -> None:
    bounds = compute_bounds(build_config(), {"archive_hourly": None}, at(10, 0))

    assert bounds.sources[0].lower_bound is None
    assert bounds.is_incremental is False


def test_one_missing_checkpoint_forces_full_refresh() -> None:
    """Source chưa từng xử lý mà lọc incremental thì bỏ qua toàn bộ lịch sử của nó."""
    config = build_config(sources=("archive_hourly", "ifs_hourly"))

    bounds = compute_bounds(
        config, {"archive_hourly": at(10, 0), "ifs_hourly": None}, at(11, 0)
    )

    assert bounds.is_incremental is False


def test_all_checkpoints_present_is_incremental() -> None:
    config = build_config(sources=("archive_hourly", "ifs_hourly"))

    bounds = compute_bounds(
        config, {"archive_hourly": at(10, 0), "ifs_hourly": at(9, 0)}, at(11, 0)
    )

    assert bounds.is_incremental is True
    assert {s.source_ref: s.lower_bound for s in bounds.sources} == {
        "archive_hourly": at(10, 0),
        "ifs_hourly": at(9, 0),
    }


def test_forced_full_refresh_keeps_checkpoint_for_audit_but_omits_bound() -> None:
    checkpoint = at(10, 0)

    bounds = compute_bounds(
        build_config(),
        {"archive_hourly": checkpoint},
        at(11, 0),
        force_full_refresh=True,
    )

    assert bounds.is_incremental is False
    assert bounds.sources[0].checkpoint_before == checkpoint
    assert bounds.sources[0].lower_bound is None


def test_forced_full_refresh_requires_reason() -> None:
    repository = FakeRepository(
        clock=[at(11, 0)], checkpoints={"archive_hourly": at(10, 0)}
    )

    with pytest.raises(ValueError, match="reason"):
        run_process(
            config=build_config(),
            repository=repository,
            execute=lambda b: None,
            force_full_refresh=True,
        )

    assert repository.begun == []


def test_forced_full_refresh_is_audited_and_advances_on_success() -> None:
    repository = FakeRepository(
        clock=[at(11, 0), at(11, 5)],
        checkpoints={"archive_hourly": at(10, 0)},
    )
    seen: list[Any] = []

    run_process(
        config=build_config(),
        repository=repository,
        execute=seen.append,
        force_full_refresh=True,
        actor="deployer",
        reason="rain band v2",
    )

    assert seen[0].is_incremental is False
    assert repository.begun[0]["actor"] == "deployer"
    assert repository.begun[0]["reason"] == "rain band v2"
    assert repository.checkpoints["archive_hourly"] == at(11, 0)


# ── audit ─────────────────────────────────────────────────────────────────────
def test_run_records_bounds_for_audit() -> None:
    repository = FakeRepository(
        clock=[at(11, 0), at(11, 5)], checkpoints={"archive_hourly": at(10, 0)}
    )

    run_process(config=build_config(), repository=repository, execute=lambda b: None)

    recorded = repository.begun[0]
    assert recorded["started_at"] == at(11, 0)
    assert recorded["target_ref"] == "gold.fct_rainfall_historical_hourly"
    assert recorded["bounds"]["archive_hourly"]["checkpoint_before"] == (
        at(10, 0).isoformat()
    )
    assert recorded["bounds"]["archive_hourly"]["lower_bound"] == at(10, 0).isoformat()


def test_metrics_from_execute_are_recorded_with_the_run() -> None:
    """ "Run xanh nhưng bảng rỗng" chỉ thấy được nếu số dòng được ghi lại."""
    repository = FakeRepository(clock=[at(11, 0), at(11, 5)])

    run_process(
        config=build_config(),
        repository=repository,
        execute=lambda b: {"target_row_count": 5_818_584, "published_snapshot_id": 42},
    )

    assert repository.completed[0]["metrics"]["target_row_count"] == 5_818_584
    assert repository.completed[0]["metrics"]["published_snapshot_id"] == 42


def test_execute_may_return_no_metrics() -> None:
    """Metrics là tuỳ chọn, không phải điều kiện để run thành công."""
    repository = FakeRepository(clock=[at(11, 0), at(11, 5)])

    run_process(config=build_config(), repository=repository, execute=lambda b: None)

    assert repository.completed[0]["metrics"] is None


def test_run_is_opened_before_transform_executes() -> None:
    """Run phải RUNNING trước khi transform chạy, nếu không crash là mất audit."""
    repository = FakeRepository(clock=[at(11, 0), at(11, 5)])
    seen: list[int] = []

    run_process(
        config=build_config(),
        repository=repository,
        execute=lambda b: seen.append(len(repository.begun)),
    )

    assert seen == [1]
