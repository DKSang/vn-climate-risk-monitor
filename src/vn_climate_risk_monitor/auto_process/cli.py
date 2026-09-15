"""Automatically run checkpointed dbt flows defined in Python."""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
from datetime import UTC, datetime
from pathlib import Path
from types import FrameType
from uuid import NAMESPACE_URL, UUID, uuid5

from vn_climate_risk_monitor.auto_process.config import (
    ACTIVE_PROCESS_KEYS,
    ProcessConfig,
    load_active_config,
)
from vn_climate_risk_monitor.auto_process.dbt import build_vars, run_dbt
from vn_climate_risk_monitor.auto_process.runner import (
    Bounds,
    ProcessingResult,
    begin_process,
    complete_process,
    restore_bounds,
)
from vn_climate_risk_monitor.auto_process.schema import ensure_processing_state
from vn_climate_risk_monitor.auto_process.state import (
    ProcessingRepository,
    connect_control_plane,
)
from vn_climate_risk_monitor.platform.lakehouse import PRIMARY_CATALOG, get_connection
from vn_climate_risk_monitor.platform.settings import load_settings

TRANSFORM_DIR = Path("transform")


def execution_run_id(process_key: str, execution_key: str) -> UUID:
    """Map an Airflow DAG run to the same processing UUID on every retry."""
    return uuid5(NAMESPACE_URL, f"vn-climate-risk-monitor:{process_key}:{execution_key}")


class Terminated(RuntimeError):
    """Process stopped by a signal while a dbt build was running."""


def _terminate(signum: int, _frame: FrameType | None) -> None:
    raise Terminated(f"received signal {signum}")


def install_signal_handlers() -> None:
    """Convert SIGTERM into an exception so the run is marked FAILED."""
    signal.signal(signal.SIGTERM, _terminate)


def load_config(process_key: str) -> ProcessConfig:
    """Load one supported active flow."""
    try:
        return load_active_config(process_key)
    except ValueError as error:
        raise SystemExit(str(error)) from error


def open_repository() -> tuple[ProcessingRepository, object]:
    settings = load_settings()
    connection = connect_control_plane(settings.postgres.ducklake_connection_string)
    ensure_processing_state(connection)
    return ProcessingRepository(connection), connection


def current_actor() -> str:
    return os.environ.get("PROCESSING_ACTOR") or os.environ.get("USER") or "unknown"


def parse_timestamp(value: str) -> datetime:
    """Treat timestamps without an offset as UTC, not local machine time."""
    moment = datetime.fromisoformat(value)
    return moment if moment.tzinfo else moment.replace(tzinfo=UTC)


def _publication_metrics(config: ProcessConfig) -> dict[str, int | None]:
    """Read row count and the catalog snapshot created by a successful graph."""
    lakehouse = get_connection()
    try:
        row = lakehouse.execute(f"SELECT COUNT(*) FROM {config.target}").fetchone()
        snapshot = lakehouse.execute(
            f"SELECT MAX(snapshot_id) FROM {PRIMARY_CATALOG}.snapshots()"
        ).fetchone()
    finally:
        lakehouse.close()
    return {
        "target_row_count": None if row is None else int(row[0]),
        "published_snapshot_id": (
            None
            if snapshot is None or snapshot[0] is None
            else int(snapshot[0])
        ),
    }


def run_silver_phase(
    config: ProcessConfig,
    bounds: Bounds,
    *,
    dbt_runner=run_dbt,
) -> None:
    """Build only the staging-to-intermediate Silver model."""
    dbt_runner(
        bounds,
        project_dir=TRANSFORM_DIR,
        selection=f"int_weather_{config.process_key}_hourly",
    )


def _running_process(
    config: ProcessConfig, repository: ProcessingRepository, run_id: UUID
) -> ProcessingResult:
    run_id, started_at, saved_bounds = repository.read_running_run(
        run_id=run_id, process_key=config.process_key, scope=config.scope
    )
    return ProcessingResult(
        run_id,
        config.process_key,
        "RUNNING",
        restore_bounds(started_at, saved_bounds),
    )


def cmd_silver(args: argparse.Namespace) -> int:
    install_signal_handlers()
    config = load_config(args.process_key)
    if args.full_refresh and not (args.reason and args.reason.strip()):
        raise SystemExit("--full-refresh cần --reason để audit")

    repository, connection = open_repository()
    try:
        stable_id = (
            execution_run_id(config.process_key, args.execution_key)
            if args.execution_key
            else None
        )
        status = (
            repository.read_run_status(
                run_id=stable_id,
                process_key=config.process_key,
                scope=config.scope,
            )
            if stable_id
            else None
        )
        if status == "SUCCEEDED":
            print(stable_id)
            return 0
        if status == "RUNNING":
            result = _running_process(config, repository, stable_id)  # type: ignore[arg-type]
        elif status is None:
            result = begin_process(
                config=config,
                repository=repository,
                force_full_refresh=args.full_refresh,
                actor=current_actor(),
                reason=args.reason,
                run_id=stable_id,
            )
        else:
            raise RuntimeError(f"processing run {stable_id} is {status}")
        try:
            run_silver_phase(config, result.bounds)
        except BaseException as error:
            if not args.execution_key:
                repository.fail_run(
                    result.run_id,
                    process_key=config.process_key,
                    error=error,
                    completed_at=repository.control_now(),
                )
            raise
    finally:
        connection.close()  # type: ignore[attr-defined]

    print(result.run_id)
    return 0


def cmd_vars(args: argparse.Namespace) -> int:
    config = load_config(args.process_key)
    repository, connection = open_repository()
    try:
        run = _running_process(config, repository, args.run_id)
    finally:
        connection.close()  # type: ignore[attr-defined]
    print(json.dumps(build_vars(run.bounds)))
    return 0


def cmd_finalize(args: argparse.Namespace) -> int:
    config = load_config(args.process_key)
    repository, connection = open_repository()
    try:
        run = _running_process(config, repository, args.run_id)
        result = complete_process(
            config=config,
            repository=repository,
            run=run,
            metrics=_publication_metrics(config),
        )
    finally:
        connection.close()  # type: ignore[attr-defined]
    print(f"{result.process_key}: SUCCEEDED run={result.run_id}")
    return 0


def cmd_refresh_flag(args: argparse.Namespace) -> int:
    config = load_config(args.process_key)
    repository, connection = open_repository()
    try:
        run = _running_process(config, repository, args.run_id)
    finally:
        connection.close()  # type: ignore[attr-defined]
    print("" if run.bounds.is_incremental else "--full-refresh")
    return 0


def cmd_fail(args: argparse.Namespace) -> int:
    config = load_config(args.process_key)
    run_id = args.run_id or execution_run_id(config.process_key, args.execution_key)
    repository, connection = open_repository()
    try:
        run = _running_process(config, repository, run_id)
        repository.fail_run(
            run.run_id,
            process_key=config.process_key,
            error=RuntimeError(args.reason),
            completed_at=repository.control_now(),
        )
    finally:
        connection.close()  # type: ignore[attr-defined]
    return 0


def cmd_state(args: argparse.Namespace) -> int:
    config = load_config(args.process_key)
    repository, connection = open_repository()
    try:
        status = repository.read_run_status(
            run_id=args.run_id,
            process_key=config.process_key,
            scope=config.scope,
        )
    finally:
        connection.close()  # type: ignore[attr-defined]
    if status is None:
        raise RuntimeError(f"processing run {args.run_id} does not exist")
    print(status)
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    config = load_config(args.process_key)
    repository, connection = open_repository()
    try:
        checkpoints = repository.read_checkpoints(
            process_key=config.process_key,
            scope=config.scope,
            source_refs=config.source_refs,
        )
        runs = repository.recent_runs(
            process_key=config.process_key,
            scope=config.scope,
            limit=args.limit,
        )
    finally:
        connection.close()  # type: ignore[attr-defined]

    print(f"{config.process_key} [{config.scope}] → {config.target}")
    print(f"  selection: tag:{config.process_key}")
    for source_ref, checkpoint in checkpoints.items():
        print(f"  {source_ref}: {checkpoint or '(chưa có — full refresh)'}")
    print("  runs:")
    for run in runs:
        (
            _id,
            status,
            started,
            completed,
            candidate,
            who,
            reason,
            error_type,
            error_message,
            rows,
            snapshot,
        ) = run
        line = f"    {status:<9} {started} → {completed or '...'}  ckpt={candidate}"
        if rows is not None:
            line += f"  rows={rows:,}"
        if snapshot is not None:
            line += f"  snapshot=S{snapshot}"
        if reason:
            line += f"  [{who}: {reason}]"
        if error_type:
            line += f"  {error_type}: {(error_message or '')[:80]}"
        print(line)
    return 0


def cmd_reprocess_from(args: argparse.Namespace) -> int:
    config = load_config(args.process_key)
    source_refs = tuple(args.source) if args.source else config.source_refs
    unknown = set(source_refs) - set(config.source_refs)
    if unknown:
        raise SystemExit(f"Source không có trong config: {sorted(unknown)}")

    checkpoint = parse_timestamp(args.from_timestamp)
    repository, connection = open_repository()
    try:
        run_id = repository.rewind(
            process_key=config.process_key,
            scope=config.scope,
            target_ref=config.target,
            source_refs=source_refs,
            checkpoint=checkpoint,
            actor=current_actor(),
            reason=args.reason,
            now=repository.control_now(),
        )
    finally:
        connection.close()  # type: ignore[attr-defined]

    print(f"REWIND {run_id}: {', '.join(source_refs)} → {checkpoint}")
    print("Lần `run` kế sẽ tính lại từ mốc này. MERGE idempotent nên an toàn.")
    return 0


def cmd_abandon(args: argparse.Namespace) -> int:
    config = load_config(args.process_key)
    repository, connection = open_repository()
    try:
        closed = repository.abandon_running(
            process_key=config.process_key,
            scope=config.scope,
            actor=current_actor(),
            reason=args.reason,
            completed_at=repository.control_now(),
        )
    finally:
        connection.close()  # type: ignore[attr-defined]
    print(f"Đóng {closed} run RUNNING mồ côi. Checkpoint KHÔNG đổi.")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    silver = sub.add_parser("silver", help="build Silver intermediate, chưa checkpoint")
    silver.add_argument("process_key", choices=ACTIVE_PROCESS_KEYS)
    silver.add_argument(
        "--full-refresh",
        action="store_true",
        help="rebuild Silver; không xóa checkpoint",
    )
    silver.add_argument("--reason", help="bắt buộc khi dùng --full-refresh")
    silver.add_argument("--execution-key", help="khóa Airflow ổn định qua retry")
    silver.set_defaults(func=cmd_silver)

    state = sub.add_parser("state", help="in trạng thái của đúng processing run")
    state.add_argument("process_key", choices=ACTIVE_PROCESS_KEYS)
    state.add_argument("--run-id", type=UUID, required=True)
    state.set_defaults(func=cmd_state)

    variables = sub.add_parser("vars", help="in dbt vars của run đang mở")
    variables.add_argument("process_key", choices=ACTIVE_PROCESS_KEYS)
    variables.add_argument("--run-id", type=UUID, required=True)
    variables.set_defaults(func=cmd_vars)

    refresh = sub.add_parser("refresh-flag", help="in --full-refresh khi cần")
    refresh.add_argument("process_key", choices=ACTIVE_PROCESS_KEYS)
    refresh.add_argument("--run-id", type=UUID, required=True)
    refresh.set_defaults(func=cmd_refresh_flag)

    finalize = sub.add_parser("finalize", help="publish metrics và advance checkpoint")
    finalize.add_argument("process_key", choices=ACTIVE_PROCESS_KEYS)
    finalize.add_argument("--run-id", type=UUID, required=True)
    finalize.set_defaults(func=cmd_finalize)

    fail = sub.add_parser("fail", help="đóng processing run lỗi, không đổi checkpoint")
    fail.add_argument("process_key", choices=ACTIVE_PROCESS_KEYS)
    fail_id = fail.add_mutually_exclusive_group(required=True)
    fail_id.add_argument("--run-id", type=UUID)
    fail_id.add_argument("--execution-key")
    fail.add_argument("--reason", required=True)
    fail.set_defaults(func=cmd_fail)

    status = sub.add_parser("status", help="checkpoint + run gần nhất")
    status.add_argument("process_key", choices=ACTIVE_PROCESS_KEYS)
    status.add_argument("--limit", type=int, default=5)
    status.set_defaults(func=cmd_status)

    rewind = sub.add_parser("reprocess-from", help="kéo checkpoint lùi, có audit")
    rewind.add_argument("process_key", choices=ACTIVE_PROCESS_KEYS)
    rewind.add_argument("--from", dest="from_timestamp", required=True)
    rewind.add_argument("--reason", required=True)
    rewind.add_argument(
        "--source", action="append", help="chỉ rewind source này (lặp lại được)"
    )
    rewind.set_defaults(func=cmd_reprocess_from)

    abandon = sub.add_parser("abandon", help="đóng run RUNNING mồ côi")
    abandon.add_argument("process_key", choices=ACTIVE_PROCESS_KEYS)
    abandon.add_argument("--reason", required=True)
    abandon.set_defaults(func=cmd_abandon)

    return parser


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
    args = build_parser().parse_args()
    raise SystemExit(args.func(args))


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as exc:  # noqa: BLE001
        print(f"processing: {type(exc).__name__}: {exc}", file=sys.stderr)
        sys.exit(1)
