"""Automatically run checkpointed dbt flows defined in Python."""

from __future__ import annotations

import argparse
import os
import signal
import sys
from datetime import UTC, datetime
from pathlib import Path
from types import FrameType

from vn_climate_risk_monitor.config import load_settings
from vn_climate_risk_monitor.lakehouse import PRIMARY_CATALOG, get_connection
from vn_climate_risk_monitor.processing.config import (
    ACTIVE_PROCESS_KEYS,
    ProcessConfig,
    load_active_config,
)
from vn_climate_risk_monitor.processing.dbt import run_dbt
from vn_climate_risk_monitor.processing.runner import Bounds, run_process
from vn_climate_risk_monitor.processing.schema import ensure_processing_state
from vn_climate_risk_monitor.processing.state import (
    ProcessingRepository,
    connect_control_plane,
)

TRANSFORM_DIR = Path("transform")


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


def _run_one(config: ProcessConfig, args: argparse.Namespace) -> None:
    if args.full_refresh and not (args.reason and args.reason.strip()):
        raise SystemExit("--full-refresh cần --reason để audit")

    repository, connection = open_repository()

    def execute(bounds: Bounds) -> dict[str, int | None]:
        # Read metrics only after dbt build and its data tests succeed.
        run_dbt(
            bounds,
            project_dir=TRANSFORM_DIR,
            selection=f"tag:{config.process_key}",
        )
        return _publication_metrics(config)

    try:
        result = run_process(
            config=config,
            repository=repository,
            execute=execute,
            force_full_refresh=args.full_refresh,
            actor=current_actor(),
            reason=args.reason,
        )
    finally:
        connection.close()  # type: ignore[attr-defined]

    mode = "incremental" if result.bounds.is_incremental else "full refresh"
    print(f"{result.process_key}: {result.status} ({mode}) run={result.run_id}")
    for source in result.bounds.sources:
        print(f"  {source.source_ref}: lower_bound={source.lower_bound}")
    print(f"  checkpoint → {result.bounds.run_started_at} (run start, not end)")


def cmd_run(args: argparse.Namespace) -> int:
    install_signal_handlers()
    process_keys = (args.process_key,) if args.process_key else ACTIVE_PROCESS_KEYS
    for process_key in process_keys:
        _run_one(load_config(process_key), args)
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
    print(f"  safety_lag: {config.safety_lag}")
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

    run = sub.add_parser("run", help="chạy một graph dbt + advance checkpoint")
    run.add_argument(
        "process_key",
        nargs="?",
        choices=ACTIVE_PROCESS_KEYS,
        help="Bỏ trống = tự chạy tất cả process.",
    )
    run.add_argument(
        "--full-refresh",
        action="store_true",
        help="rebuild toàn bộ graph đã chọn; không xóa checkpoint",
    )
    run.add_argument("--reason", help="lý do audit; bắt buộc khi dùng --full-refresh")
    run.set_defaults(func=cmd_run)

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
