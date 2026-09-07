#!/usr/bin/env python3
"""CLI cho processing framework: chạy, xem, và rewind checkpoint có audit.

    run             chạy dbt (incremental/full-refresh), checkpoint chỉ advance khi xanh
    status          checkpoint hiện tại + vài run gần nhất
    reprocess-from  kéo checkpoint lùi để tính lại (thay cho UPDATE tay)
    abandon         đóng run RUNNING mồ côi
    migrate         chuyển ingestion.gold_watermarks → processing_state
"""

from __future__ import annotations

import argparse
import os
import signal
import sys
from datetime import UTC, datetime
from pathlib import Path
from types import FrameType

from processing import (
    ProcessConfig,
    ProcessingRepository,
    apply_soft_delete,
    ensure_processing_state,
    run_dbt,
    run_process,
)
from processing.softdelete import SoftDeleteResult
from processing.state import connect_control_plane
from vn_climate_risk_monitor.config import load_settings
from vn_climate_risk_monitor.lakehouse import get_connection

PROCESSING_DIR = Path("processing")
TRANSFORM_DIR = Path("transform")


class Terminated(RuntimeError):
    """Process bị dừng bằng tín hiệu trong lúc transform đang chạy."""


def _terminate(signum: int, _frame: FrameType | None) -> None:
    raise Terminated(f"received signal {signum}")


def install_signal_handlers() -> None:
    """Biến SIGTERM thành exception để run được đánh FAILED trước khi chết.

    Mặc định SIGTERM giết process ngay, không chạy ``except``, nên bỏ lại một row
    ``RUNNING`` mồ côi — và unique index chặn RUNNING sẽ khóa mọi lần chạy sau cho
    tới khi có người `abandon` tay. Đây không phải đường hiếm: cron bọc `timeout`,
    `systemd stop`, và Docker stop đều gửi SIGTERM giữa lúc `dbt build` chạy vài
    phút. Đo 2026-09-03: `timeout 60` trên một build 3 phút để lại đúng row đó.

    SIGINT đã tự raise ``KeyboardInterrupt`` nên không cần xử lý.
    """
    signal.signal(signal.SIGTERM, _terminate)


def load_config(process_key: str) -> ProcessConfig:
    path = PROCESSING_DIR / f"{process_key}.yml"
    if not path.is_file():
        raise SystemExit(f"Không tìm thấy process config: {path}")
    return ProcessConfig.from_yaml(path)


def open_repository() -> tuple[ProcessingRepository, object]:
    settings = load_settings()
    connection = connect_control_plane(settings.postgres.ducklake_connection_string)
    ensure_processing_state(connection)
    return ProcessingRepository(connection), connection


def actor() -> str:
    return os.environ.get("PROCESSING_ACTOR") or os.environ.get("USER") or "unknown"


def parse_timestamp(value: str) -> datetime:
    """`--from` không có offset thì hiểu là UTC, không phải giờ máy đang chạy."""
    moment = datetime.fromisoformat(value)
    return moment if moment.tzinfo else moment.replace(tzinfo=UTC)


# ── commands ─────────────────────────────────────────────────────────────────
def cmd_run(args: argparse.Namespace) -> int:
    if args.full_refresh and not (args.reason and args.reason.strip()):
        raise SystemExit("--full-refresh cần --reason để audit")

    config = load_config(args.process_key)
    select = None if args.full_graph else (args.select or config.runner.select)
    install_signal_handlers()
    repository, connection = open_repository()
    deletions: list[SoftDeleteResult] = []

    def execute(bounds: object) -> dict[str, int | None]:
        """dbt rồi soft delete, TRONG CÙNG một run.

        Thứ tự bắt buộc: soft delete đọc bảng mà dbt vừa ghi. Và vì nó nằm trong
        `execute`, lỗi ở đây làm run FAILED nên checkpoint KHÔNG nhích — lần sau
        chạy lại đúng cửa sổ đó.
        """
        run_dbt(bounds, project_dir=TRANSFORM_DIR, select=select)  # type: ignore[arg-type]
        lakehouse = get_connection()
        try:
            for rule in config.soft_delete:
                deletions.append(
                    apply_soft_delete(
                        lakehouse,
                        rule,
                        now=bounds.run_started_at,  # type: ignore[attr-defined]
                    )
                )
            row = lakehouse.execute(
                f"SELECT COUNT(*) FROM {config.target}"
            ).fetchone()
        finally:
            lakehouse.close()
        return {
            "target_row_count": None if row is None else int(row[0]),
            "rows_deactivated": (
                sum(d.deactivated for d in deletions) if deletions else None
            ),
            "rows_reactivated": (
                sum(d.reactivated for d in deletions) if deletions else None
            ),
        }

    try:
        result = run_process(
            config=config,
            repository=repository,
            execute=execute,
            force_full_refresh=args.full_refresh,
            actor=actor(),
            reason=args.reason,
        )
    finally:
        connection.close()  # type: ignore[attr-defined]

    mode = "incremental" if result.bounds.is_incremental else "full refresh"
    print(f"{result.process_key}: {result.status} ({mode}) run={result.run_id}")
    for source in result.bounds.sources:
        print(f"  {source.source_ref}: lower_bound={source.lower_bound}")
    for deletion in deletions:
        print(
            f"  soft delete {deletion.target}: "
            f"{deletion.source_keys} key nguồn, "
            f"tắt {deletion.deactivated}, bật lại {deletion.reactivated}"
        )
    print(f"  checkpoint → {result.bounds.run_started_at} (run start, not end)")
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
            process_key=config.process_key, scope=config.scope, limit=args.limit
        )
    finally:
        connection.close()  # type: ignore[attr-defined]

    print(f"{config.process_key} [{config.scope}] → {config.target}")
    print(f"  safety_lag: {config.checkpoint.safety_lag}")
    for source_ref, checkpoint in checkpoints.items():
        print(f"  {source_ref}: {checkpoint or '(chưa có — full refresh)'}")
    print("  runs:")
    for run in runs:
        (_id, status, started, completed, candidate, who, reason, etype, emsg,
         rows, off, on) = run
        line = f"    {status:<9} {started} → {completed or '...'}  ckpt={candidate}"
        if rows is not None:
            line += f"  rows={rows:,}"
        if off or on:
            line += f"  soft_delete(-{off or 0}/+{on or 0})"
        if reason:
            line += f"  [{who}: {reason}]"
        if etype:
            line += f"  {etype}: {(emsg or '')[:80]}"
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
            actor=actor(),
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
            actor=actor(),
            reason=args.reason,
            completed_at=repository.control_now(),
        )
    finally:
        connection.close()  # type: ignore[attr-defined]
    print(f"Đóng {closed} run RUNNING mồ côi. Checkpoint KHÔNG đổi.")
    return 0


def cmd_migrate(args: argparse.Namespace) -> int:
    """Chuyển watermark cũ sang processing_state.

    Giá trị cũ là ``MAX(_ingested_at)`` đọc SAU khi dbt xong, nên nó có thể đi
    TRƯỚC thứ đã thực sự được xử lý — đó chính là bug thiết kế cũ. Migration chỉ
    copy nguyên trạng; đóng khoảng hở là quyết định của người vận hành qua
    `reprocess-from`, không phải thứ script tự đoán.
    """
    config = load_config(args.process_key)
    legacy = args.legacy_pipeline or config.process_key
    repository, connection = open_repository()
    try:
        row = connection.execute(  # type: ignore[attr-defined]
            """
            SELECT last_successful_ingestion_watermark
            FROM ingestion.gold_watermarks
            WHERE pipeline_name = %s
            """,
            (legacy,),
        ).fetchone()
        if row is None:
            print(f"Không có watermark cũ cho {legacy!r}; bỏ qua.")
            return 0
        run_id = repository.rewind(
            process_key=config.process_key,
            scope=config.scope,
            target_ref=config.target,
            source_refs=config.source_refs,
            checkpoint=row[0],
            actor=actor(),
            reason=f"migrate from ingestion.gold_watermarks ({legacy})",
            now=repository.control_now(),
        )
    finally:
        connection.close()  # type: ignore[attr-defined]

    print(f"Migrated {legacy} → {config.process_key}: {row[0]} (run {run_id})")
    print(
        "⚠️  Giá trị cũ dựa trên MAX(_ingested_at) đọc sau khi dbt xong, nên có thể\n"
        "    đã nhảy qua rows chưa xử lý. Cân nhắc chạy một lần:\n"
        f"    scripts/run_processing.py reprocess-from {config.process_key} "
        '--from <ts> --reason "close gap from MAX() watermark bug"'
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="chạy transform + advance checkpoint")
    run.add_argument("process_key")
    run.add_argument("--select", help="ghi đè runner.select trong config")
    run.add_argument(
        "--full-graph",
        action="store_true",
        help="bỏ qua runner.select, build toàn bộ project",
    )
    run.add_argument(
        "--full-refresh",
        action="store_true",
        help="rebuild toàn bộ model đã select; không xóa checkpoint",
    )
    run.add_argument(
        "--reason",
        help="lý do audit; bắt buộc khi dùng --full-refresh",
    )
    run.set_defaults(func=cmd_run)

    status = sub.add_parser("status", help="checkpoint + run gần nhất")
    status.add_argument("process_key")
    status.add_argument("--limit", type=int, default=5)
    status.set_defaults(func=cmd_status)

    rewind = sub.add_parser("reprocess-from", help="kéo checkpoint lùi, có audit")
    rewind.add_argument("process_key")
    rewind.add_argument("--from", dest="from_timestamp", required=True)
    rewind.add_argument("--reason", required=True)
    rewind.add_argument(
        "--source", action="append", help="chỉ rewind source này (lặp lại được)"
    )
    rewind.set_defaults(func=cmd_reprocess_from)

    abandon = sub.add_parser("abandon", help="đóng run RUNNING mồ côi")
    abandon.add_argument("process_key")
    abandon.add_argument("--reason", required=True)
    abandon.set_defaults(func=cmd_abandon)

    migrate = sub.add_parser("migrate", help="gold_watermarks → processing_state")
    migrate.add_argument("process_key")
    migrate.add_argument("--legacy-pipeline")
    migrate.set_defaults(func=cmd_migrate)

    return parser


def main() -> None:
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
