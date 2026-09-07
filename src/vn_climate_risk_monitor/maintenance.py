"""DuckLake retention: expire snapshot cũ, cleanup file sau grace period."""

from __future__ import annotations

import argparse

from vn_climate_risk_monitor.lakehouse import PRIMARY_CATALOG, get_connection


def build_statements(
    catalog: str,
    *,
    snapshot_retention_days: int,
    file_grace_days: int,
    dry_run: bool,
) -> tuple[str, str]:
    """SQL maintenance; các duration là integer đã validate trước khi nội suy."""
    if snapshot_retention_days < 1:
        raise ValueError("snapshot_retention_days phải >= 1")
    if file_grace_days < 1:
        raise ValueError("file_grace_days phải >= 1")
    preview = "true" if dry_run else "false"
    return (
        (
            f"CALL ducklake_expire_snapshots('{catalog}', "
            f"dry_run => {preview}, "
            f"older_than => now() - INTERVAL {snapshot_retention_days} DAY)"
        ),
        (
            f"CALL ducklake_cleanup_old_files('{catalog}', "
            f"dry_run => {preview}, "
            f"older_than => now() - INTERVAL {file_grace_days} DAY)"
        ),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot-retention-days", type=int, default=7)
    parser.add_argument("--file-grace-days", type=int, default=2)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="chỉ liệt kê snapshot/file sẽ xóa",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    statements = build_statements(
        PRIMARY_CATALOG,
        snapshot_retention_days=args.snapshot_retention_days,
        file_grace_days=args.file_grace_days,
        dry_run=args.dry_run,
    )
    connection = get_connection()
    try:
        labels = ("snapshots", "files")
        for label, statement in zip(labels, statements, strict=True):
            affected = connection.execute(statement).fetchall()
            action = "would affect" if args.dry_run else "affected"
            print(f"{label}: {action} {len(affected)} item(s)")
    finally:
        connection.close()
