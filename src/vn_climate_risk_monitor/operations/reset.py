"""Confirmation-protected reset of DuckLake Silver and Gold only."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from typing import Any

from vn_climate_risk_monitor.auto_loader.state import connect_control_plane
from vn_climate_risk_monitor.platform.lakehouse import get_connection
from vn_climate_risk_monitor.platform.settings import load_settings

PRIMARY_CATALOG = "catalog1"
PROTECTED_PREFIX = "bronze/files/"
RESETTABLE_SCHEMAS = frozenset({"silver", "gold"})
STAGING_PREFIX = "stg_"


def resettable_objects(
    objects: Sequence[tuple[str, str, str, str]], *, keep_staging: bool = False
) -> list[tuple[str, str, str, str]]:
    """Return only objects in the DuckLake Silver/Gold schemas."""
    return [
        row
        for row in objects
        if row[0] == PRIMARY_CATALOG
        and row[1] in RESETTABLE_SCHEMAS
        and not (keep_staging and row[1] == "silver" and row[2].startswith(STAGING_PREFIX))
    ]


def list_objects(
    duck: Any, *, keep_staging: bool = False
) -> list[tuple[str, str, str, str]]:
    rows = duck.execute(
        """
        SELECT table_catalog, table_schema, table_name, table_type
        FROM information_schema.tables
        WHERE table_catalog = 'catalog1'
          AND table_schema IN ('silver', 'gold')
        ORDER BY 1, 2, 3
        """
    ).fetchall()
    return resettable_objects(rows, keep_staging=keep_staging)


def drop_objects(duck: Any, objects: Sequence[tuple[str, str, str, str]]) -> int:
    dropped = 0
    for catalog, schema, name, kind in resettable_objects(objects):
        keyword = "VIEW" if kind == "VIEW" else "TABLE"
        duck.execute(f'DROP {keyword} IF EXISTS "{catalog}"."{schema}"."{name}"')
        dropped += 1
    return dropped


def ingestion_gap(control: Any) -> tuple[int, int]:
    settings = load_settings()
    from vn_climate_risk_monitor.platform.minio import get_minio_client

    client = get_minio_client(settings.minio)
    live = {
        obj.object_name
        for obj in client.list_objects(
            settings.minio.bucket, PROTECTED_PREFIX, recursive=True
        )
    }
    known = {
        row[0]
        for row in control.execute(
            "SELECT object_key FROM ingestion.ingestion_files"
        ).fetchall()
    }
    return len(known), len(known & live)


def reset_ingestion(control: Any) -> None:
    control.execute("DELETE FROM ingestion.ingestion_files")
    control.execute("DELETE FROM ingestion.ingestion_runs")


def reset_processing(control: Any) -> None:
    control.execute("DELETE FROM processing.processing_state")
    control.execute("DELETE FROM processing.processing_runs")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--list", action="store_true", help="chỉ liệt kê, không xóa")
    parser.add_argument("--yes", action="store_true", help="xác nhận xóa thật")
    parser.add_argument("--keep-staging", action="store_true")
    parser.add_argument("--reset-ingestion", action="store_true")
    parser.add_argument("--reset-processing", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    duck = get_connection()
    objects = list_objects(duck, keep_staging=args.keep_staging)

    if args.list or not args.yes:
        for catalog, schema, name, kind in objects:
            print(f"  {kind:<10} {catalog}.{schema}.{name}")
        print(f"\n{len(objects)} Silver/Gold object sẽ bị xóa. Thêm --yes để thực hiện.")
        print(f"Landing zone {PROTECTED_PREFIX!r} KHÔNG bị đụng tới.")
        duck.close()
        return 0

    settings = load_settings()
    control = connect_control_plane(settings.postgres.ducklake_connection_string)
    try:
        if args.reset_ingestion and args.keep_staging:
            raise SystemExit(
                "--keep-staging và --reset-ingestion mâu thuẫn: giữ bảng staging "
                "nhưng xoá ledger sẽ nạp lại toàn bộ raw vào bảng đã có dữ liệu."
            )
        if args.reset_ingestion:
            known, still_live = ingestion_gap(control)
            print(
                f"Ledger có {known} file; còn trên MinIO {still_live}. "
                f"Nạp lại sẽ MẤT dữ liệu của {known - still_live} file đã biến mất."
            )
        dropped = drop_objects(duck, objects)
        print(
            f"✓ Dropped {dropped} Silver/Gold object. "
            "File cleanup deferred to the scoped maintenance policy."
        )
        if args.reset_ingestion:
            reset_ingestion(control)
            print("✓ Reset ingestion ledger — Bronze không bị xóa")
        if args.reset_processing:
            reset_processing(control)
            print("✓ Reset processing checkpoint")
    finally:
        control.close()
        duck.close()
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:  # noqa: BLE001
        print(f"reset-lakehouse: {type(exc).__name__}: {exc}", file=sys.stderr)
        sys.exit(1)
