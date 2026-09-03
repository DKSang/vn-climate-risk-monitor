#!/usr/bin/env python3
"""Xóa bảng/view DuckLake và (tuỳ chọn) checkpoint, để dựng lại từ raw.

KHÔNG BAO GIỜ chạm ``bronze/files/`` trên MinIO. Đó là landing zone bất biến —
SSOT payload nguồn. Script này chỉ drop object trong catalog DuckLake rồi bảo
DuckLake dọn parquet mà nó tự sinh ra (``bronze/tables/``, ``silver/``, ``gold/``).

    reset_lakehouse.py --list                     xem sẽ xóa gì, không xóa
    reset_lakehouse.py --yes                      drop bảng + view
    reset_lakehouse.py --yes --reset-ingestion    + nạp lại từ raw ở lần load sau
    reset_lakehouse.py --yes --reset-processing   + xoá checkpoint transform

``--reset-ingestion`` là thao tác nguy hiểm nhất: nó xoá ledger file, nên mọi
object còn trên MinIO sẽ được nạp lại — và mọi object ĐÃ BIẾN MẤT khỏi MinIO
sẽ mất luôn phần dữ liệu tương ứng trong Bronze. Script in ra con số chênh lệch
trước khi làm.
"""

from __future__ import annotations

import argparse
import sys
from typing import Any

from autoloader import connect_control_plane
from vn_climate_risk_monitor.config import load_settings
from vn_climate_risk_monitor.lakehouse import get_connection

# Landing zone: script này không có bất kỳ đường nào ghi/xoá vào đây.
PROTECTED_PREFIX = "bronze/files/"


def list_objects(duck: Any) -> list[tuple[str, str, str, str]]:
    return duck.execute(
        """
        SELECT table_catalog, table_schema, table_name, table_type
        FROM information_schema.tables
        WHERE table_catalog IN ('catalog1', 'bronze_store')
        ORDER BY 1, 2, 3
        """
    ).fetchall()


def drop_objects(duck: Any, objects: list[tuple[str, str, str, str]]) -> int:
    dropped = 0
    for catalog, schema, name, kind in objects:
        keyword = "VIEW" if kind == "VIEW" else "TABLE"
        duck.execute(f'DROP {keyword} IF EXISTS "{catalog}"."{schema}"."{name}"')
        dropped += 1
    return dropped


def reclaim_files(duck: Any) -> None:
    """Bảo DuckLake bỏ snapshot cũ rồi xoá parquet không còn ai tham chiếu."""
    for catalog in ("catalog1", "bronze_store"):
        duck.execute(f"CALL ducklake_expire_snapshots('{catalog}', older_than => now())")
        duck.execute(f"CALL ducklake_cleanup_old_files('{catalog}', cleanup_all => true)")


def ingestion_gap(control: Any) -> tuple[int, int]:
    """(số file trong ledger, số file còn thật trên MinIO)."""
    settings = load_settings()
    from vn_climate_risk_monitor.storage import get_minio_client

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
    # files -> runs: có FK, phải xoá con trước.
    control.execute("DELETE FROM ingestion.ingestion_files")
    control.execute("DELETE FROM ingestion.ingestion_runs")


def reset_processing(control: Any) -> None:
    control.execute("DELETE FROM processing.processing_state")
    control.execute("DELETE FROM processing.processing_runs")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--list", action="store_true", help="chỉ liệt kê, không xóa")
    parser.add_argument("--yes", action="store_true", help="xác nhận xóa thật")
    parser.add_argument("--reset-ingestion", action="store_true")
    parser.add_argument("--reset-processing", action="store_true")
    args = parser.parse_args()

    duck = get_connection()
    objects = list_objects(duck)

    if args.list or not args.yes:
        for catalog, schema, name, kind in objects:
            print(f"  {kind:<10} {catalog}.{schema}.{name}")
        print(f"\n{len(objects)} object sẽ bị xóa. Thêm --yes để thực hiện.")
        print(f"Landing zone {PROTECTED_PREFIX!r} KHÔNG bị đụng tới.")
        duck.close()
        return 0

    settings = load_settings()
    control = connect_control_plane(settings.postgres.ducklake_connection_string)
    try:
        if args.reset_ingestion:
            known, still_live = ingestion_gap(control)
            print(
                f"Ledger có {known} file; còn trên MinIO {still_live}. "
                f"Nạp lại sẽ MẤT dữ liệu của {known - still_live} file đã biến mất."
            )

        dropped = drop_objects(duck, objects)
        reclaim_files(duck)
        print(f"✓ Dropped {dropped} object, đã dọn parquet mồ côi")

        if args.reset_ingestion:
            reset_ingestion(control)
            print("✓ Reset ingestion ledger — lần load sau nạp lại toàn bộ raw")
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
        print(f"reset_lakehouse: {type(exc).__name__}: {exc}", file=sys.stderr)
        sys.exit(1)
