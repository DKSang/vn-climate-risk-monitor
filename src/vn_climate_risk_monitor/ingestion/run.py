"""Nối autoloader vào cấu hình nguồn của dự án.

Toàn bộ phần "biết về Open-Meteo" nằm trong ``ingestion/sources/*.yml`` và
``*.sql``. File này chỉ dựng kết nối rồi giao cho engine.

Thêm nguồn mới: tạo thêm 1 cặp YAML + SQL, KHÔNG cần sửa Python.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from autoloader import (
    AutoLoader,
    PostgresIngestionRepository,
    SourceConfig,
    connect_control_plane,
    ensure_ingestion_state,
)
from vn_climate_risk_monitor.config import load_settings
from vn_climate_risk_monitor.lakehouse import get_connection
from vn_climate_risk_monitor.storage import get_minio_client

SOURCES_DIR = Path("ingestion/sources")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "sources",
        nargs="*",
        help="Tên nguồn (không đuôi .yml). Bỏ trống = chạy tất cả.",
    )
    args = parser.parse_args()

    configs = [
        SourceConfig.from_yaml(path)
        for path in sorted(SOURCES_DIR.glob("*.yml"))
        if not args.sources or path.stem in args.sources
    ]
    if not configs:
        raise SystemExit(f"Không tìm thấy nguồn nào trong {SOURCES_DIR}")

    settings = load_settings()
    control = connect_control_plane(settings.postgres.ducklake_connection_string)
    lakehouse = get_connection()
    try:
        ensure_ingestion_state(control)
        checkpoint = PostgresIngestionRepository(control)
        client = get_minio_client(settings.minio)
        exit_code = 0
        for config in configs:
            result = AutoLoader(
                config=config,
                checkpoint=checkpoint,
                object_client=client,
                sql=lakehouse,
                bucket=settings.minio.bucket,
            ).load()
            print(
                f"{result.source}: thấy {result.discovered} file, "
                f"mới {result.newly_registered}, nạp {result.committed_files} file / "
                f"{result.rows_inserted} dòng, lỗi {len(result.failures)}"
            )
            for failure in result.failures:
                print(f"  FAILED {failure}")
                exit_code = 1
    finally:
        lakehouse.close()
        control.close()
    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
