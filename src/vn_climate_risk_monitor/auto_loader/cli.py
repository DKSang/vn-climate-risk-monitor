"""Automatically load code-native source groups into Silver staging."""

from __future__ import annotations

import argparse
import sys

from vn_climate_risk_monitor.auto_loader.config import SOURCE_GROUPS, source_configs
from vn_climate_risk_monitor.auto_loader.loader import AutoLoader
from vn_climate_risk_monitor.auto_loader.state import (
    PostgresIngestionRepository,
    connect_control_plane,
    ensure_ingestion_state,
)
from vn_climate_risk_monitor.platform.lakehouse import get_connection
from vn_climate_risk_monitor.platform.minio import get_minio_client
from vn_climate_risk_monitor.platform.settings import load_settings


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "group",
        nargs="?",
        choices=tuple(SOURCE_GROUPS),
        help="Nhóm nguồn cần nạp. Bỏ trống = tự nạp tất cả.",
    )
    args = parser.parse_args()

    configs = source_configs(args.group)

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
