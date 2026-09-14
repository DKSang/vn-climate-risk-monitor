"""Automatically load code-native source groups into Silver staging."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from vn_climate_risk_monitor.config import load_settings
from vn_climate_risk_monitor.ingestion.loader import AutoLoader, SourceConfig
from vn_climate_risk_monitor.ingestion.state import (
    PostgresIngestionRepository,
    connect_control_plane,
    ensure_ingestion_state,
)
from vn_climate_risk_monitor.lakehouse import get_connection
from vn_climate_risk_monitor.storage.minio import get_minio_client

SOURCE_DIR = Path("sources")
SOURCE_GROUPS = {
    "forecast": (
        SourceConfig(
            name="open_meteo_forecast",
            dataset="forecast",
            prefix="bronze/files/open_meteo/forecast",
            pattern="**/response_*.json",
            sql_file="open_meteo_forecast.sql",
            target="catalog1.silver.stg_weather_forecast",
            batch_size=10,
            parameters={},
            base_dir=SOURCE_DIR,
        ),
    ),
    "archive": (
        SourceConfig(
            name="open_meteo_archive",
            dataset="weather_hourly_era5",
            prefix="bronze/files/open_meteo/historical_weather_hourly/backfill",
            pattern="**/response_*.json",
            sql_file="open_meteo_archive_hourly.sql",
            target="catalog1.silver.stg_weather_archive_hourly",
            batch_size=50,
            parameters={"weather_model": "era5"},
            base_dir=SOURCE_DIR,
        ),
        SourceConfig(
            name="open_meteo_ifs",
            dataset="weather_hourly_ifs",
            prefix="bronze/files/open_meteo/historical_weather_hourly/ifs",
            pattern="**/response_*.json",
            sql_file="open_meteo_archive_hourly.sql",
            target="catalog1.silver.stg_weather_archive_hourly",
            batch_size=50,
            parameters={"weather_model": "ecmwf_ifs"},
            base_dir=SOURCE_DIR,
        ),
    ),
}


def source_configs(group: str | None = None) -> tuple[SourceConfig, ...]:
    if group is not None:
        return SOURCE_GROUPS[group]
    return tuple(config for configs in SOURCE_GROUPS.values() for config in configs)


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
