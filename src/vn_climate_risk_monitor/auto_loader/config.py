"""Code-native Bronze source definitions for the auto-loader."""

from __future__ import annotations

from pathlib import Path

from vn_climate_risk_monitor.auto_loader.loader import SourceConfig

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
