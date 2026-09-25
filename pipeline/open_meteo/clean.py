"""silver.stg_ -> silver.clean_ tables with dbt (dedup, then merge)."""

from __future__ import annotations

from collections.abc import Callable

import pandas as pd

from pipeline import lake
from pipeline.dbt import dbt_build
from pipeline.job_run import job_run
from pipeline.open_meteo.fetch import grid_cells
from pipeline.quality import check_silver_archive, check_silver_forecast
from pipeline.settings import load_settings

FORECAST_ASSET = "silver.clean_weather_forecast_hourly"
ARCHIVE_ASSET = "silver.clean_weather_archive_hourly"


def build_clean_forecast() -> int:
    """Merge forecast rows staged since the watermark; return rows inserted or updated."""
    rows_per_run = len(grid_cells()) * load_settings().open_meteo.forecast_hours
    return build_clean(
        FORECAST_ASSET,
        period="forecast_run",
        check=lambda periods: check_silver_forecast(periods, rows_per_run=rows_per_run),
    )


def build_clean_archive() -> int:
    """Merge archive rows staged since the watermark; return rows inserted or updated."""
    cells = len(grid_cells())
    return build_clean(
        ARCHIVE_ASSET,
        period="date_trunc('month', valid_at AT TIME ZONE 'UTC')",
        check=lambda months: check_silver_archive(months, cells=cells),
    )


def build_clean(
    asset: str, *, period: str, check: Callable[[pd.DataFrame], None]
) -> int:
    """Build the dbt model behind `asset` from its watermark, then check it.

    `check` receives every row of each `period` (a SQL expression) the build
    touched, with that expression as a `period` column: a re-read file touches
    only part of a period, but volume is judged on the whole period.
    """
    with job_run(asset) as run:
        watermark = run.watermark.isoformat() if run.watermark else None
        dbt_build(asset.split(".")[1], vars={"watermark": watermark})
        with lake.connect() as con:
            touched = con.execute(
                f"SELECT count(*) FROM {asset} WHERE _updated_at >= ?", [run.started_at]
            ).fetchone()[0]
            periods = con.execute(
                f"""
                SELECT *, {period} AS period FROM {asset} WHERE {period} IN (
                    SELECT {period} FROM {asset} WHERE _updated_at >= ?
                )
                """,
                [run.started_at],
            ).df()
        check(periods)
        run.rows_out = touched
        return touched


def main_forecast() -> None:
    print(f"Merged {build_clean_forecast()} row(s) into {FORECAST_ASSET}")


def main_archive() -> None:
    print(f"Merged {build_clean_archive()} row(s) into {ARCHIVE_ASSET}")
