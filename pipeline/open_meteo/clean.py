"""silver.stg_open_meteo_forecast -> silver.clean_weather_forecast_hourly (dbt merge)."""

from __future__ import annotations

from pipeline import lake
from pipeline.dbt import dbt_build
from pipeline.job_run import job_run
from pipeline.open_meteo.forecast import grid_cells
from pipeline.quality import check_silver_forecast
from pipeline.settings import load_settings

ASSET = "silver.clean_weather_forecast_hourly"


def build_clean_forecast() -> int:
    """Merge rows staged since the watermark; return the rows inserted or updated."""
    with job_run(ASSET) as run:
        watermark = run.watermark.isoformat() if run.watermark else None
        dbt_build("clean_weather_forecast_hourly", vars={"watermark": watermark})
        with lake.connect() as con:
            touched = con.execute(
                f"SELECT count(*) FROM {ASSET} WHERE _updated_at >= ?", [run.started_at]
            ).fetchone()[0]
            # Every row of each touched run: a re-read file touches only part of one.
            runs = con.execute(
                f"""
                SELECT * FROM {ASSET} WHERE forecast_run IN (
                    SELECT forecast_run FROM {ASSET} WHERE _updated_at >= ?
                )
                """,
                [run.started_at],
            ).df()
        rows_per_run = len(grid_cells()) * load_settings().open_meteo.forecast_hours
        check_silver_forecast(runs, rows_per_run=rows_per_run)
        run.rows_out = touched
        return touched


def main() -> None:
    print(f"Merged {build_clean_forecast()} row(s) into {ASSET}")
