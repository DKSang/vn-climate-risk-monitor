"""Auto loader: new Bronze files -> append-only silver.stg_ tables."""

from __future__ import annotations

from collections.abc import Callable

import pandas as pd

from pipeline import lake
from pipeline.job_run import job_run
from pipeline.open_meteo.fetch import ARCHIVE_PREFIX, FORECAST_PREFIX, grid_cells
from pipeline.quality import check_bronze_archive, check_bronze_forecast
from pipeline.settings import load_settings

FORECAST_ASSET = "silver.stg_open_meteo_forecast"

FORECAST_STAGING = f"""
CREATE TABLE IF NOT EXISTS {FORECAST_ASSET} (
    forecast_run              TIMESTAMPTZ,
    grid_latitude             DOUBLE,
    grid_longitude            DOUBLE,
    valid_at                  TIMESTAMPTZ,
    precipitation             DOUBLE,
    rain                      DOUBLE,
    showers                   DOUBLE,
    precipitation_probability DOUBLE,
    weather_code              DOUBLE,
    _source_file              VARCHAR,
    _inserted_at              TIMESTAMPTZ
)
"""

# One row per forecast run x grid cell x hour. The run comes from the Hive
# partition in the object path: this is the only place it is parsed.
READ_FORECAST = """
SELECT
    strptime(forecast_run || '+0000', '%Y%m%dT%H%z') AS forecast_run,
    latitude AS grid_latitude,
    longitude AS grid_longitude,
    to_timestamp(t) AS valid_at,
    precipitation, rain, showers, precipitation_probability, weather_code,
    filename AS _source_file
FROM (
    SELECT
        forecast_run, latitude, longitude, filename,
        unnest(hourly.time) AS t,
        unnest(hourly.precipitation) AS precipitation,
        unnest(hourly.rain) AS rain,
        unnest(hourly.showers) AS showers,
        unnest(hourly.precipitation_probability) AS precipitation_probability,
        unnest(hourly.weather_code) AS weather_code
    FROM read_json(
        $files, format = 'array', filename = true,
        hive_partitioning = true, hive_types = {'forecast_run': VARCHAR}
    )
)
"""


ARCHIVE_ASSET = "silver.stg_open_meteo_archive"

ARCHIVE_STAGING = f"""
CREATE TABLE IF NOT EXISTS {ARCHIVE_ASSET} (
    grid_latitude  DOUBLE,
    grid_longitude DOUBLE,
    valid_at       TIMESTAMPTZ,
    precipitation  DOUBLE,
    rain           DOUBLE,
    weather_code   DOUBLE,
    _source_file   VARCHAR,
    _inserted_at   TIMESTAMPTZ
)
"""

# One row per grid cell x hour.
READ_ARCHIVE = """
SELECT
    latitude AS grid_latitude,
    longitude AS grid_longitude,
    to_timestamp(t) AS valid_at,
    precipitation, rain, weather_code,
    filename AS _source_file
FROM (
    SELECT
        latitude, longitude, filename,
        unnest(hourly.time) AS t,
        unnest(hourly.precipitation) AS precipitation,
        unnest(hourly.rain) AS rain,
        unnest(hourly.weather_code) AS weather_code
    FROM read_json($files, format = 'array', filename = true)
)
"""


def load_forecast() -> int:
    """Append every new Bronze forecast file to staging; return the rows added."""
    settings = load_settings()
    rows_per_run = len(grid_cells()) * settings.open_meteo.forecast_hours
    return append_new_files(
        FORECAST_ASSET,
        FORECAST_PREFIX,
        FORECAST_STAGING,
        READ_FORECAST,
        lambda rows: check_bronze_forecast(rows, rows_per_run=rows_per_run),
    )


def load_archive() -> int:
    """Append every new Bronze archive file to staging; return the rows added."""
    cells = len(grid_cells())
    return append_new_files(
        ARCHIVE_ASSET,
        ARCHIVE_PREFIX,
        ARCHIVE_STAGING,
        READ_ARCHIVE,
        lambda rows: check_bronze_archive(rows, cells=cells),
    )


def append_new_files(
    asset: str,
    prefix: str,
    create_sql: str,
    read_sql: str,
    check: Callable[[pd.DataFrame], None],
) -> int:
    """Read Bronze files landed since `asset`'s watermark, check them, append them.

    `read_sql` turns the `$files` list into staging rows; `check` raises to
    reject the whole batch (nothing is appended, the watermark stays).
    """
    bucket = load_settings().minio.bucket
    with job_run(asset) as run:
        # Compare with landing time, not the date partitions, so the whole
        # prefix is listed and a late-landed old period is not missed. S3 keeps
        # LastModified to the second: re-reading the watermark's second may append
        # a file twice (dedup in silver.clean_ handles it); skipping it would lose it.
        cutoff = run.watermark and run.watermark.replace(microsecond=0)
        files = [
            f"s3://{bucket}/{obj.object_name}"
            for obj in lake.minio_client().list_objects(
                bucket, prefix=prefix, recursive=True
            )
            if cutoff is None or obj.last_modified >= cutoff
        ]
        if not files:
            run.rows_in = run.rows_out = 0
            return 0
        with lake.connect() as con:
            con.execute(create_sql)
            rows = con.execute(read_sql, {"files": files}).df()
            run.rows_in = len(rows)
            check(rows)
            con.register("new_rows", rows)
            con.execute(f"INSERT INTO {asset} SELECT *, now() FROM new_rows")
        run.rows_out = len(rows)
        return len(rows)


def main_forecast() -> None:
    print(f"Appended {load_forecast()} row(s) to {FORECAST_ASSET}")


def main_archive() -> None:
    print(f"Appended {load_archive()} row(s) to {ARCHIVE_ASSET}")
