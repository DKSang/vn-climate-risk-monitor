"""Auto loader: new Bronze forecast files -> silver.stg_open_meteo_forecast (append-only)."""

from __future__ import annotations

from pipeline import lake
from pipeline.job_run import job_run
from pipeline.open_meteo.forecast import BRONZE_PREFIX, grid_cells
from pipeline.quality import check_bronze_forecast
from pipeline.settings import load_settings

ASSET = "silver.stg_open_meteo_forecast"

CREATE_STAGING = f"""
CREATE TABLE IF NOT EXISTS {ASSET} (
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
READ_BRONZE = """
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


def load_forecast() -> int:
    """Append every new Bronze forecast file to staging; return the rows added."""
    settings = load_settings()
    bucket = settings.minio.bucket
    rows_per_run = len(grid_cells()) * settings.open_meteo.forecast_hours
    with job_run(ASSET) as run:
        # Compare with landing time, not the run's date partition, so the whole
        # prefix is listed and a late-landed old slot is not missed. S3 keeps
        # LastModified to the second: re-reading the watermark's second may append
        # a file twice (dedup in silver.clean_ handles it); skipping it would lose it.
        cutoff = run.watermark and run.watermark.replace(microsecond=0)
        files = [
            f"s3://{bucket}/{obj.object_name}"
            for obj in lake.minio_client().list_objects(
                bucket, prefix=BRONZE_PREFIX, recursive=True
            )
            if cutoff is None or obj.last_modified >= cutoff
        ]
        if not files:
            run.rows_in = run.rows_out = 0
            return 0
        with lake.connect() as con:
            con.execute(CREATE_STAGING)
            rows = con.execute(READ_BRONZE, {"files": files}).df()
            run.rows_in = len(rows)
            check_bronze_forecast(rows, rows_per_run=rows_per_run)
            con.execute(f"INSERT INTO {ASSET} SELECT *, now() FROM rows")
        run.rows_out = len(rows)
        return len(rows)


def main() -> None:
    print(f"Appended {load_forecast()} row(s) to {ASSET}")
