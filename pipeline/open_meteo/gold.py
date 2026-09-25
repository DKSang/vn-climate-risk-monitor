"""silver.clean_ -> Gold forecast models, then publication for the dashboard."""

from __future__ import annotations

import json
from datetime import datetime

from pipeline import lake
from pipeline.dbt import dbt_build
from pipeline.job_run import job_run

FORECAST_ASSET = "gold.fct_rain_forecast_hourly"
ARCHIVE_ASSET = "gold.fct_rain_archive_hourly"


def build_gold_forecast() -> int:
    """Rebuild Gold for forecast runs changed since the watermark, then publish."""
    return build_gold(FORECAST_ASSET, selector="gold_forecast")


def build_gold_archive() -> int:
    """Rebuild the archive Gold fact in full, then publish."""
    return build_gold(ARCHIVE_ASSET, selector="gold_archive")


def build_gold(asset: str, *, selector: str) -> int:
    """`dbt build` a Gold selector from `asset`'s watermark, then publish.

    Nothing is published unless every model and dbt test in the build passes.
    Returns the rows of `asset` inserted or updated.
    """
    with job_run(asset) as run:
        watermark = run.watermark.isoformat() if run.watermark else None
        dbt_build(selector=selector, vars={"watermark": watermark})
        with lake.connect() as con:
            touched = con.execute(
                f"SELECT count(*) FROM {asset} WHERE _updated_at >= ?", [run.started_at]
            ).fetchone()[0]
        publish(asset, run.started_at)
        run.rows_out = touched
        return touched


def publish(asset: str, job_started_at: datetime) -> None:
    """Mark the current lake snapshot as the one the dashboard should read.

    The marker row and the commit message land in one DuckLake snapshot; the
    dashboard pins the latest snapshot whose commit message is 'publish'.
    """
    with lake.connect() as con:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS gold._publications (
                published_at TIMESTAMPTZ,
                asset VARCHAR,
                job_started_at TIMESTAMPTZ
            )
            """
        )
        # Lakes published before the archive existed have a forecast_run column
        # instead of asset: upgrade them in place (a no-op once done).
        con.execute(
            "ALTER TABLE gold._publications ADD COLUMN IF NOT EXISTS asset VARCHAR"
        )
        con.execute("ALTER TABLE gold._publications DROP COLUMN IF EXISTS forecast_run")
        con.execute("BEGIN")
        con.execute(
            "INSERT INTO gold._publications (published_at, asset, job_started_at) "
            "VALUES (now(), ?, ?)",
            [asset, job_started_at],
        )
        info = json.dumps({"asset": asset})
        con.execute(
            f"CALL {lake.CATALOG}.set_commit_message('airflow', 'publish', "
            f"extra_info => '{info}')"
        )
        con.execute("COMMIT")


def main_forecast() -> None:
    print(f"Built {build_gold_forecast()} Gold forecast row(s) and published")


def main_archive() -> None:
    print(f"Built {build_gold_archive()} Gold archive row(s) and published")
