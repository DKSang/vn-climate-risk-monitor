"""silver.clean_ -> Gold forecast models, then publication for the dashboard."""

from __future__ import annotations

import json
from datetime import datetime

from pipeline import lake
from pipeline.dbt import dbt_build
from pipeline.job_run import job_run

ASSET = "gold.fct_rain_forecast_hourly"


def build_gold_forecast() -> int:
    """Rebuild Gold for forecast runs changed since the watermark, then publish.

    Nothing is published unless every model and dbt test in the build passes.
    Returns the history-fact rows inserted or updated.
    """
    with job_run(ASSET) as run:
        watermark = run.watermark.isoformat() if run.watermark else None
        dbt_build(selector="gold_forecast", vars={"watermark": watermark})
        with lake.connect() as con:
            touched = con.execute(
                f"SELECT count(*) FROM {ASSET} WHERE _updated_at >= ?", [run.started_at]
            ).fetchone()[0]
        publish(run.started_at)
        run.rows_out = touched
        return touched


def publish(job_started_at: datetime) -> None:
    """Mark the current lake snapshot as the one the dashboard should read.

    The marker row and the commit message land in one DuckLake snapshot; the
    dashboard pins the latest snapshot whose commit message is 'publish'.
    """
    with lake.connect() as con:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS gold._publications (
                published_at TIMESTAMPTZ,
                forecast_run TIMESTAMPTZ,
                job_started_at TIMESTAMPTZ
            )
            """
        )
        con.execute("BEGIN")
        latest_run = con.execute(f"SELECT max(forecast_run) FROM {ASSET}").fetchone()[0]
        con.execute(
            "INSERT INTO gold._publications VALUES (now(), ?, ?)",
            [latest_run, job_started_at],
        )
        info = json.dumps({"forecast_run": str(latest_run)})
        con.execute(
            f"CALL {lake.CATALOG}.set_commit_message('airflow', 'publish', "
            f"extra_info => '{info}')"
        )
        con.execute("COMMIT")


def main() -> None:
    print(f"Built {build_gold_forecast()} Gold forecast row(s) and published")
