"""One job run of one asset, using the start-time watermark pattern (docs/adr/0001).

    with job_run("silver.clean_weather_forecast_hourly") as run:
        process(rows_newer_than=run.watermark)

On success the asset's watermark becomes `run.started_at`; on failure it is left
untouched, so the next run reads the same rows again.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import psycopg

from pipeline.settings import load_settings

META_SQL = Path(__file__).with_name("meta.sql")


@dataclass
class JobRun:
    asset: str
    started_at: datetime
    watermark: datetime | None
    rows_in: int | None = None
    rows_out: int | None = None


def create_meta_tables() -> None:
    with _connect() as conn:
        conn.execute(META_SQL.read_text(encoding="utf-8"))


@contextmanager
def job_run(asset: str) -> Iterator[JobRun]:
    with _connect() as conn:
        row = conn.execute(
            "SELECT watermark FROM meta.watermarks WHERE asset = %s", (asset,)
        ).fetchone()
        run = JobRun(asset, datetime.now(UTC), row[0] if row else None)
        (run_id,) = conn.execute(
            """
            INSERT INTO meta.job_runs (asset, status, started_at, watermark)
            VALUES (%s, 'running', %s, %s) RETURNING id
            """,
            (asset, run.started_at, run.watermark),
        ).fetchone()
        try:
            yield run
        except BaseException as error:
            conn.execute(
                """
                UPDATE meta.job_runs
                SET status = 'failed', finished_at = now(), error = %s,
                    rows_in = %s, rows_out = %s
                WHERE id = %s
                """,
                (str(error), run.rows_in, run.rows_out, run_id),
            )
            raise
        with conn.transaction():
            conn.execute(
                """
                UPDATE meta.job_runs
                SET status = 'success', finished_at = now(),
                    rows_in = %s, rows_out = %s
                WHERE id = %s
                """,
                (run.rows_in, run.rows_out, run_id),
            )
            conn.execute(
                """
                INSERT INTO meta.watermarks (asset, watermark) VALUES (%s, %s)
                ON CONFLICT (asset) DO UPDATE
                SET watermark = EXCLUDED.watermark, updated_at = now()
                """,
                (asset, run.started_at),
            )


def _connect() -> psycopg.Connection:
    return psycopg.connect(load_settings().postgres.dsn, autocommit=True)
