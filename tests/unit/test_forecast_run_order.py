"""Behavioral tests for logical forecast-vintage ordering."""

from __future__ import annotations

from pathlib import Path

import duckdb
import jinja2

MACRO_FILE = Path("transform/macros/forecast.sql")


def test_late_reprocessing_does_not_make_an_old_forecast_current() -> None:
    environment = jinja2.Environment(autoescape=False)
    template = environment.from_string(MACRO_FILE.read_text(encoding="utf-8"))
    order_by = str(template.module.forecast_run_order())
    connection = duckdb.connect()
    try:
        connection.execute(
            """
            CREATE TABLE forecast_history (
                forecast_run_id VARCHAR,
                forecast_run_at TIMESTAMPTZ,
                _ingested_at TIMESTAMPTZ
            )
            """
        )
        connection.execute(
            """
            INSERT INTO forecast_history VALUES
                ('run_old', TIMESTAMPTZ '2026-09-15 00:00:00+00',
                            TIMESTAMPTZ '2026-09-15 08:00:00+00'),
                ('run_new', TIMESTAMPTZ '2026-09-15 06:00:00+00',
                            TIMESTAMPTZ '2026-09-15 06:05:00+00')
            """
        )

        selected = connection.execute(
            f"""
            SELECT forecast_run_id
            FROM forecast_history
            GROUP BY forecast_run_id
            ORDER BY {order_by}
            LIMIT 1
            """
        ).fetchone()[0]

        assert selected == "run_new"
    finally:
        connection.close()
