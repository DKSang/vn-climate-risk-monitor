"""Everything the dashboard reads, pinned to the latest published DuckLake snapshot.

A snapshot never changes, so query results are cached per snapshot for good.
"""

from __future__ import annotations

from datetime import date, datetime

import duckdb
import pandas as pd
import streamlit as st

from pipeline import lake

MODEL = "ecmwf_ifs"


def published_snapshot() -> int | None:
    """Id of the newest snapshot the pipeline marked 'publish', or None."""
    with lake.connect() as con:
        return con.execute(
            f"SELECT max(snapshot_id) FROM {lake.CATALOG}.snapshots() "
            "WHERE commit_message = 'publish'"
        ).fetchone()[0]


@st.cache_resource
def _connection(snapshot: int) -> duckdb.DuckDBPyConnection:
    return lake.connect(snapshot=snapshot)


def _query(snapshot: int, sql: str, params: list | None = None) -> pd.DataFrame:
    # One cursor per query (the cached connection is shared by sessions); a
    # cursor does not inherit the connection's USE, so set it again.
    cursor = _connection(snapshot).cursor()
    cursor.execute(f"USE {lake.CATALOG}")
    return cursor.execute(sql, params or []).df()


@st.cache_data
def ward_hour(snapshot: int, hour: datetime) -> pd.DataFrame:
    """Rain and rain pressure of every ward at one hour of the current forecast run."""
    return _query(
        snapshot,
        """
        SELECT
            w.ward_code, w.ward_name, w.ward_latitude, w.ward_longitude,
            f.precipitation_mm, f.forecast_next_1h_mm, f.forecast_next_3h_mm,
            f.forecast_next_6h_mm, f.forecast_next_24h_mm,
            p.pressure_level, p.pressure_score, p.trigger_reasons
        FROM gold.dim_ward AS w
        JOIN gold.bridge_ward_grid AS b
          ON b.ward_code = w.ward_code AND b.weather_model = ?
        JOIN gold.fct_rain_forecast_current_hourly AS f
          ON f.grid_cell_id = b.grid_cell_id AND f.valid_at = ?
        LEFT JOIN gold.fct_rain_pressure_alert AS p
          ON p.ward_code = w.ward_code
         AND p.forecast_run = f.forecast_run
         AND p.valid_at = f.valid_at
        ORDER BY w.ward_name
        """,
        [MODEL, hour],
    )


@st.cache_data
def overview(snapshot: int) -> dict:
    """The published forecast run, when it was published, and its remaining hours."""
    run = _query(
        snapshot,
        """
        SELECT
            (SELECT max(forecast_run) FROM gold.fct_rain_forecast_current_hourly)
                AS forecast_run,
            (SELECT max(published_at) FROM gold._publications) AS published_at
        """,
    ).iloc[0]
    hours = _query(
        snapshot,
        "SELECT DISTINCT valid_at FROM gold.fct_rain_forecast_current_hourly ORDER BY 1",
    )["valid_at"]
    return {
        "forecast_run": run["forecast_run"],
        "published_at": run["published_at"],
        "hours": list(hours),
    }


@st.cache_data
def timeseries(snapshot: int, ward_code: str | None = None) -> pd.DataFrame:
    """Hourly rain and pressure over the current run: one ward, or the ward average."""
    return _query(
        snapshot,
        """
        SELECT
            f.valid_at,
            avg(f.precipitation_mm) AS precipitation_mm,
            avg(f.forecast_next_24h_mm) AS forecast_next_24h_mm,
            avg(p.pressure_score) AS pressure_score,
            CASE WHEN count(DISTINCT b.ward_code) = 1
                 THEN any_value(p.pressure_level) END AS pressure_level
        FROM gold.fct_rain_forecast_current_hourly AS f
        JOIN gold.bridge_ward_grid AS b
          ON b.grid_cell_id = f.grid_cell_id AND b.weather_model = ?
        LEFT JOIN gold.fct_rain_pressure_alert AS p
          ON p.ward_code = b.ward_code
         AND p.forecast_run = f.forecast_run
         AND p.valid_at = f.valid_at
        WHERE ? IS NULL OR b.ward_code = ?
        GROUP BY f.valid_at
        ORDER BY f.valid_at
        """,
        [MODEL, ward_code, ward_code],
    )


@st.cache_data
def archive_months(snapshot: int) -> list[date]:
    """Months with archive data in the snapshot, newest first (empty before any)."""
    try:
        months = _query(
            snapshot,
            """
            SELECT DISTINCT CAST(date_trunc('month', rain_date) AS DATE) AS month
            FROM gold.fct_rain_archive_hourly ORDER BY month DESC
            """,
        )
    except duckdb.CatalogException:  # the archive has never been published
        return []
    return list(months["month"].dt.date)


@st.cache_data
def archive_month(snapshot: int, month: date) -> pd.DataFrame:
    """Hourly past rain of every ward over one calendar month (UTC)."""
    return _query(
        snapshot,
        """
        SELECT w.ward_code, w.ward_name, a.valid_at, a.precipitation_mm
        FROM gold.fct_rain_archive_hourly AS a
        JOIN gold.bridge_ward_grid AS b
          ON b.grid_cell_id = a.grid_cell_id AND b.weather_model = ?
        JOIN gold.dim_ward AS w ON w.ward_code = b.ward_code
        WHERE date_trunc('month', a.rain_date) = ?
        ORDER BY a.valid_at, w.ward_code
        """,
        [MODEL, month],
    )
