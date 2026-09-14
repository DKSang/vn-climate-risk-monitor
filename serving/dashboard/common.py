"""Shared read-only dashboard queries and snapshot resolution."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

import pandas as pd

from vn_climate_risk_monitor.config import load_settings
from vn_climate_risk_monitor.ingestion.state import connect_control_plane
from vn_climate_risk_monitor.lakehouse import get_connection
from vn_climate_risk_monitor.processing.config import PROCESS_CONFIGS

logger = logging.getLogger(__name__)

_PUBLICATION_PROCESS = {
    config.target.rsplit(".", 1)[-1]: process_key
    for process_key, config in PROCESS_CONFIGS.items()
}

_WEATHER_FACTS = {
    "forecast": "gold.fct_rain_forecast_current_hourly",
    "archive": "gold.fct_rain_archive_hourly",
}

RAIN_METRICS = frozenset(
    {
        "rain_1h_mm",
        "rain_12h_mm",
        "rain_24h_mm",
        "forecast_next_1h_mm",
        "forecast_next_3h_mm",
        "forecast_next_6h_mm",
        "forecast_next_12h_mm",
        "forecast_next_24h_mm",
    }
)


def _validate_rain_metric(metric: str) -> str:
    if metric not in RAIN_METRICS:
        raise ValueError(f"Rain metric không hợp lệ: {metric}")
    return metric


def load_serving_snapshot(
    table_schema: str = "gold",
    table_name: str = "fct_rain_forecast_hourly",
) -> dict[str, Any]:
    """Resolve the latest successfully published Gold snapshot."""
    process_key = _PUBLICATION_PROCESS.get(table_name)
    if process_key is None:
        raise ValueError(f"Không có publication process cho {table_name!r}")
    settings = load_settings()
    connection = connect_control_plane(settings.postgres.ducklake_connection_string)
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                WITH published_run AS (
                    SELECT published_snapshot_id AS snapshot_id
                    FROM processing.processing_runs
                    WHERE process_key = %s
                      AND scope = 'production'
                      AND status = 'SUCCEEDED'
                      AND published_snapshot_id IS NOT NULL
                    ORDER BY completed_at_utc DESC
                    LIMIT 1
                ),
                catalog_snapshot AS (
                    SELECT
                        snapshot.snapshot_id,
                        snapshot.snapshot_time
                    FROM ducklake.ducklake_snapshot AS snapshot
                    JOIN published_run USING (snapshot_id)
                ),
                target_schema AS (
                    SELECT schema.schema_id
                    FROM ducklake.ducklake_schema AS schema
                    CROSS JOIN catalog_snapshot
                    WHERE schema.schema_name = %s
                      AND schema.begin_snapshot <= catalog_snapshot.snapshot_id
                      AND (
                          schema.end_snapshot IS NULL
                          OR schema.end_snapshot > catalog_snapshot.snapshot_id
                      )
                    ORDER BY schema.begin_snapshot DESC
                    LIMIT 1
                ),
                target_table AS (
                    SELECT
                        tbl.table_id,
                        tbl.begin_snapshot
                    FROM ducklake.ducklake_table AS tbl
                    JOIN target_schema USING (schema_id)
                    CROSS JOIN catalog_snapshot
                    WHERE tbl.table_name = %s
                      AND tbl.begin_snapshot <= catalog_snapshot.snapshot_id
                      AND (
                          tbl.end_snapshot IS NULL
                          OR tbl.end_snapshot > catalog_snapshot.snapshot_id
                      )
                    ORDER BY tbl.begin_snapshot DESC
                    LIMIT 1
                ),
                tracked_table_snapshot AS (
                    SELECT
                        snapshot.snapshot_id,
                        snapshot.snapshot_time
                    FROM ducklake.ducklake_snapshot AS snapshot
                    JOIN ducklake.ducklake_snapshot_changes AS changes
                      USING (snapshot_id)
                    CROSS JOIN target_table
                    CROSS JOIN catalog_snapshot
                    WHERE changes.changes_made ~ (
                        '(^|,)(inserted_into_table|deleted_from_table|'
                        || 'compacted_table|altered_table):'
                        || target_table.table_id::text
                        || '(,|$)'
                    )
                      AND snapshot.snapshot_id <= catalog_snapshot.snapshot_id
                    ORDER BY snapshot.snapshot_id DESC
                    LIMIT 1
                ),
                table_snapshot AS (
                    SELECT
                        COALESCE(
                            tracked.snapshot_id,
                            created.snapshot_id,
                            target_table.begin_snapshot
                        )
                            AS snapshot_id,
                        COALESCE(tracked.snapshot_time, created.snapshot_time)
                            AS snapshot_time
                    FROM target_table
                    LEFT JOIN tracked_table_snapshot AS tracked ON TRUE
                    LEFT JOIN ducklake.ducklake_snapshot AS created
                      ON created.snapshot_id = target_table.begin_snapshot
                )
                SELECT
                    catalog_snapshot.snapshot_id,
                    catalog_snapshot.snapshot_time,
                    table_snapshot.snapshot_id AS table_snapshot_id,
                    table_snapshot.snapshot_time AS table_snapshot_time
                FROM catalog_snapshot
                CROSS JOIN table_snapshot
                """,
                (process_key, table_schema, table_name),
            )
            row = cursor.fetchone()
            if row is not None:
                columns = [column.name for column in cursor.description]
                return dict(zip(columns, row, strict=True))
        logger.info("Chưa có validated processing snapshot cho %s", table_name)
        return {}
    except Exception as exc:  # noqa: BLE001
        logger.warning("Không thể resolve validated DuckLake serving snapshot: %s", exc)
        return {}
    finally:
        connection.close()

def _read_dataframe(
    query: str,
    params: list[Any] | None = None,
    *,
    snapshot_version: int | None = None,
) -> pd.DataFrame:
    """Chỉ materialize DataFrame cho các bảng mà component UI cần render."""
    con = get_connection(read_only=True, snapshot_version=snapshot_version)
    try:
        return con.execute(query, params or []).df()
    finally:
        con.close()

def _read_record(
    query: str,
    params: list[Any] | None = None,
    *,
    snapshot_version: int | None = None,
) -> dict[str, Any]:
    """Lấy một record trực tiếp từ DuckDB, không đi vòng qua pandas."""
    con = get_connection(read_only=True, snapshot_version=snapshot_version)
    try:
        cursor = con.execute(query, params or [])
        row = cursor.fetchone()
        if row is None:
            return {}
        columns = [column[0] for column in cursor.description]
        return dict(zip(columns, row, strict=True))
    finally:
        con.close()

def _read_records(
    query: str,
    params: list[Any] | None = None,
    *,
    snapshot_version: int | None = None,
) -> list[dict[str, Any]]:
    """Lấy danh sách record trực tiếp từ DuckDB cho dữ liệu điều khiển nhỏ."""
    con = get_connection(read_only=True, snapshot_version=snapshot_version)
    try:
        cursor = con.execute(query, params or [])
        columns = [column[0] for column in cursor.description]
        return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]
    finally:
        con.close()

def load_all_wards(
    snapshot_version: int | None = None,
) -> list[dict[str, Any]]:
    """Danh sách 126 phường/xã hiện hành cho selector và bản đồ."""
    try:
        return _read_records(
            """
            SELECT ward_code, ward_name, ward_latitude, ward_longitude
            FROM gold.dim_ward
            WHERE is_active = TRUE
            ORDER BY ward_name
            """,
            snapshot_version=snapshot_version,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Không thể đọc danh sách phường: %s", exc)
        return []

def load_flood_points(
    ward_code: str | None = None,
    snapshot_version: int | None = None,
) -> pd.DataFrame:
    """Các điểm đang hoạt động trong danh mục nguồn, tùy chọn theo phường."""
    params: list[Any] = []
    ward_filter = ""
    if ward_code:
        ward_filter = "AND ward_code = $1"
        params.append(ward_code)
    try:
        return _read_dataframe(
            f"""
            SELECT
                point_id,
                point_name,
                ward_name,
                ward_code,
                rain_scenario,
                typical_depth_cm,
                latitude,
                longitude,
                status
            FROM gold.dim_flood_point
            WHERE is_active = TRUE
              AND status = 'active'
              {ward_filter}
            ORDER BY point_id
            """,
            params,
            snapshot_version=snapshot_version,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Không thể đọc điểm ngập: %s", exc)
        return pd.DataFrame()


def load_weather_flood_points_for_hour(
    process_key: str,
    weather_model: str,
    valid_time_utc: datetime | str,
    snapshot_version: int | None = None,
) -> pd.DataFrame:
    """Compare Gold rain levels with each flood point's required Gold level."""
    try:
        fact = _WEATHER_FACTS[process_key]
    except KeyError as error:
        raise ValueError(f"Unknown weather process: {process_key}") from error
    try:
        return _read_dataframe(
            f"""
            SELECT
                p.point_id,
                p.point_name,
                p.ward_name,
                p.ward_code,
                p.rain_scenario,
                p.latitude,
                p.longitude,
                f.rain_1h_mm,
                f.hanoi_rain_scenario_band,
                COALESCE(
                    f.hanoi_rain_scenario_level
                        >= p.required_rain_scenario_level,
                    FALSE
                ) AS is_triggered
            FROM gold.dim_flood_point AS p
            LEFT JOIN gold.bridge_ward_grid AS bwg
              ON bwg.ward_code = p.ward_code
             AND bwg.weather_model = $1
             AND bwg.is_active = TRUE
            LEFT JOIN {fact} AS f
              ON f.grid_cell_id = bwg.grid_cell_id
             AND f.valid_time_utc = $2
            WHERE p.is_active = TRUE AND p.status = 'active'
            ORDER BY is_triggered DESC, p.point_id
            """,
            [weather_model, valid_time_utc],
            snapshot_version=snapshot_version,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Không thể đối chiếu điểm ngập tại %s: %s", valid_time_utc, exc)
        return pd.DataFrame()
