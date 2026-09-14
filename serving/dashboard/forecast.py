"""Forecast Gold queries for the Streamlit map and ward drill-down pages."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

import pandas as pd

from .common import (
    _read_dataframe,
    _read_record,
    _read_records,
    _validate_rain_metric,
)

logger = logging.getLogger(__name__)

FORECAST_MODEL = "ecmwf_ifs_fc"

_CURRENT_HORIZON_CTE = """
    WITH current_horizon AS (
        SELECT
            MIN(valid_time_utc) AS starts_at_utc,
            MAX(valid_time_utc) AS ends_at_utc,
            MAX(_ingested_at) AS updated_at_utc
        FROM gold.fct_rain_forecast_current_hourly
        WHERE valid_time_utc >= DATE_TRUNC('hour', CURRENT_TIMESTAMP)
    )
"""

def load_forecast_metadata(
    snapshot_version: int | None = None,
) -> dict[str, Any]:
    """Metadata của forecast horizon đang được phục vụ."""
    try:
        return _read_record(
            _CURRENT_HORIZON_CTE
            + """
            SELECT
                h.starts_at_utc,
                h.ends_at_utc,
                h.updated_at_utc,
                DATE_DIFF('hour', h.starts_at_utc, h.ends_at_utc) + 1
                    AS horizon_hours,
                DATE_DIFF('minute', h.updated_at_utc, CURRENT_TIMESTAMP)
                    AS freshness_minutes,
                COUNT(DISTINCT f.grid_cell_id) AS grid_count,
                COUNT(DISTINCT b.ward_code) AS ward_count,
                (
                    SELECT COUNT(*)
                    FROM gold.dim_flood_point p
                    WHERE p.is_active = TRUE AND p.status = 'active'
                ) AS flood_point_count
            FROM current_horizon h
            LEFT JOIN gold.fct_rain_forecast_current_hourly f
                ON f.valid_time_utc BETWEEN h.starts_at_utc AND h.ends_at_utc
            LEFT JOIN gold.bridge_ward_grid b
                ON b.grid_cell_id = f.grid_cell_id
               AND b.weather_model = $1
               AND b.is_active = TRUE
            GROUP BY 1, 2, 3, 4, 5
            """,
            [FORECAST_MODEL],
            snapshot_version=snapshot_version,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Không thể đọc metadata forecast: %s", exc)
        return {}

def load_forecast_hours(snapshot_version: int | None = None) -> list[datetime]:
    """Các mốc tương lai thuộc trạng thái hiện hành của table đã pin snapshot."""
    try:
        rows = _read_records(
            _CURRENT_HORIZON_CTE
            + """
            SELECT DISTINCT f.valid_time_utc
            FROM gold.fct_rain_forecast_current_hourly f
            CROSS JOIN current_horizon h
            WHERE f.valid_time_utc BETWEEN h.starts_at_utc AND h.ends_at_utc
            ORDER BY f.valid_time_utc
            """,
            snapshot_version=snapshot_version,
        )
        return [row["valid_time_utc"] for row in rows]
    except Exception as exc:  # noqa: BLE001
        logger.warning("Không thể đọc forecast hours: %s", exc)
        return []

def load_forecast_by_hour(
    valid_time_utc: datetime | str,
    snapshot_version: int | None = None,
    *,
    sort_metric: str = "rain_24h_mm",
) -> pd.DataFrame:
    """Mưa và kịch bản của 126 phường tại một mốc giờ trong horizon hiện hành."""
    _validate_rain_metric(sort_metric)
    try:
        return _read_dataframe(
            """
            SELECT
                w.ward_code,
                w.ward_name,
                w.ward_latitude,
                w.ward_longitude,
                f.grid_cell_id,
                f.precipitation_mm,
                f.precipitation_probability_pct,
                f.rain_1h_mm,
                f.rain_3h_mm,
                f.rain_6h_mm,
                f.rain_12h_mm,
                f.rain_24h_mm,
                f.forecast_next_1h_mm,
                f.forecast_next_3h_mm,
                f.forecast_next_6h_mm,
                f.forecast_next_12h_mm,
                f.forecast_next_24h_mm,
                f.forecast_next_1h_band,
                f.forecast_next_12h_band,
                f.forecast_next_24h_band,
                f.hanoi_rain_scenario_band,
                f.hanoi_rain_scenario_level,
                f.vn_rain_band_12h,
                f.vn_rain_band_24h,
                pressure.pressure_level,
                pressure.pressure_score,
                pressure.coverage_status,
                pressure.trigger_reasons,
                pressure.persistence_runs,
                pressure.revision_24h_mm,
                pressure.revision_direction,
                f.valid_time_utc
            FROM gold.fct_rain_forecast_current_hourly f
            JOIN gold.bridge_ward_grid bwg
              ON bwg.grid_cell_id = f.grid_cell_id
             AND bwg.weather_model = $1
             AND bwg.is_active = TRUE
            JOIN gold.dim_ward w
              ON w.ward_code = bwg.ward_code
             AND w.is_active = TRUE
            LEFT JOIN gold.fct_rain_pressure_alert pressure
              ON pressure.forecast_run_id = f.forecast_run_id
             AND pressure.ward_code = w.ward_code
             AND pressure.valid_time_utc = f.valid_time_utc
            WHERE f.valid_time_utc = $2
            ORDER BY
                CASE $3
                    WHEN 'rain_12h_mm' THEN f.rain_12h_mm
                    WHEN 'rain_24h_mm' THEN f.rain_24h_mm
                    WHEN 'forecast_next_1h_mm' THEN f.forecast_next_1h_mm
                    WHEN 'forecast_next_3h_mm' THEN f.forecast_next_3h_mm
                    WHEN 'forecast_next_6h_mm' THEN f.forecast_next_6h_mm
                    WHEN 'forecast_next_12h_mm' THEN f.forecast_next_12h_mm
                    WHEN 'forecast_next_24h_mm' THEN f.forecast_next_24h_mm
                    ELSE f.rain_1h_mm
                END DESC NULLS LAST,
                w.ward_name
            """,
            [FORECAST_MODEL, valid_time_utc, sort_metric],
            snapshot_version=snapshot_version,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Không thể đọc forecast tại %s: %s", valid_time_utc, exc)
        return pd.DataFrame()

def load_forecast_hour_summary(
    valid_time_utc: datetime | str,
    snapshot_version: int | None = None,
) -> dict[str, Any]:
    """KPI tổng hợp tại một giờ; mọi phép tính chạy trong DuckDB."""
    try:
        return _read_record(
            """
            WITH ward_forecast AS (
                SELECT
                    bwg.ward_code,
                    f.grid_cell_id,
                    f.rain_1h_mm,
                    f.rain_6h_mm,
                    f.rain_12h_mm,
                    f.rain_24h_mm,
                    f.forecast_next_1h_mm,
                    f.forecast_next_6h_mm,
                    f.forecast_next_12h_mm,
                    f.forecast_next_24h_mm,
                    f.forecast_next_1h_band,
                    f.forecast_next_12h_band,
                    f.forecast_next_24h_band,
                    f.hanoi_rain_scenario_band,
                    f.hanoi_rain_scenario_level,
                    f.vn_rain_band_12h,
                    f.vn_rain_band_24h,
                    pressure.pressure_level,
                    pressure.pressure_score,
                    pressure.coverage_status
                FROM gold.fct_rain_forecast_current_hourly f
                JOIN gold.bridge_ward_grid bwg
                  ON bwg.grid_cell_id = f.grid_cell_id
                 AND bwg.weather_model = $1
                 AND bwg.is_active = TRUE
                LEFT JOIN gold.fct_rain_pressure_alert pressure
                  ON pressure.forecast_run_id = f.forecast_run_id
                 AND pressure.ward_code = bwg.ward_code
                 AND pressure.valid_time_utc = f.valid_time_utc
                WHERE f.valid_time_utc = $2
            ),
            point_status AS (
                SELECT
                    p.point_id,
                    COALESCE(
                        wf.hanoi_rain_scenario_level
                            >= p.required_rain_scenario_level,
                        FALSE
                    ) AS is_triggered
                FROM gold.dim_flood_point p
                LEFT JOIN ward_forecast wf USING (ward_code)
                WHERE p.is_active = TRUE AND p.status = 'active'
            )
            SELECT
                COUNT(DISTINCT ward_code) AS ward_count,
                COUNT(DISTINCT grid_cell_id) AS grid_count,
                MAX(rain_1h_mm) AS max_rain_1h_mm,
                MAX(rain_6h_mm) AS max_rain_6h_mm,
                MAX(rain_12h_mm) AS max_rain_12h_mm,
                MAX(rain_24h_mm) AS max_rain_24h_mm,
                COUNT(*) FILTER (
                    WHERE hanoi_rain_scenario_band <> 'below_50'
                ) AS elevated_ward_count,
                COUNT(*) FILTER (
                    WHERE vn_rain_band_12h <> 'below_30'
                ) AS elevated_ward_count_12h,
                COUNT(*) FILTER (
                    WHERE vn_rain_band_24h <> 'below_50'
                ) AS elevated_ward_count_24h,
                COUNT(*) FILTER (
                    WHERE forecast_next_1h_band <> 'below_50'
                ) AS forecast_elevated_ward_count_1h,
                COUNT(*) FILTER (
                    WHERE forecast_next_12h_band <> 'below_30'
                ) AS forecast_elevated_ward_count_12h,
                COUNT(*) FILTER (
                    WHERE forecast_next_24h_band <> 'below_50'
                ) AS forecast_elevated_ward_count_24h,
                COUNT(*) FILTER (
                    WHERE pressure_level IN ('ELEVATED', 'HIGH')
                ) AS pressure_alert_ward_count,
                COUNT(*) FILTER (
                    WHERE pressure_level = 'HIGH'
                ) AS pressure_high_ward_count,
                COUNT(*) FILTER (
                    WHERE pressure_level = 'UNKNOWN'
                ) AS pressure_unknown_ward_count,
                MAX(pressure_score) AS max_pressure_score,
                (SELECT COUNT(*) FROM point_status WHERE is_triggered)
                    AS triggered_point_count
            FROM ward_forecast
            """,
            [FORECAST_MODEL, valid_time_utc],
            snapshot_version=snapshot_version,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Không thể tổng hợp forecast tại %s: %s", valid_time_utc, exc)
        return {}

def load_forecast_pressure_ranking(
    valid_time_utc: datetime | str,
    snapshot_version: int | None = None,
    limit: int = 10,
) -> pd.DataFrame:
    """Xếp hạng áp lực mưa theo tín hiệu rule-based; không phải cảnh báo chính thức."""
    try:
        return _read_dataframe(
            """
            SELECT
                alert.ward_code,
                ward.ward_name,
                alert.pressure_level,
                alert.pressure_score,
                alert.coverage_status,
                alert.trigger_reasons,
                alert.forecast_next_6h_mm,
                alert.forecast_next_24h_mm,
                alert.persistence_runs,
                alert.revision_direction,
                alert.revision_24h_mm
            FROM gold.fct_rain_pressure_alert alert
            JOIN gold.dim_ward ward
              ON ward.ward_code = alert.ward_code AND ward.is_active = TRUE
            WHERE alert.valid_time_utc = $1
            ORDER BY
                CASE alert.pressure_level
                    WHEN 'HIGH' THEN 1
                    WHEN 'ELEVATED' THEN 2
                    WHEN 'WATCH' THEN 3
                    WHEN 'NORMAL' THEN 4
                    ELSE 5
                END,
                alert.pressure_score DESC NULLS LAST,
                ward.ward_name
            LIMIT $2
            """,
            [valid_time_utc, limit],
            snapshot_version=snapshot_version,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Không thể xếp hạng áp lực mưa tại %s: %s", valid_time_utc, exc)
        return pd.DataFrame()

def load_ward_forecast_timeseries(
    ward_code: str,
    snapshot_version: int | None = None,
) -> pd.DataFrame:
    """Chuỗi dự báo hiện hành cho một phường, không lẫn các horizon cũ."""
    try:
        return _read_dataframe(
            _CURRENT_HORIZON_CTE
            + """
            SELECT
                f.valid_time_utc,
                f.precipitation_mm,
                f.precipitation_probability_pct,
                f.rain_1h_mm,
                f.rain_3h_mm,
                f.rain_6h_mm,
                f.rain_12h_mm,
                f.rain_24h_mm,
                f.forecast_next_1h_mm,
                f.forecast_next_3h_mm,
                f.forecast_next_6h_mm,
                f.forecast_next_12h_mm,
                f.forecast_next_24h_mm,
                f.hanoi_rain_scenario_band
            FROM gold.fct_rain_forecast_current_hourly f
            JOIN gold.bridge_ward_grid bwg
              ON bwg.grid_cell_id = f.grid_cell_id
             AND bwg.weather_model = $1
             AND bwg.is_active = TRUE
            CROSS JOIN current_horizon h
            WHERE bwg.ward_code = $2
              AND f.valid_time_utc BETWEEN h.starts_at_utc AND h.ends_at_utc
            ORDER BY f.valid_time_utc
            """,
            [FORECAST_MODEL, ward_code],
            snapshot_version=snapshot_version,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Không thể đọc forecast phường %s: %s", ward_code, exc)
        return pd.DataFrame()

def load_ward_forecast_summary(
    ward_code: str,
    snapshot_version: int | None = None,
) -> dict[str, Any]:
    """KPI horizon cho một phường, tính hoàn toàn trong DuckDB."""
    try:
        return _read_record(
            _CURRENT_HORIZON_CTE
            + """
            , series AS (
                SELECT
                    f.*,
                    ROW_NUMBER() OVER (ORDER BY f.valid_time_utc) AS hour_number
                FROM gold.fct_rain_forecast_current_hourly f
                JOIN gold.bridge_ward_grid bwg
                  ON bwg.grid_cell_id = f.grid_cell_id
                 AND bwg.weather_model = $1
                 AND bwg.is_active = TRUE
                CROSS JOIN current_horizon h
                WHERE bwg.ward_code = $2
                  AND f.valid_time_utc BETWEEN h.starts_at_utc AND h.ends_at_utc
            )
            , aggregated AS (
                SELECT
                    COUNT(*) AS available_hours,
                    SUM(precipitation_mm) FILTER (WHERE hour_number > 1)
                        AS horizon_rain_mm,
                    MAX(forecast_next_24h_mm) FILTER (WHERE hour_number = 1)
                        AS next_24h_rain_mm,
                    MAX(forecast_next_6h_mm) FILTER (WHERE hour_number = 1)
                        AS forecast_next_6h_mm,
                    MAX(forecast_next_24h_mm) FILTER (WHERE hour_number = 1)
                        AS forecast_next_24h_mm,
                    MAX(rain_1h_mm) FILTER (WHERE hour_number > 1) AS peak_1h_mm,
                    MAX(hanoi_rain_scenario_level) FILTER (WHERE hour_number > 1)
                        AS peak_rain_scenario_level,
                    ARG_MAX(valid_time_utc, rain_1h_mm) FILTER (WHERE hour_number > 1)
                        AS peak_time_utc,
                    MAX(rain_6h_mm) FILTER (WHERE hour_number > 1) AS peak_6h_mm,
                    MAX(precipitation_probability_pct) FILTER (WHERE hour_number > 1)
                        AS max_probability_pct
                FROM series
            )
            SELECT
                aggregated.*,
                (
                    SELECT COUNT(*)
                    FROM gold.dim_flood_point p
                    WHERE p.ward_code = $2
                      AND p.is_active = TRUE
                      AND p.status = 'active'
                      AND aggregated.peak_rain_scenario_level
                            >= p.required_rain_scenario_level
                ) AS triggered_point_count
                , (
                    SELECT a.pressure_level
                    FROM gold.fct_rain_pressure_alert a
                    CROSS JOIN current_horizon h
                    WHERE a.ward_code = $2
                      AND a.valid_time_utc BETWEEN h.starts_at_utc AND h.ends_at_utc
                    ORDER BY a.valid_time_utc
                    LIMIT 1
                ) AS pressure_level
                , (
                    SELECT a.pressure_score
                    FROM gold.fct_rain_pressure_alert a
                    CROSS JOIN current_horizon h
                    WHERE a.ward_code = $2
                      AND a.valid_time_utc BETWEEN h.starts_at_utc AND h.ends_at_utc
                    ORDER BY a.valid_time_utc
                    LIMIT 1
                ) AS pressure_score
                , (
                    SELECT a.trigger_reasons
                    FROM gold.fct_rain_pressure_alert a
                    CROSS JOIN current_horizon h
                    WHERE a.ward_code = $2
                      AND a.valid_time_utc BETWEEN h.starts_at_utc AND h.ends_at_utc
                    ORDER BY a.valid_time_utc
                    LIMIT 1
                ) AS pressure_trigger_reasons
            FROM aggregated
            """,
            [FORECAST_MODEL, ward_code],
            snapshot_version=snapshot_version,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Không thể tổng hợp forecast phường %s: %s", ward_code, exc)
        return {}

def load_ward_flood_context(
    ward_code: str,
    snapshot_version: int | None = None,
) -> pd.DataFrame:
    """Điểm ngập của phường và việc ngưỡng có bị vượt trong horizon hay không."""
    try:
        return _read_dataframe(
            _CURRENT_HORIZON_CTE
            + """
            , peak AS (
                SELECT
                    MAX(f.rain_1h_mm) AS peak_1h_mm,
                    MAX(f.hanoi_rain_scenario_level) AS peak_rain_scenario_level
                FROM gold.fct_rain_forecast_current_hourly f
                JOIN gold.bridge_ward_grid bwg
                  ON bwg.grid_cell_id = f.grid_cell_id
                 AND bwg.weather_model = $1
                 AND bwg.is_active = TRUE
                CROSS JOIN current_horizon h
                WHERE bwg.ward_code = $2
                  AND f.valid_time_utc BETWEEN h.starts_at_utc AND h.ends_at_utc
            )
            SELECT
                p.point_id,
                p.point_name,
                p.rain_scenario,
                p.latitude,
                p.longitude,
                peak.peak_1h_mm,
                COALESCE(
                    peak.peak_rain_scenario_level
                        >= p.required_rain_scenario_level,
                    FALSE
                ) AS threshold_reached
            FROM gold.dim_flood_point p
            CROSS JOIN peak
            WHERE p.ward_code = $2
              AND p.is_active = TRUE
              AND p.status = 'active'
            ORDER BY threshold_reached DESC, p.point_id
            """,
            [FORECAST_MODEL, ward_code],
            snapshot_version=snapshot_version,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Không thể đọc ngữ cảnh điểm ngập phường %s: %s", ward_code, exc)
        return pd.DataFrame()
