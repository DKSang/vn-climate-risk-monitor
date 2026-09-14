"""Archive Gold queries for the Streamlit historical replay page."""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any

import pandas as pd

from .common import (
    _read_dataframe,
    _read_record,
    _read_records,
    _validate_rain_metric,
)

logger = logging.getLogger(__name__)

def load_archive_models(
    snapshot_version: int | None = None,
) -> list[dict[str, Any]]:
    """Phạm vi và cực trị của từng nguồn weather archive."""
    try:
        return _read_records(
            """
            SELECT
                d.weather_model,
                COUNT(*) AS row_count,
                COUNT(DISTINCT f.grid_cell_id) AS grid_count,
                MIN(f.valid_time_utc) AS starts_at_utc,
                MAX(f.valid_time_utc) AS ends_at_utc,
                MAX(f._updated_at) AS updated_at_utc,
                MAX(f.rain_1h_mm) AS max_rain_1h_mm,
                MAX(f.rain_6h_mm) AS max_rain_6h_mm,
                MAX(f.rain_24h_mm) AS max_rain_24h_mm
            FROM gold.fct_rain_archive_hourly AS f
            JOIN gold.dim_grid AS d USING (grid_cell_id)
            GROUP BY d.weather_model
            ORDER BY d.weather_model
            """,
            snapshot_version=snapshot_version,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Không thể đọc metadata archive: %s", exc)
        return []

def load_archive_top_events(
    weather_model: str,
    snapshot_version: int | None = None,
    *,
    rain_metric: str = "rain_1h_mm",
    alert_threshold_mm: float = 50.0,
    limit: int = 40,
) -> list[dict[str, Any]]:
    """Các giờ có cực trị lớn nhất theo cửa sổ mưa được chọn."""
    _validate_rain_metric(rain_metric)
    try:
        return _read_records(
            """
            WITH selected_metric AS (
                SELECT
                    f.valid_time_utc,
                    CASE $2
                        WHEN 'rain_12h_mm' THEN f.rain_12h_mm
                        WHEN 'rain_24h_mm' THEN f.rain_24h_mm
                        ELSE f.rain_1h_mm
                    END AS rain_metric_mm
                FROM gold.fct_rain_archive_hourly AS f
                JOIN gold.dim_grid AS d USING (grid_cell_id)
                WHERE d.weather_model = $1
            )
            SELECT
                valid_time_utc,
                MAX(rain_metric_mm) AS max_metric_mm,
                COUNT(*) FILTER (WHERE rain_metric_mm >= $3)
                    AS elevated_grid_count
            FROM selected_metric
            GROUP BY valid_time_utc
            ORDER BY max_metric_mm DESC NULLS LAST, valid_time_utc DESC
            LIMIT $4
            """,
            [weather_model, rain_metric, alert_threshold_mm, limit],
            snapshot_version=snapshot_version,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Không thể đọc top event archive %s: %s", weather_model, exc)
        return []

def load_archive_peak_hour_for_local_date(
    weather_model: str,
    local_date: date | str,
    rain_metric: str,
    snapshot_version: int | None = None,
) -> datetime | None:
    """Giờ có cực trị lớn nhất trong một ngày Hà Nội theo cửa sổ đã chọn."""
    _validate_rain_metric(rain_metric)
    try:
        row = _read_record(
            """
            WITH hourly_max AS (
                SELECT
                    f.valid_time_utc,
                    MAX(
                        CASE $3
                            WHEN 'rain_12h_mm' THEN f.rain_12h_mm
                            WHEN 'rain_24h_mm' THEN f.rain_24h_mm
                            ELSE f.rain_1h_mm
                        END
                    ) AS max_metric_mm
                FROM gold.fct_rain_archive_hourly AS f
                JOIN gold.dim_grid AS d USING (grid_cell_id)
                WHERE d.weather_model = $1
                  AND CAST(
                        f.valid_time_utc AT TIME ZONE 'Asia/Ho_Chi_Minh' AS DATE
                      ) = CAST($2 AS DATE)
                GROUP BY f.valid_time_utc
            )
            SELECT ARG_MAX(valid_time_utc, max_metric_mm) AS peak_time_utc
            FROM hourly_max
            """,
            [weather_model, local_date, rain_metric],
            snapshot_version=snapshot_version,
        )
        return row.get("peak_time_utc")
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Không thể tìm giờ đỉnh archive %s ngày %s: %s",
            weather_model,
            local_date,
            exc,
        )
        return None

def load_archive_hours_for_local_date(
    weather_model: str,
    local_date: date | str,
    snapshot_version: int | None = None,
) -> list[datetime]:
    """Các mốc giờ thuộc một ngày Hà Nội, không dùng ngày UTC của fact."""
    try:
        rows = _read_records(
            """
            SELECT DISTINCT f.valid_time_utc
            FROM gold.fct_rain_archive_hourly AS f
            JOIN gold.dim_grid AS d USING (grid_cell_id)
            WHERE d.weather_model = $1
              AND CAST(
                    f.valid_time_utc AT TIME ZONE 'Asia/Ho_Chi_Minh' AS DATE
                  ) = CAST($2 AS DATE)
            ORDER BY f.valid_time_utc
            """,
            [weather_model, local_date],
            snapshot_version=snapshot_version,
        )
        return [row["valid_time_utc"] for row in rows]
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Không thể đọc archive hours %s ngày %s: %s",
            weather_model,
            local_date,
            exc,
        )
        return []

def load_archive_by_hour(
    weather_model: str,
    valid_time_utc: datetime | str,
    snapshot_version: int | None = None,
    *,
    sort_metric: str = "rain_1h_mm",
) -> pd.DataFrame:
    """Mưa archive chiếu từ ô lưới sang 126 phường tại một giờ."""
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
                f.rain_mm,
                f.rain_1h_mm,
                f.rain_3h_mm,
                f.rain_6h_mm,
                f.rain_12h_mm,
                f.rain_24h_mm,
                f.hanoi_rain_scenario_band,
                f.hanoi_rain_scenario_level,
                f.vn_rain_band_12h,
                f.vn_rain_band_24h,
                f.valid_time_utc
            FROM gold.fct_rain_archive_hourly AS f
            JOIN gold.bridge_ward_grid AS bwg
              ON bwg.grid_cell_id = f.grid_cell_id
             AND bwg.weather_model = $1
             AND bwg.is_active = TRUE
            JOIN gold.dim_ward AS w
              ON w.ward_code = bwg.ward_code
             AND w.is_active = TRUE
            WHERE f.valid_time_utc = $2
            ORDER BY
                CASE $3
                    WHEN 'rain_12h_mm' THEN f.rain_12h_mm
                    WHEN 'rain_24h_mm' THEN f.rain_24h_mm
                    ELSE f.rain_1h_mm
                END DESC NULLS LAST,
                w.ward_name
            """,
            [weather_model, valid_time_utc, sort_metric],
            snapshot_version=snapshot_version,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Không thể đọc archive %s tại %s: %s",
            weather_model,
            valid_time_utc,
            exc,
        )
        return pd.DataFrame()

def load_archive_hour_summary(
    weather_model: str,
    valid_time_utc: datetime | str,
    snapshot_version: int | None = None,
) -> dict[str, Any]:
    """KPI archive cấp phường tại giờ được chọn, tính trong DuckDB."""
    try:
        return _read_record(
            """
            WITH ward_archive AS (
                SELECT
                    bwg.ward_code,
                    f.grid_cell_id,
                    f.rain_1h_mm,
                    f.rain_6h_mm,
                    f.rain_12h_mm,
                    f.rain_24h_mm,
                    f.hanoi_rain_scenario_band,
                    f.hanoi_rain_scenario_level,
                    f.vn_rain_band_12h,
                    f.vn_rain_band_24h
                FROM gold.fct_rain_archive_hourly AS f
                JOIN gold.bridge_ward_grid AS bwg
                  ON bwg.grid_cell_id = f.grid_cell_id
                 AND bwg.weather_model = $1
                 AND bwg.is_active = TRUE
                WHERE f.valid_time_utc = $2
            ),
            point_status AS (
                SELECT
                    p.point_id,
                    COALESCE(
                        wa.hanoi_rain_scenario_level
                            >= p.required_rain_scenario_level,
                        FALSE
                    ) AS is_triggered
                FROM gold.dim_flood_point AS p
                LEFT JOIN ward_archive AS wa USING (ward_code)
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
                (SELECT COUNT(*) FROM point_status WHERE is_triggered)
                    AS triggered_point_count
            FROM ward_archive
            """,
            [weather_model, valid_time_utc],
            snapshot_version=snapshot_version,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Không thể tổng hợp archive %s tại %s: %s",
            weather_model,
            valid_time_utc,
            exc,
        )
        return {}

def load_archive_event_timeseries(
    weather_model: str,
    valid_time_utc: datetime | str,
    snapshot_version: int | None = None,
    window_hours: int = 18,
) -> pd.DataFrame:
    """Cực trị và trung bình theo ô lưới quanh một sự kiện archive."""
    try:
        return _read_dataframe(
            """
            SELECT
                f.valid_time_utc,
                MAX(f.rain_1h_mm) AS max_rain_1h_mm,
                AVG(f.rain_1h_mm) AS avg_rain_1h_mm,
                MAX(f.rain_6h_mm) AS max_rain_6h_mm,
                MAX(f.rain_12h_mm) AS max_rain_12h_mm,
                AVG(f.rain_12h_mm) AS avg_rain_12h_mm,
                MAX(f.rain_24h_mm) AS max_rain_24h_mm,
                AVG(f.rain_24h_mm) AS avg_rain_24h_mm
            FROM gold.fct_rain_archive_hourly AS f
            JOIN gold.dim_grid AS d USING (grid_cell_id)
            WHERE d.weather_model = $1
              AND f.valid_time_utc BETWEEN
                  CAST($2 AS TIMESTAMPTZ) - $3 * INTERVAL '1 hour'
                  AND CAST($2 AS TIMESTAMPTZ) + $3 * INTERVAL '1 hour'
            GROUP BY f.valid_time_utc
            ORDER BY f.valid_time_utc
            """,
            [weather_model, valid_time_utc, window_hours],
            snapshot_version=snapshot_version,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Không thể đọc diễn biến archive quanh %s: %s", valid_time_utc, exc
        )
        return pd.DataFrame()

def load_verified_flood_observations_until(
    valid_time_utc: datetime | str,
    snapshot_version: int | None = None,
    *,
    event_id: str | None = None,
) -> pd.DataFrame:
    """Nhãn ngập đã geocode/review xảy ra trước mốc replay trong cùng ngày."""
    try:
        return _read_dataframe(
            """
            SELECT
                observation_id,
                event_id,
                location_name_raw,
                ward_code,
                latitude,
                longitude,
                observed_at_utc,
                depth_min_cm,
                depth_max_cm,
                depth_text_raw,
                traffic_status,
                traffic_text_raw,
                source_grade,
                source_publisher,
                source_url,
                geocode_confidence
            FROM gold.fct_flood_event_observation
            WHERE geocode_verified = TRUE
              AND is_replay_eligible = TRUE
              AND observed_at_utc <= CAST($1 AS TIMESTAMPTZ)
              AND CAST(
                    observed_at_utc AT TIME ZONE 'Asia/Ho_Chi_Minh' AS DATE
                  ) = CAST(
                    CAST($1 AS TIMESTAMPTZ) AT TIME ZONE 'Asia/Ho_Chi_Minh' AS DATE
                  )
              AND ($2 IS NULL OR event_id = CAST($2 AS VARCHAR))
            ORDER BY observed_at_utc, location_name_raw
            """,
            [valid_time_utc, event_id],
            snapshot_version=snapshot_version,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Không thể đọc nhãn ngập đã xác minh tại %s: %s", valid_time_utc, exc
        )
        return pd.DataFrame()

def load_verified_flood_events(
    snapshot_version: int | None = None,
) -> list[dict[str, Any]]:
    """Sự kiện có anchor đã review để đưa lên bộ chọn archive replay."""
    try:
        return _read_records(
            """
            SELECT
                event_id,
                COUNT(*) AS observation_count,
                MIN(observed_at_utc) AS first_observed_at_utc,
                MAX(observed_at_utc) AS last_observed_at_utc,
                MAX(as_of_utc) AS replay_at_utc,
                ANY_VALUE(source_publisher) AS source_publisher,
                ANY_VALUE(source_url) AS source_url
            FROM gold.fct_flood_event_observation
            WHERE geocode_verified = TRUE
              AND is_replay_eligible = TRUE
            GROUP BY event_id
            ORDER BY last_observed_at_utc DESC, event_id
            """,
            snapshot_version=snapshot_version,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Không thể đọc danh sách sự kiện ngập đã xác minh: %s", exc)
        return []
