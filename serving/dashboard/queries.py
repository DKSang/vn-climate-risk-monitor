"""Truy vấn Gold marts cho Streamlit trên một DuckLake snapshot nhất quán.

Table forecast lưu lịch sử ở grain
``forecast_run_id × grid_cell_id × valid_time_utc``. Dashboard chỉ đọc view
current của retrieval run mới nhất. Request pin DuckLake snapshot để các query
trong cùng trang không nhìn thấy hai lần publish khác nhau.
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any

import pandas as pd

from processing.state import connect_control_plane
from vn_climate_risk_monitor.config import load_settings
from vn_climate_risk_monitor.lakehouse import get_connection

logger = logging.getLogger(__name__)

FORECAST_MODEL = "ecmwf_ifs_fc"

# Metric mưa được phép chọn động. Tên cột đi vào SQL qua CASE trên tham số bind,
# nhưng ORDER BY và whitelist này là hàng rào duy nhất chặn giá trị lạ — giữ một
# chỗ thay vì lặp lại ở từng hàm.
RAIN_METRICS = frozenset({"rain_1h_mm", "rain_12h_mm", "rain_24h_mm"})


def _validate_rain_metric(metric: str) -> str:
    if metric not in RAIN_METRICS:
        raise ValueError(f"Rain metric không hợp lệ: {metric}")
    return metric


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


def load_serving_snapshot(
    table_schema: str = "gold",
    table_name: str = "fct_rain_forecast_hourly",
) -> dict[str, Any]:
    """Resolve catalog hiện hành và lần đổi mới nhất của một bảng từ Postgres.

    Request được pin vào ``snapshot_id`` của toàn catalog để fact, dimension và
    bridge cùng một trạng thái nhất quán. ``table_snapshot_id`` cho biết snapshot
    gần nhất thực sự insert/delete/compact bảng đích, không suy luận từ
    ``MAX(_ingested_at)``. Tên bảng được bind như giá trị metadata, không được
    nội suy vào SQL.
    """
    settings = load_settings()
    connection = connect_control_plane(settings.postgres.ducklake_connection_string)
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                WITH catalog_snapshot AS (
                    SELECT
                        snapshot_id,
                        snapshot_time
                    FROM ducklake.ducklake_snapshot
                    ORDER BY snapshot_id DESC
                    LIMIT 1
                ),
                target_table AS (
                    SELECT
                        tbl.table_id,
                        tbl.begin_snapshot
                    FROM ducklake.ducklake_table AS tbl
                    JOIN ducklake.ducklake_schema AS schema
                      ON schema.schema_id = tbl.schema_id
                     AND schema.end_snapshot IS NULL
                    WHERE schema.schema_name = %s
                      AND tbl.table_name = %s
                      AND tbl.end_snapshot IS NULL
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
                    WHERE changes.changes_made ~ (
                        '(^|,)(inserted_into_table|deleted_from_table|'
                        || 'compacted_table|altered_table):'
                        || target_table.table_id::text
                        || '(,|$)'
                    )
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
                (table_schema, table_name),
            )
            row = cursor.fetchone()
            if row is None:
                return {}
            columns = [column.name for column in cursor.description]
            return dict(zip(columns, row, strict=True))
    except Exception as exc:  # noqa: BLE001
        logger.warning("Không thể resolve DuckLake serving snapshot: %s", exc)
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
                    CASE
                        WHEN p.rain_scenario = 'scenario_50_70mm'
                            THEN wa.rain_1h_mm >= 50
                        WHEN p.rain_scenario = 'scenario_70_100mm'
                            THEN wa.rain_1h_mm >= 70
                        WHEN p.rain_scenario = 'scenario_over_100mm'
                            THEN wa.rain_1h_mm > 100
                        ELSE FALSE
                    END AS is_triggered
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
                    WHERE rain_12h_mm >= 30.0
                ) AS elevated_ward_count_12h,
                COUNT(*) FILTER (
                    WHERE rain_24h_mm >= 50.0
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


def load_archive_flood_points_for_hour(
    weather_model: str,
    valid_time_utc: datetime | str,
    snapshot_version: int | None = None,
) -> pd.DataFrame:
    """Danh mục điểm úng ngập đối chiếu với một giờ archive."""
    try:
        return _read_dataframe(
            """
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
                CASE
                    WHEN p.rain_scenario = 'scenario_50_70mm'
                        THEN f.rain_1h_mm >= 50
                    WHEN p.rain_scenario = 'scenario_70_100mm'
                        THEN f.rain_1h_mm >= 70
                    WHEN p.rain_scenario = 'scenario_over_100mm'
                        THEN f.rain_1h_mm > 100
                    ELSE FALSE
                END AS is_triggered
            FROM gold.dim_flood_point AS p
            LEFT JOIN gold.bridge_ward_grid AS bwg
              ON bwg.ward_code = p.ward_code
             AND bwg.weather_model = $1
             AND bwg.is_active = TRUE
            LEFT JOIN gold.fct_rain_archive_hourly AS f
              ON f.grid_cell_id = bwg.grid_cell_id
             AND f.valid_time_utc = $2
            WHERE p.is_active = TRUE AND p.status = 'active'
            ORDER BY is_triggered DESC, p.point_id
            """,
            [weather_model, valid_time_utc],
            snapshot_version=snapshot_version,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Không thể đối chiếu điểm ngập archive tại %s: %s",
            valid_time_utc,
            exc,
        )
        return pd.DataFrame()


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
              AND is_training_eligible = TRUE
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
              AND is_training_eligible = TRUE
            GROUP BY event_id
            ORDER BY last_observed_at_utc DESC, event_id
            """,
            snapshot_version=snapshot_version,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Không thể đọc danh sách sự kiện ngập đã xác minh: %s", exc)
        return []


def load_flood_backtest_metrics(
    event_id: str,
    snapshot_version: int | None = None,
) -> list[dict[str, Any]]:
    """Metric backtest của một trận; NULL được giữ nguyên khi không đo được."""
    try:
        return _read_records(
            """
            SELECT
                rule_name,
                threshold_value,
                observation_count,
                positive_observation_count,
                negative_observation_count,
                hit_count,
                miss_count,
                false_alarm_count,
                pod,
                far,
                csi,
                risk_model_version
            FROM gold.fct_flood_backtest_metric
            WHERE event_id = $1
            ORDER BY rule_name, threshold_value NULLS FIRST
            """,
            [event_id],
            snapshot_version=snapshot_version,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Không thể đọc backtest event %s: %s", event_id, exc)
        return []


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
                f.hanoi_rain_scenario_band,
                f.vn_rain_band_12h,
                f.vn_rain_band_24h,
                risk.hazard_index,
                risk.vulnerability_index,
                risk.risk_score,
                risk.risk_model_version,
                f.valid_time_utc
            FROM gold.fct_rain_forecast_current_hourly f
            JOIN gold.bridge_ward_grid bwg
              ON bwg.grid_cell_id = f.grid_cell_id
             AND bwg.weather_model = $1
             AND bwg.is_active = TRUE
            JOIN gold.dim_ward w
              ON w.ward_code = bwg.ward_code
             AND w.is_active = TRUE
            LEFT JOIN gold.fct_flood_risk_score risk
              ON risk.ward_code = w.ward_code
             AND risk.valid_time_utc = f.valid_time_utc
            WHERE f.valid_time_utc = $2
            ORDER BY
                CASE $3
                    WHEN 'rain_12h_mm' THEN f.rain_12h_mm
                    WHEN 'rain_24h_mm' THEN f.rain_24h_mm
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
                    f.hanoi_rain_scenario_band,
                    f.vn_rain_band_12h,
                    f.vn_rain_band_24h
                FROM gold.fct_rain_forecast_current_hourly f
                JOIN gold.bridge_ward_grid bwg
                  ON bwg.grid_cell_id = f.grid_cell_id
                 AND bwg.weather_model = $1
                 AND bwg.is_active = TRUE
                WHERE f.valid_time_utc = $2
            ),
            point_status AS (
                SELECT
                    p.point_id,
                    CASE
                        WHEN p.rain_scenario = 'scenario_50_70mm'
                            THEN wf.rain_1h_mm >= 50
                        WHEN p.rain_scenario = 'scenario_70_100mm'
                            THEN wf.rain_1h_mm >= 70
                        WHEN p.rain_scenario = 'scenario_over_100mm'
                            THEN wf.rain_1h_mm > 100
                        ELSE FALSE
                    END AS is_triggered
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
                    WHERE rain_12h_mm >= 30.0
                ) AS elevated_ward_count_12h,
                COUNT(*) FILTER (
                    WHERE rain_24h_mm >= 50.0
                ) AS elevated_ward_count_24h,
                (SELECT COUNT(*) FROM point_status WHERE is_triggered)
                    AS triggered_point_count,
                (
                    SELECT COUNT(*)
                    FROM gold.fct_flood_risk_score risk
                    WHERE risk.valid_time_utc = $2 AND risk.risk_score >= 50
                ) AS experimental_risk_50_count,
                (
                    SELECT MAX(risk_score)
                    FROM gold.fct_flood_risk_score risk
                    WHERE risk.valid_time_utc = $2
                ) AS max_experimental_risk_score
            FROM ward_forecast
            """,
            [FORECAST_MODEL, valid_time_utc],
            snapshot_version=snapshot_version,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Không thể tổng hợp forecast tại %s: %s", valid_time_utc, exc)
        return {}


def load_forecast_risk_ranking(
    valid_time_utc: datetime | str,
    snapshot_version: int | None = None,
    limit: int = 10,
) -> pd.DataFrame:
    """Xếp hạng risk index heuristic trong DuckDB; không phải xác suất ngập."""
    try:
        return _read_dataframe(
            """
            SELECT
                ward.ward_name,
                risk.risk_score,
                risk.hazard_index,
                risk.vulnerability_index,
                risk.rain_24h_mm,
                risk.risk_model_version
            FROM gold.fct_flood_risk_score risk
            JOIN gold.dim_ward ward
              ON ward.ward_code = risk.ward_code AND ward.is_active = TRUE
            WHERE risk.valid_time_utc = $1
            ORDER BY risk.risk_score DESC, ward.ward_name
            LIMIT $2
            """,
            [valid_time_utc, limit],
            snapshot_version=snapshot_version,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Không thể xếp hạng risk tại %s: %s", valid_time_utc, exc)
        return pd.DataFrame()


def load_flood_points_for_hour(
    valid_time_utc: datetime | str,
    snapshot_version: int | None = None,
) -> pd.DataFrame:
    """Danh mục điểm kèm trạng thái đạt ngưỡng tại giờ được chọn."""
    try:
        return _read_dataframe(
            """
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
                CASE
                    WHEN p.rain_scenario = 'scenario_50_70mm'
                        THEN f.rain_1h_mm >= 50
                    WHEN p.rain_scenario = 'scenario_70_100mm'
                        THEN f.rain_1h_mm >= 70
                    WHEN p.rain_scenario = 'scenario_over_100mm'
                        THEN f.rain_1h_mm > 100
                    ELSE FALSE
                END AS is_triggered
            FROM gold.dim_flood_point p
            LEFT JOIN gold.bridge_ward_grid bwg
              ON bwg.ward_code = p.ward_code
             AND bwg.weather_model = $1
             AND bwg.is_active = TRUE
            LEFT JOIN gold.fct_rain_forecast_current_hourly f
              ON f.grid_cell_id = bwg.grid_cell_id
             AND f.valid_time_utc = $2
            WHERE p.is_active = TRUE AND p.status = 'active'
            ORDER BY is_triggered DESC, p.point_id
            """,
            [FORECAST_MODEL, valid_time_utc],
            snapshot_version=snapshot_version,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Không thể đối chiếu điểm ngập tại %s: %s", valid_time_utc, exc)
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
                    SUM(precipitation_mm) AS horizon_rain_mm,
                    SUM(precipitation_mm) FILTER (WHERE hour_number <= 24)
                        AS next_24h_rain_mm,
                    MAX(rain_1h_mm) AS peak_1h_mm,
                    ARG_MAX(valid_time_utc, rain_1h_mm) AS peak_time_utc,
                    MAX(rain_6h_mm) AS peak_6h_mm,
                    MAX(precipitation_probability_pct) AS max_probability_pct
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
                      AND CASE
                          WHEN p.rain_scenario = 'scenario_50_70mm'
                              THEN aggregated.peak_1h_mm >= 50
                          WHEN p.rain_scenario = 'scenario_70_100mm'
                              THEN aggregated.peak_1h_mm >= 70
                          WHEN p.rain_scenario = 'scenario_over_100mm'
                              THEN aggregated.peak_1h_mm > 100
                          ELSE FALSE
                      END
                ) AS triggered_point_count
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
                SELECT MAX(f.rain_1h_mm) AS peak_1h_mm
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
                CASE
                    WHEN p.rain_scenario = 'scenario_50_70mm'
                        THEN peak.peak_1h_mm >= 50
                    WHEN p.rain_scenario = 'scenario_70_100mm'
                        THEN peak.peak_1h_mm >= 70
                    WHEN p.rain_scenario = 'scenario_over_100mm'
                        THEN peak.peak_1h_mm > 100
                    ELSE FALSE
                END AS threshold_reached
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
