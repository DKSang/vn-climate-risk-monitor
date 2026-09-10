"""Operational health and data-quality checks for the lakehouse.

The collector intentionally reads catalogs instead of Parquet paths so DuckLake
snapshot semantics are preserved. It never includes connection strings or
credentials in its output.
"""

from __future__ import annotations

import json
import os
import shutil
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from autoloader import connect_control_plane
from vn_climate_risk_monitor.config import load_settings
from vn_climate_risk_monitor.lakehouse import get_connection

Status = Literal["PASS", "WARN", "FAIL"]


@dataclass(frozen=True)
class CheckResult:
    name: str
    status: Status
    message: str
    metrics: dict[str, Any]


@dataclass(frozen=True)
class HealthReport:
    status: Literal["HEALTHY", "DEGRADED", "UNHEALTHY"]
    checked_at_utc: str
    scope: str
    checks: tuple[CheckResult, ...]

    @property
    def exit_code(self) -> int:
        return 1 if self.status == "UNHEALTHY" else 0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_json(self, *, indent: int | None = 2) -> str:
        return json.dumps(
            self.to_dict(), ensure_ascii=False, indent=indent, default=str
        )


def _result(
    name: str,
    status: Status,
    message: str,
    **metrics: Any,
) -> CheckResult:
    return CheckResult(name=name, status=status, message=message, metrics=metrics)


def _relation_exists(connection: Any, relation: str) -> bool:
    try:
        connection.execute(f"SELECT 1 FROM {relation} WHERE false")
    except Exception:  # noqa: BLE001 - adapter exceptions differ by catalog
        return False
    return True


def _scalar(connection: Any, query: str) -> Any:
    row = connection.execute(query).fetchone()
    return None if row is None else row[0]


def _hours_old(value: datetime | None) -> float | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return (datetime.now(UTC) - value.astimezone(UTC)).total_seconds() / 3600


def _check_weather_table(
    connection: Any,
    relation: str,
    *,
    required: bool,
    freshness_hours: int | None,
) -> list[CheckResult]:
    label = relation.rsplit(".", 1)[-1]
    if not _relation_exists(connection, relation):
        status: Status = "FAIL" if required else "WARN"
        return [_result(f"{label}.exists", status, f"Không tìm thấy {relation}")]

    row_count, null_keys, rescued, out_of_range, latest = connection.execute(
        f"""
        SELECT
            COUNT(*),
            COUNT(*) FILTER (
                WHERE grid_latitude IS NULL
                   OR grid_longitude IS NULL
                   OR valid_time_utc IS NULL
            ),
            COUNT(*) FILTER (WHERE _rescued_data IS NOT NULL),
            COUNT(*) FILTER (
                WHERE precipitation_mm < 0 OR precipitation_mm > 500
            ),
            MAX(_ingested_at)
        FROM {relation}
        """
    ).fetchone()
    checks = [
        _result(
            f"{label}.rows",
            "PASS" if row_count > 0 else "FAIL",
            f"{row_count:,} dòng" if row_count else "Bảng rỗng",
            row_count=row_count,
        ),
        _result(
            f"{label}.required_keys",
            "PASS" if null_keys == 0 else "FAIL",
            f"{null_keys:,} dòng thiếu grid/time",
            null_key_rows=null_keys,
        ),
        _result(
            f"{label}.rescued_data",
            "PASS" if rescued == 0 else "FAIL",
            f"{rescued:,} dòng có dữ liệu cứu hộ",
            rescued_rows=rescued,
            rescued_ratio=(rescued / row_count if row_count else None),
        ),
        _result(
            f"{label}.precipitation_range",
            "PASS" if out_of_range == 0 else "FAIL",
            f"{out_of_range:,} dòng ngoài [0, 500] mm",
            out_of_range_rows=out_of_range,
        ),
    ]
    if freshness_hours is not None:
        age = _hours_old(latest)
        checks.append(
            _result(
                f"{label}.freshness",
                "PASS" if age is not None and age <= freshness_hours else "FAIL",
                "Chưa có thời điểm ingest"
                if age is None
                else f"Dữ liệu mới nhất cách {age:.2f} giờ",
                age_hours=age,
                maximum_hours=freshness_hours,
            )
        )
    return checks


def _check_forecast_coverage(connection: Any) -> CheckResult:
    if not _relation_exists(connection, "gold.dim_ward"):
        return _result(
            "forecast.complete_run", "FAIL", "Thiếu gold.dim_ward; chạy make transform"
        )
    run = connection.execute(
        """
        WITH source_rows AS (
            SELECT
                REGEXP_EXTRACT(
                    _source_file,
                    '/incremental/[0-9]{4}/[0-9]{2}/[0-9]{2}/[0-9]{2}/(run_[0-9]{8}T[0-9]{6})/',
                    1
                ) AS run_name,
                valid_time_utc,
                _source_file,
                _ingested_at
            FROM catalog1.silver.stg_weather_forecast
        ),
        latest_file_ingests AS (
            SELECT *
            FROM source_rows
            WHERE run_name <> ''
            QUALIFY _ingested_at = MAX(_ingested_at) OVER (PARTITION BY _source_file)
        ),
        latest_run AS (
            SELECT run_name
            FROM latest_file_ingests
            GROUP BY run_name
            ORDER BY MAX(_ingested_at) DESC, run_name DESC
            LIMIT 1
        ),
        run_hours AS (
            SELECT run_name, valid_time_utc, COUNT(*) AS locations
            FROM latest_file_ingests
            WHERE run_name = (SELECT run_name FROM latest_run)
            GROUP BY run_name, valid_time_utc
        ),
        wards AS (
            SELECT COUNT(*) AS expected FROM gold.dim_ward
        )
        SELECT
            run_name,
            COUNT(*) AS forecast_hours,
            MIN(locations) AS minimum_locations,
            MAX(locations) AS maximum_locations,
            expected
        FROM run_hours CROSS JOIN wards
        GROUP BY run_name, expected
        """,
    ).fetchone()
    if run is None:
        return _result(
            "forecast.complete_run",
            "FAIL",
            "Không tìm thấy forecast run hợp lệ trong _source_file",
        )

    run_name, hours, minimum_locations, maximum_locations, expected_locations = run
    expected_hours = load_settings().open_meteo.forecast_hours
    complete = (
        hours == expected_hours
        and minimum_locations == expected_locations
        and maximum_locations == expected_locations
    )
    return _result(
        "forecast.complete_run",
        "PASS" if complete else "FAIL",
        (
            f"{run_name}: {hours}/{expected_hours} giờ, "
            f"{minimum_locations}–{maximum_locations}/{expected_locations} phường"
        ),
        run_name=run_name,
        forecast_hours=hours,
        expected_hours=expected_hours,
        minimum_locations=minimum_locations,
        maximum_locations=maximum_locations,
        expected_locations=expected_locations,
    )


def _check_archive_coverage(connection: Any) -> CheckResult:
    incomplete = _scalar(
        connection,
        """
        WITH monthly AS (
            SELECT
                weather_model,
                grid_latitude,
                grid_longitude,
                DATE_TRUNC('month', valid_time_utc) AS month_start,
                COUNT(DISTINCT valid_time_utc) AS observed_hours
            FROM silver.int_weather_archive_hourly
            WHERE valid_time_utc < DATE_TRUNC('month', CURRENT_TIMESTAMP)
            GROUP BY 1, 2, 3, 4
        )
        SELECT COUNT(*)
        FROM monthly
        WHERE observed_hours <> DATE_DIFF(
            'hour', month_start, month_start + INTERVAL '1 month'
        )
        """,
    )
    return _result(
        "archive.monthly_coverage",
        "PASS" if incomplete == 0 else "FAIL",
        f"{incomplete:,} nhóm model/grid/tháng quá khứ thiếu giờ",
        incomplete_month_groups=incomplete,
    )


def _check_archive_duplicates(connection: Any) -> CheckResult:
    raw_count, unique_count = connection.execute(
        """
        SELECT COUNT(*), COUNT(DISTINCT (grid_cell_id, valid_time_utc))
        FROM silver.int_weather_archive_hourly
        """
    ).fetchone()
    duplicates = raw_count - unique_count
    return _result(
        "archive.silver_grain",
        "PASS" if duplicates == 0 else "FAIL",
        f"{duplicates:,} dòng trùng grain sau khi dedup ở Silver",
        row_count=raw_count,
        unique_grain_count=unique_count,
        duplicate_rows=duplicates,
        duplicate_ratio=(duplicates / raw_count if raw_count else 0),
    )


def _check_mapping(connection: Any) -> CheckResult:
    """Mỗi phường phải có đúng một ô lưới cho mỗi model.

    Bản cũ đo `mapping_distance_km`; cột đó đến từ model `ward_grid_map` tự tính
    nearest-neighbour, nay đã bỏ. Ánh xạ hiện lấy thẳng từ phép snap của
    Open-Meteo (seed) nên khoảng cách không còn là thứ ta kiểm soát — thứ đáng
    kiểm là ĐỘ PHỦ: thiếu một dòng thì phường đó biến mất khỏi fact mà INNER JOIN
    không báo gì.
    """
    relation = "gold.bridge_ward_grid"
    if not _relation_exists(connection, relation):
        return _result("archive.ward_mapping", "FAIL", f"Thiếu {relation}")
    rows, wards, models = connection.execute(
        f"""
        SELECT COUNT(*), COUNT(DISTINCT ward_code), COUNT(DISTINCT weather_model)
        FROM {relation}
        """
    ).fetchone()
    expected = wards * models
    return _result(
        "archive.ward_mapping",
        "PASS" if rows == expected and rows > 0 else "FAIL",
        f"{rows} ánh xạ cho {wards} phường × {models} model (cần {expected})",
        mapping_count=rows,
        ward_count=wards,
        model_count=models,
        expected_count=expected,
    )


def _check_archive_gold(connection: Any) -> list[CheckResult]:
    """Fact mưa lõi: có tồn tại, có dòng, và grain không trùng.

    CỐ Ý không kiểm freshness ở phase này: pipeline mới chỉ có lịch sử, nên
    `valid_time_utc` mới nhất luôn lùi vài tuần một cách hợp lệ. Freshness quay
    lại cùng nhánh forecast.
    """
    relation = "gold.fct_rain_archive_hourly"
    if not _relation_exists(connection, relation):
        return [
            _result("gold.rain_hourly.exists", "FAIL", f"Không tìm thấy {relation}")
        ]
    rows, distinct_keys = connection.execute(
        f"SELECT COUNT(*), COUNT(DISTINCT rain_archive_hourly_key) FROM {relation}"
    ).fetchone()
    duplicates = rows - distinct_keys
    return [
        _result(
            "gold.rain_hourly.rows",
            "PASS" if rows > 0 else "FAIL",
            f"{rows:,} dòng trong {relation}",
            row_count=rows,
        ),
        _result(
            "gold.rain_hourly.grain",
            "PASS" if duplicates == 0 else "FAIL",
            f"{duplicates:,} khóa rain_archive_hourly bị trùng",
            duplicate_keys=duplicates,
        ),
    ]


def _check_forecast_gold(connection: Any) -> list[CheckResult]:
    """Current view và pressure alert phải được publish cùng forecast run."""
    current_relation = "gold.fct_rain_forecast_current_hourly"
    alert_relation = "gold.fct_rain_pressure_alert"
    if not _relation_exists(connection, current_relation):
        return [
            _result(
                "forecast.gold.current_exists",
                "FAIL",
                f"Không tìm thấy {current_relation}",
            )
        ]
    if not _relation_exists(connection, alert_relation):
        return [
            _result(
                "forecast.pressure.exists",
                "FAIL",
                f"Không tìm thấy {alert_relation}",
            )
        ]
    current_rows, current_distinct_keys, expired, current_runs = connection.execute(
        f"""
        SELECT
            COUNT(*),
            COUNT(DISTINCT rain_forecast_hourly_key),
            COUNT(*) FILTER (
                WHERE valid_time_utc < DATE_TRUNC('hour', CURRENT_TIMESTAMP)
            ),
            COUNT(DISTINCT forecast_run_id)
        FROM {current_relation}
        """
    ).fetchone()
    current_duplicates = current_rows - current_distinct_keys

    rows, distinct_keys, invalid, incomplete, unknown, first_hour, last_hour = (
        connection.execute(
            f"""
        SELECT
            COUNT(*),
            COUNT(DISTINCT rain_pressure_alert_key),
            COUNT(*) FILTER (
                WHERE pressure_level NOT IN ('UNKNOWN', 'NORMAL', 'WATCH', 'ELEVATED', 'HIGH')
                   OR coverage_status NOT IN ('COMPLETE', 'PARTIAL', 'NONE')
                   OR pressure_score < 0
                   OR pressure_score > 100
                   OR (pressure_level = 'NORMAL' AND coverage_status <> 'COMPLETE')
                   OR (coverage_status = 'NONE' AND pressure_level <> 'UNKNOWN')
                   OR (coverage_status = 'NONE' AND pressure_score IS NOT NULL)
                   OR (coverage_status <> 'NONE' AND pressure_score IS NULL)
                   OR (revision_direction = 'UNKNOWN' AND forecast_next_24h_mm IS NOT NULL)
                   OR (revision_direction <> 'UNKNOWN' AND forecast_next_24h_mm IS NULL)
            ),
            COUNT(*) FILTER (WHERE coverage_status <> 'COMPLETE'),
            COUNT(*) FILTER (WHERE pressure_level = 'UNKNOWN'),
            MIN(valid_time_utc),
            MAX(valid_time_utc)
        FROM {alert_relation}
        """
        ).fetchone()
    )
    duplicates = rows - distinct_keys
    missing_current = _scalar(
        connection,
        f"""
        SELECT COUNT(*)
        FROM {current_relation} AS forecast
        JOIN gold.bridge_ward_grid AS bridge
          ON bridge.grid_cell_id = forecast.grid_cell_id
         AND bridge.weather_model = 'ecmwf_ifs_fc'
         AND bridge.is_active = TRUE
        LEFT JOIN {alert_relation} AS pressure
          ON pressure.forecast_run_id = forecast.forecast_run_id
         AND pressure.ward_code = bridge.ward_code
         AND pressure.valid_time_utc = forecast.valid_time_utc
        WHERE pressure.rain_pressure_alert_key IS NULL
        """,
    )
    return [
        _result(
            "forecast.gold.current_rows",
            "PASS" if current_rows > 0 else "FAIL",
            f"{current_rows:,} dòng trong current horizon"
            if current_rows
            else "View rỗng",
            row_count=current_rows,
        ),
        _result(
            "forecast.gold.current_grain",
            "PASS" if current_duplicates == 0 else "FAIL",
            f"{current_duplicates:,} khóa current forecast bị trùng",
            duplicate_keys=current_duplicates,
        ),
        _result(
            "forecast.gold.current_horizon",
            "PASS" if expired == 0 and current_runs == 1 else "FAIL",
            f"{expired:,} dòng hết hạn; {current_runs} forecast run",
            expired_rows=expired,
            forecast_run_count=current_runs,
        ),
        _result(
            "forecast.pressure.rows",
            "PASS" if rows > 0 else "FAIL",
            f"{rows:,} dòng cảnh báo áp lực mưa" if rows else "Bảng rỗng",
            row_count=rows,
            first_hour=first_hour,
            last_hour=last_hour,
        ),
        _result(
            "forecast.pressure.grain",
            "PASS" if duplicates == 0 else "FAIL",
            f"{duplicates:,} khóa pressure alert bị trùng",
            duplicate_keys=duplicates,
        ),
        _result(
            "forecast.pressure.contract",
            "PASS" if invalid == 0 else "FAIL",
            f"{invalid:,} dòng vi phạm level/score/coverage/revision",
            invalid_rows=invalid,
        ),
        _result(
            "forecast.pressure.coverage",
            "PASS",
            f"{incomplete:,} dòng coverage chưa đủ; {unknown:,} dòng UNKNOWN",
            incomplete_rows=incomplete,
            unknown_rows=unknown,
        ),
        _result(
            "forecast.pressure.current_coverage",
            "PASS" if missing_current == 0 else "FAIL",
            f"{missing_current:,} ward-giờ current thiếu pressure cùng forecast run",
            missing_current_rows=missing_current,
        ),
    ]


def _check_control_plane(
    *, scope: Literal["forecast", "archive", "all"], require_gold: bool
) -> list[CheckResult]:
    settings = load_settings()
    connection = connect_control_plane(settings.postgres.ducklake_connection_string)
    try:
        pending, processing, failed = connection.execute(
            """
            SELECT
                COUNT(*) FILTER (WHERE status = 'PENDING'),
                COUNT(*) FILTER (WHERE status = 'PROCESSING'),
                COUNT(*) FILTER (WHERE status = 'FAILED')
            FROM ingestion.ingestion_files
            """
        ).fetchone()
        backlog = pending + processing
        max_backlog = int(os.getenv("HEALTH_MAX_FILE_BACKLOG", "0"))
        checks = [
            _result(
                "ingestion.backlog",
                "PASS" if backlog <= max_backlog else "WARN",
                f"{backlog} file đang chờ/được xử lý",
                pending_files=pending,
                processing_files=processing,
                allowed_backlog=max_backlog,
            ),
            _result(
                "ingestion.failed_files",
                "PASS" if failed == 0 else "FAIL",
                f"{failed} file FAILED",
                failed_files=failed,
            ),
        ]
        if require_gold:
            process_keys = []
            if scope in {"forecast", "all"}:
                process_keys.append("forecast_gold")
            if scope in {"archive", "all"}:
                process_keys.append("rain_gold")
            for process_key in process_keys:
                row = connection.execute(
                    """
                    SELECT run.published_snapshot_id,
                           snapshot.snapshot_id IS NOT NULL
                    FROM processing.processing_runs AS run
                    LEFT JOIN ducklake.ducklake_snapshot AS snapshot
                      ON snapshot.snapshot_id = run.published_snapshot_id
                    WHERE run.process_key = %s
                      AND run.scope = 'production'
                      AND run.status = 'SUCCEEDED'
                      AND run.published_snapshot_id IS NOT NULL
                    ORDER BY run.completed_at_utc DESC
                    LIMIT 1
                    """,
                    (process_key,),
                ).fetchone()
                snapshot_id = None if row is None else row[0]
                available = bool(row and row[1])
                checks.append(
                    _result(
                        f"gold.publication.{process_key}",
                        "PASS" if available else "FAIL",
                        (
                            f"Snapshot S{snapshot_id} đã publish và còn đọc được"
                            if available
                            else "Không có snapshot từ Gold run thành công còn đọc được"
                        ),
                        snapshot_id=snapshot_id,
                        snapshot_available=available,
                    )
                )
        return checks
    finally:
        connection.close()


def _check_disk() -> CheckResult:
    path = Path(os.getenv("HEALTH_DISK_PATH", ".")).resolve()
    free_gb = shutil.disk_usage(path).free / (1024**3)
    minimum = float(os.getenv("HEALTH_MIN_FREE_DISK_GB", "5"))
    return _result(
        "host.free_disk",
        "PASS" if free_gb >= minimum else "FAIL",
        f"Còn {free_gb:.2f} GiB tại {path}",
        free_gib=round(free_gb, 3),
        minimum_gib=minimum,
        path=str(path),
    )


def collect_health(
    *,
    scope: Literal["forecast", "archive", "all"] = "all",
    require_gold: bool = False,
) -> HealthReport:
    """Collect health without raising for individual infrastructure failures."""
    checks: list[CheckResult] = [_check_disk()]
    try:
        checks.extend(_check_control_plane(scope=scope, require_gold=require_gold))
    except Exception as error:  # noqa: BLE001 - health must report, not crash
        checks.append(
            _result(
                "ingestion.control_plane",
                "FAIL",
                f"Không đọc được PostgreSQL control plane: {type(error).__name__}",
            )
        )

    try:
        connection = get_connection(read_only=True)
    except Exception as error:  # noqa: BLE001
        checks.append(
            _result(
                "lakehouse.connection",
                "FAIL",
                f"Không attach được DuckLake: {type(error).__name__}",
            )
        )
    else:
        try:
            if scope in {"forecast", "all"}:
                checks.extend(
                    _check_weather_table(
                        connection,
                        "catalog1.silver.stg_weather_forecast",
                        required=True,
                        freshness_hours=24,
                    )
                )
                if _relation_exists(connection, "catalog1.silver.stg_weather_forecast"):
                    checks.append(_check_forecast_coverage(connection))
                if require_gold:
                    checks.extend(_check_forecast_gold(connection))
            if scope in {"archive", "all"}:
                # era5 và ecmwf_ifs dùng CHUNG một bảng staging, phân biệt bằng
                # cột `weather_model`, nên một check thay cho hai. Độ phủ theo
                # từng model vẫn được `archive.monthly_coverage` kiểm riêng.
                checks.extend(
                    _check_weather_table(
                        connection,
                        "catalog1.silver.stg_weather_archive_hourly",
                        required=True,
                        freshness_hours=None,
                    )
                )
                if _relation_exists(connection, "silver.int_weather_archive_hourly"):
                    checks.append(_check_archive_coverage(connection))
                    checks.append(_check_archive_duplicates(connection))
                    checks.append(_check_mapping(connection))
                    if require_gold:
                        checks.extend(_check_archive_gold(connection))
                elif require_gold:
                    checks.append(
                        _result(
                            "archive.silver",
                            "FAIL",
                            "Thiếu silver.int_weather_archive_hourly",
                        )
                    )
        except Exception as error:  # noqa: BLE001
            checks.append(
                _result(
                    "lakehouse.query",
                    "FAIL",
                    f"Truy vấn health thất bại: {type(error).__name__}: {error}",
                )
            )
        finally:
            connection.close()

    statuses = {check.status for check in checks}
    overall: Literal["HEALTHY", "DEGRADED", "UNHEALTHY"]
    if "FAIL" in statuses:
        overall = "UNHEALTHY"
    elif "WARN" in statuses:
        overall = "DEGRADED"
    else:
        overall = "HEALTHY"
    return HealthReport(
        status=overall,
        checked_at_utc=datetime.now(UTC).isoformat(),
        scope=scope,
        checks=tuple(checks),
    )
