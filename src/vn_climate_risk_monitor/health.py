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
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=indent, default=str)


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
    complete_runs = _scalar(
        connection,
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
        run_hours AS (
            SELECT run_name, valid_time_utc, COUNT(*) AS locations
            FROM latest_file_ingests
            GROUP BY run_name, valid_time_utc
        ),
        wards AS (
            SELECT COUNT(*) AS expected FROM gold.dim_ward
        )
        SELECT COUNT(*)
        FROM (
            SELECT run_name
            FROM run_hours CROSS JOIN wards
            GROUP BY run_name, expected
            HAVING MIN(locations) = expected AND MAX(locations) = expected
        )
        """,
    )
    return _result(
        "forecast.complete_run",
        "PASS" if complete_runs and complete_runs > 0 else "FAIL",
        f"{complete_runs or 0} run forecast có đủ phường ở mọi giờ",
        complete_runs=complete_runs or 0,
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
            FROM silver.int_weather_hourly
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
        FROM silver.int_weather_hourly
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


def _check_gold(connection: Any) -> list[CheckResult]:
    """Fact mưa lõi: có tồn tại, có dòng, và grain không trùng.

    CỐ Ý không kiểm freshness ở phase này: pipeline mới chỉ có lịch sử, nên
    `valid_time_utc` mới nhất luôn lùi vài tuần một cách hợp lệ. Freshness quay
    lại cùng nhánh forecast.
    """
    relation = "gold.fct_rain_hourly"
    if not _relation_exists(connection, relation):
        return [_result("gold.rain_hourly.exists", "FAIL", f"Không tìm thấy {relation}")]
    rows, distinct_keys = connection.execute(
        f"SELECT COUNT(*), COUNT(DISTINCT rain_hourly_key) FROM {relation}"
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
            f"{duplicates:,} khóa rain_hourly bị trùng",
            duplicate_keys=duplicates,
        ),
    ]


def _check_control_plane() -> list[CheckResult]:
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
    finally:
        connection.close()
    backlog = pending + processing
    max_backlog = int(os.getenv("HEALTH_MAX_FILE_BACKLOG", "0"))
    return [
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
        checks.extend(_check_control_plane())
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
                if _relation_exists(
                    connection, "catalog1.silver.stg_weather_forecast"
                ):
                    checks.append(_check_forecast_coverage(connection))
                if require_gold:
                    checks.extend(_check_gold(connection))
            if scope in {"archive", "all"}:
                # era5 và ecmwf_ifs dùng CHUNG một bảng staging, phân biệt bằng
                # cột `weather_model`, nên một check thay cho hai. Độ phủ theo
                # từng model vẫn được `archive.monthly_coverage` kiểm riêng.
                checks.extend(
                    _check_weather_table(
                        connection,
                        "catalog1.silver.stg_weather_hourly",
                        required=True,
                        freshness_hours=None,
                    )
                )
                if _relation_exists(connection, "silver.int_weather_hourly"):
                    checks.append(_check_archive_coverage(connection))
                    checks.append(_check_archive_duplicates(connection))
                    checks.append(_check_mapping(connection))
                elif require_gold:
                    checks.append(
                        _result(
                            "archive.silver", "FAIL", "Thiếu silver.int_weather_hourly"
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
