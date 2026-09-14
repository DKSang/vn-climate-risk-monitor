"""Operational health checks for the lakehouse and ingestion control plane."""

from __future__ import annotations

import argparse
import json
import os
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal
from urllib.request import Request, urlopen

from vn_climate_risk_monitor.auto_loader.config import SOURCE_GROUPS
from vn_climate_risk_monitor.auto_loader.state import connect_control_plane
from vn_climate_risk_monitor.platform.lakehouse import get_connection
from vn_climate_risk_monitor.platform.settings import load_settings

Status = Literal["PASS", "WARN", "FAIL"]
HealthScope = Literal["forecast", "archive", "all"]
INGESTION_PIPELINES: dict[HealthScope, tuple[str, ...]] = {
    group: tuple(config.name for config in configs)
    for group, configs in SOURCE_GROUPS.items()
} | {
    "all": (),
}


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
    """Check staging presence and freshness; row-level rules belong to dbt."""
    label = relation.rsplit(".", 1)[-1]
    if not _relation_exists(connection, relation):
        status: Status = "FAIL" if required else "WARN"
        return [_result(f"{label}.exists", status, f"Không tìm thấy {relation}")]
    if freshness_hours is None:
        return []

    latest = connection.execute(
        f"SELECT MAX(_ingested_at) FROM {relation}"
    ).fetchone()[0]
    age = _hours_old(latest)
    return [
        _result(
            f"{label}.freshness",
            "PASS" if age is not None and age <= freshness_hours else "FAIL",
            "Chưa có thời điểm ingest"
            if age is None
            else f"Dữ liệu mới nhất cách {age:.2f} giờ",
            age_hours=age,
            maximum_hours=freshness_hours,
        )
    ]


def _check_control_plane(
    *, scope: HealthScope, require_gold: bool
) -> list[CheckResult]:
    settings = load_settings()
    connection = connect_control_plane(settings.postgres.ducklake_connection_string)
    try:
        pipelines = INGESTION_PIPELINES[scope]
        pipeline_filter = ""
        parameters: tuple[object, ...] = ()
        if pipelines:
            pipeline_filter = "WHERE run.pipeline_name = ANY(%s)"
            parameters = (list(pipelines),)
        incomplete, failed = connection.execute(
            f"""
            SELECT
                COUNT(*) FILTER (WHERE file.status IN ('PENDING', 'PROCESSING')),
                COUNT(*) FILTER (WHERE file.status = 'FAILED')
            FROM ingestion.ingestion_files AS file
            JOIN ingestion.ingestion_runs AS run USING (attempt_id)
            {pipeline_filter}
            """
            ,
            parameters,
        ).fetchone()
        allowed_incomplete = int(os.getenv("HEALTH_MAX_FILE_BACKLOG", "0"))
        checks = [
            _result(
                "ingestion.incomplete_files",
                "PASS" if incomplete <= allowed_incomplete else "FAIL",
                f"{incomplete} file chưa COMMITTED",
                incomplete_files=incomplete,
                allowed_incomplete=allowed_incomplete,
            ),
            _result(
                "ingestion.failed_files",
                "PASS" if failed == 0 else "FAIL",
                f"{failed} file FAILED",
                failed_files=failed,
            ),
        ]
        if require_gold:
            checks.extend(_check_validated_snapshots(connection, scope=scope))
        return checks
    finally:
        connection.close()


def _check_validated_snapshots(
    connection: Any, *, scope: HealthScope
) -> list[CheckResult]:
    process_keys = {
        "forecast": ("forecast",),
        "archive": ("archive",),
        "all": ("forecast", "archive"),
    }[scope]
    checks: list[CheckResult] = []
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
                f"snapshot.validated.{process_key}",
                "PASS" if available else "FAIL",
                (
                    f"Snapshot S{snapshot_id} đã publish và còn đọc được"
                    if available
                    else "Không có snapshot từ run thành công còn đọc được"
                ),
                snapshot_id=snapshot_id,
                snapshot_available=available,
            )
        )
    return checks


def _check_gold_readiness(
    connection: Any, *, scope: HealthScope
) -> list[CheckResult]:
    relations: list[tuple[str, str]] = []
    if scope in {"forecast", "all"}:
        relations.extend(
            (
                ("forecast", "gold.fct_rain_forecast_hourly"),
                ("forecast_current", "gold.fct_rain_forecast_current_hourly"),
                ("forecast_pressure", "gold.fct_rain_pressure_alert"),
            )
        )
    if scope in {"archive", "all"}:
        relations.append(("archive", "gold.fct_rain_archive_hourly"))

    checks: list[CheckResult] = []
    for label, relation in relations:
        try:
            row = connection.execute(
                f"SELECT 1 FROM {relation} LIMIT 1"
            ).fetchone()
            ready = row is not None
        except Exception:  # noqa: BLE001 - report missing/unreadable Gold
            ready = False
        checks.append(
            _result(
                f"gold.readiness.{label}",
                "PASS" if ready else "FAIL",
                f"{relation} sẵn sàng đọc" if ready else f"{relation} chưa sẵn sàng",
                relation=relation,
            )
        )
    return checks


def collect_health(
    *,
    scope: HealthScope = "all",
    require_gold: bool = False,
) -> HealthReport:
    """Collect only infrastructure, freshness, file, Gold, and snapshot checks."""
    checks: list[CheckResult] = []
    try:
        checks.extend(_check_control_plane(scope=scope, require_gold=require_gold))
    except Exception as error:  # noqa: BLE001 - health must report, not crash
        checks.append(
            _result(
                "control_plane.connection",
                "FAIL",
                f"Không đọc được PostgreSQL control plane: {type(error).__name__}",
            )
        )

    try:
        connection = get_connection(read_only=True)
    except Exception as error:  # noqa: BLE001 - health must report, not crash
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
            if scope in {"archive", "all"}:
                checks.extend(
                    _check_weather_table(
                        connection,
                        "catalog1.silver.stg_weather_archive_hourly",
                        required=True,
                        freshness_hours=None,
                    )
                )
            if require_gold:
                checks.extend(_check_gold_readiness(connection, scope=scope))
        except Exception as error:  # noqa: BLE001 - report query failures
            checks.append(
                _result(
                    "lakehouse.health_query",
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


def _notify(report: HealthReport) -> None:
    webhook = os.getenv("ALERT_WEBHOOK_URL")
    if report.status == "HEALTHY" or not webhook:
        return
    body = json.dumps(
        {
            "project": "vn-climate-risk-monitor",
            "status": report.status,
            "scope": report.scope,
            "checked_at_utc": report.checked_at_utc,
            "failed_checks": [
                check.name for check in report.checks if check.status != "PASS"
            ],
        }
    ).encode()
    request = Request(
        webhook,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urlopen(request, timeout=10) as response:
        if response.status >= 300:
            raise RuntimeError(f"Webhook trả HTTP {response.status}")


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scope", choices=("forecast", "archive", "all"), default="all"
    )
    parser.add_argument(
        "--require-gold",
        action="store_true",
        help="Kiểm tra Gold sau dbt build",
    )
    parser.add_argument(
        "--notify", action="store_true", help="Gửi webhook khi health lỗi"
    )
    parser.add_argument("--output", type=Path, help="Ghi JSON atomically vào file")
    args = parser.parse_args(argv)

    report = collect_health(scope=args.scope, require_gold=args.require_gold)
    payload = report.to_json()
    print(payload)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        temporary = args.output.with_suffix(args.output.suffix + ".tmp")
        temporary.write_text(payload + "\n", encoding="utf-8")
        temporary.replace(args.output)
    if args.notify:
        _notify(report)
    raise SystemExit(report.exit_code)
