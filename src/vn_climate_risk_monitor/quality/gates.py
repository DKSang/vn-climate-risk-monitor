"""Fail-closed Provero gates for raw landing and curated Silver."""

from __future__ import annotations

import argparse
from collections.abc import Callable, Iterable, Sequence
from datetime import UTC, date, datetime, timedelta
from pathlib import PurePosixPath
from typing import Any

from provero.connectors.duckdb import DuckDBConnection, DuckDBConnector
from provero.core.compiler import CheckConfig, SourceConfig, SuiteConfig
from provero.core.engine import run_suite
from provero.core.results import Status

from vn_climate_risk_monitor.platform.lakehouse import get_connection
from vn_climate_risk_monitor.platform.minio import get_minio_client
from vn_climate_risk_monitor.platform.settings import load_settings
from vn_climate_risk_monitor.sources.open_meteo.planner import (
    FORECAST_PREFIX,
    forecast_run_id,
    model_for_month,
    month_params,
    slot_params,
)

BLOCKER = "blocker"
PROCESS_KEYS = ("forecast", "archive")


class QualityGateError(RuntimeError):
    """A blocking runtime quality rule failed."""


class DuckLakeConnector(DuckDBConnector):
    """Use Provero's DuckDB checks on the project's attached DuckLake catalog."""

    def __init__(self, factory: Callable[..., Any] = get_connection) -> None:
        self.factory = factory

    def connect(self) -> DuckDBConnection:
        return DuckDBConnection(self.factory(read_only=True))


def _check(
    check_type: str,
    *,
    column: str | None = None,
    columns: Iterable[str] = (),
    **params: Any,
) -> CheckConfig:
    return CheckConfig(
        check_type=check_type,
        column=column,
        columns=list(columns),
        params=params,
        severity=BLOCKER,
    )


def _custom(name: str, query: str) -> CheckConfig:
    return _check("custom_sql", name=name, query=query)


def _raw_source(files: Sequence[str]) -> str:
    if not files:
        raise QualityGateError("raw landing contains no response files")
    quoted = ", ".join(f"'{file.replace("'", "''")}'" for file in files)
    return (
        f"read_json_auto([{quoted}], filename=true, union_by_name=true, "
        "maximum_object_size=209715200)"
    )


def select_raw_keys(
    process_key: str, keys: Sequence[str], *, month: date | None
) -> tuple[str, ...]:
    responses = sorted(
        key for key in keys if PurePosixPath(key).name.startswith("response_")
    )
    if process_key == "archive":
        if month is None:
            raise QualityGateError("--month is required for the archive raw gate")
        prefix = month_params(month, model_for_month(month))[0] + "/"
        return tuple(key for key in responses if key.startswith(prefix))

    runs = {key.rsplit("/", 1)[0] for key in responses if key.startswith(FORECAST_PREFIX)}
    if not runs:
        return ()
    latest = max(runs)
    return tuple(key for key in responses if key.startswith(latest + "/"))


def build_raw_suite(
    process_key: str,
    files: Sequence[str],
    *,
    window_start: datetime | None = None,
    window_end: datetime | None = None,
) -> SuiteConfig:
    source = _raw_source(files)
    hourly_fields = ["precipitation", "rain", "weather_code"]
    if process_key == "forecast":
        hourly_fields += ["showers", "precipitation_probability"]
    else:
        hourly_fields += ["soil_moisture_0_to_7cm", "soil_moisture_7_to_28cm"]
    unnested = ",\n".join(
        f"UNNEST(hourly.{field}) AS {field}" for field in hourly_fields
    )
    valid = [
        "time IS NOT NULL",
        "TRY_CAST(time AS DOUBLE) = TRY_CAST(time AS BIGINT)",
        "TRY_CAST(precipitation AS DOUBLE) BETWEEN 0 AND 500",
        "TRY_CAST(rain AS DOUBLE) BETWEEN 0 AND 500",
        "TRY_CAST(weather_code AS DOUBLE) = TRY_CAST(weather_code AS INTEGER)",
        *(f"{field} IS NOT NULL" for field in hourly_fields),
    ]
    if process_key == "forecast":
        valid += [
            "TRY_CAST(showers AS DOUBLE) BETWEEN 0 AND 500",
            "TRY_CAST(precipitation_probability AS INTEGER) BETWEEN 0 AND 100",
            (
                "TRY_CAST(precipitation_probability AS DOUBLE) "
                "= TRY_CAST(precipitation_probability AS INTEGER)"
            ),
        ]
    else:
        valid += [
            "TRY_CAST(soil_moisture_0_to_7cm AS DOUBLE) IS NOT NULL",
            "TRY_CAST(soil_moisture_7_to_28cm AS DOUBLE) IS NOT NULL",
        ]
    if window_start and window_end:
        valid += [
            f"time >= {int(window_start.timestamp())}",
            f"time < {int(window_end.timestamp())}",
        ]
    required = (
        "latitude",
        "longitude",
        "elevation",
        "timezone",
        "utc_offset_seconds",
        "hourly_units",
        "hourly",
        "filename",
    )
    checks = [
        _check("row_count", min=1),
        _check("not_null", columns=required),
        _check("range", column="latitude", min=-90, max=90),
        _check("range", column="longitude", min=-180, max=180),
        _custom(
            "raw_hourly_contract",
            f"""
            WITH exploded AS (
                SELECT filename, latitude, longitude, elevation, timezone,
                       utc_offset_seconds, hourly_units,
                       UNNEST(hourly.time) AS time,
                       {unnested}
                FROM {source}
            )
            SELECT COUNT(*) > 0
               AND COUNT(DISTINCT filename) = {len(files)}
               AND COUNT_IF(
                   TRY_CAST(latitude AS DOUBLE) IS NULL
                   OR TRY_CAST(longitude AS DOUBLE) IS NULL
                   OR TRY_CAST(elevation AS DOUBLE) IS NULL
                   OR NULLIF(timezone, '') IS NULL
                   OR TRY_CAST(utc_offset_seconds AS INTEGER) IS NULL
                   OR TRY_CAST(utc_offset_seconds AS DOUBLE)
                      <> TRY_CAST(utc_offset_seconds AS INTEGER)
                   OR hourly_units IS NULL
               ) = 0
               AND COUNT_IF(COALESCE(NOT ({' AND '.join(valid)}), TRUE)) = 0
               AND COUNT(*) = COUNT(DISTINCT (filename, latitude, longitude, time))
            FROM exploded
            """,
        ),
    ]
    return SuiteConfig(
        name=f"quality_raw_{process_key}",
        source=SourceConfig(type="duckdb", table=source),
        checks=checks,
    )


def build_silver_suite(process_key: str) -> SuiteConfig:
    table = f"catalog1.silver.int_weather_{process_key}_hourly"
    rainfall = ("precipitation_mm", "rain_mm")
    checks = [
        _check("row_count", min=1),
        _check("not_null", columns=("grid_cell_id", "valid_time_utc", *rainfall)),
        *(_check("range", column=column, min=0, max=500) for column in rainfall),
    ]
    if process_key == "forecast":
        checks.extend(
            (
                _check("not_null", columns=("forecast_run_id", "showers_mm")),
                _check("range", column="showers_mm", min=0, max=500),
                _check("range", column="precipitation_probability_pct", min=0, max=100),
                _check(
                    "unique_combination",
                    columns=("forecast_run_id", "grid_cell_id", "valid_time_utc"),
                ),
                _custom(
                    "forecast_complete_126x72",
                    """
                    WITH staged AS (
                        SELECT DISTINCT REGEXP_EXTRACT(
                            _source_file,
                            '/incremental/[0-9]{4}/[0-9]{2}/[0-9]{2}/[0-9]{2}/(run_[0-9]{8}T[0-9]{6})/',
                            1
                        ) AS forecast_run_id
                        FROM catalog1.silver.stg_weather_forecast
                    ),
                    curated AS (
                        SELECT forecast_run_id, COUNT(*) AS rows,
                               COUNT(DISTINCT grid_cell_id) AS cells,
                               COUNT(DISTINCT valid_time_utc) AS hours
                        FROM catalog1.silver.int_weather_forecast_hourly
                        GROUP BY forecast_run_id
                    )
                    SELECT COUNT(*) = 0
                    FROM staged
                    LEFT JOIN curated USING (forecast_run_id)
                    WHERE curated.forecast_run_id IS NULL
                       OR rows <> 126 * 72 OR cells <> 126 OR hours <> 72
                    """,
                ),
            )
        )
    else:
        checks.extend(
            (
                _check("not_null", column="weather_model"),
                _check(
                    "unique_combination",
                    columns=("weather_model", "grid_cell_id", "valid_time_utc"),
                ),
            )
        )
    return SuiteConfig(
        name=f"quality_silver_int_{process_key}",
        source=SourceConfig(type="duckdb", table=table),
        checks=checks,
    )


def run_suite_or_raise(
    suite: SuiteConfig, *, connector: DuckDBConnector | None = None
) -> None:
    result = run_suite(suite, connector or DuckLakeConnector())
    print(
        f"{suite.name}: {result.status.value} "
        f"({result.passed}/{result.total}, score={result.quality_score})"
    )
    if result.status != Status.PASS:
        failed = ", ".join(
            check.check_name for check in result.checks if check.status != Status.PASS
        )
        raise QualityGateError(f"{suite.name} failed: {failed}")


def run_raw_gate(
    process_key: str,
    *,
    month: date | None = None,
    slot: datetime | None = None,
) -> None:
    settings = load_settings()
    if process_key == "forecast":
        slot = (slot or datetime.now(UTC)).astimezone(UTC).replace(
            minute=0, second=0, microsecond=0
        )
        prefix = f"{slot_params(slot, settings.open_meteo)[0]}/{forecast_run_id(slot)}"
        window_start = slot
        window_end = slot + timedelta(hours=settings.open_meteo.forecast_hours)
    else:
        if month is None:
            raise QualityGateError("--month is required for the archive raw gate")
        prefix = month_params(month, model_for_month(month))[0]
        window_start = datetime(month.year, month.month, 1, tzinfo=UTC)
        window_end = (
            datetime(month.year + 1, 1, 1, tzinfo=UTC)
            if month.month == 12
            else datetime(month.year, month.month + 1, 1, tzinfo=UTC)
        )
    client = get_minio_client(settings.minio)
    keys = tuple(
        obj.object_name
        for obj in client.list_objects(
            settings.minio.bucket, prefix=prefix + "/", recursive=True
        )
        if obj.object_name
    )
    selected = select_raw_keys(process_key, keys, month=month)
    files = tuple(f"s3://{settings.minio.bucket}/{key}" for key in selected)
    run_suite_or_raise(
        build_raw_suite(
            process_key, files, window_start=window_start, window_end=window_end
        )
    )


def run_silver_gate(process_key: str) -> None:
    run_suite_or_raise(build_silver_suite(process_key))


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("gate", choices=("raw", "silver-int"))
    parser.add_argument("process_key", choices=PROCESS_KEYS)
    parser.add_argument("--month", type=date.fromisoformat)
    parser.add_argument("--slot", type=datetime.fromisoformat)
    args = parser.parse_args(argv)
    if args.gate == "raw":
        run_raw_gate(args.process_key, month=args.month, slot=args.slot)
    else:
        run_silver_gate(args.process_key)


if __name__ == "__main__":
    main()
