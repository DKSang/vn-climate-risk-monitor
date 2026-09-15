"""Fail-closed Provero gates for staging and curated Silver."""

from __future__ import annotations

import argparse
from collections.abc import Callable, Iterable, Sequence
from typing import Any

from provero.connectors.duckdb import DuckDBConnection, DuckDBConnector
from provero.core.compiler import CheckConfig, SourceConfig, SuiteConfig
from provero.core.engine import run_suite
from provero.core.results import Status

from vn_climate_risk_monitor.auto_loader.config import SOURCE_GROUPS
from vn_climate_risk_monitor.auto_loader.state import connect_control_plane
from vn_climate_risk_monitor.platform.lakehouse import get_connection
from vn_climate_risk_monitor.platform.settings import load_settings

BLOCKER = "blocker"
TABLES = {
    "forecast": "catalog1.silver.stg_weather_forecast",
    "archive": "catalog1.silver.stg_weather_archive_hourly",
}


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


def _source_files_sql(files: Sequence[str]) -> str:
    if not files:
        return "SELECT NULL::VARCHAR AS source_file WHERE FALSE"
    values = ", ".join(f"('{value.replace("'", "''")}')" for value in files)
    return f"SELECT source_file FROM (VALUES {values}) AS files(source_file)"


def committed_source_files(
    rows: Sequence[tuple[str, str]], *, bucket: str
) -> tuple[str, ...]:
    bad = sorted({status for status, _ in rows if status != "COMMITTED"})
    if bad:
        raise QualityGateError(f"ingestion files are not COMMITTED: {', '.join(bad)}")
    return tuple(f"s3://{bucket}/{object_key}" for _, object_key in rows)


def build_ingest_suite(process_key: str, committed_files: Sequence[str]) -> SuiteConfig:
    table = TABLES[process_key]
    common_not_null = (
        "grid_latitude",
        "grid_longitude",
        "valid_time_utc",
        "precipitation_mm",
        "rain_mm",
        "_source_file",
        "_ingested_at",
    )
    checks = [
        _check("row_count", min=1),
        _check("not_null", columns=common_not_null),
        _check("type", column="grid_latitude", expected="float"),
        _check("type", column="grid_longitude", expected="float"),
        _check("type", column="valid_time_utc", expected="timestamp"),
        _check("type", column="_ingested_at", expected="timestamp"),
        _check("range", column="grid_latitude", min=-90, max=90),
        _check("range", column="grid_longitude", min=-180, max=180),
        _check("range", column="precipitation_mm", min=0, max=500),
        _check("range", column="rain_mm", min=0, max=500),
        _custom(
            "rescued_data_empty",
            f"SELECT COUNT(*) = 0 FROM {table} WHERE _rescued_data IS NOT NULL",
        ),
        _custom(
            "source_file_matches_committed_ledger",
            f"""
            WITH committed AS ({_source_files_sql(committed_files)})
            SELECT
                (SELECT COUNT(*) FROM committed)
                    = (SELECT COUNT(DISTINCT _source_file) FROM {table})
                AND NOT EXISTS (
                    SELECT 1 FROM committed
                    LEFT JOIN {table} AS staged ON staged._source_file = committed.source_file
                    WHERE staged._source_file IS NULL
                )
            """,
        ),
    ]
    if process_key == "forecast":
        checks.extend(
            (
                _check(
                    "not_null", columns=("showers_mm", "precipitation_probability_pct")
                ),
                _check("range", column="showers_mm", min=0, max=500),
                _check("range", column="precipitation_probability_pct", min=0, max=100),
                _check(
                    "unique_combination",
                    columns=(
                        "_source_file",
                        "grid_latitude",
                        "grid_longitude",
                        "valid_time_utc",
                    ),
                ),
            )
        )
    else:
        checks.extend(
            (
                _check("not_null", column="weather_model"),
                _check(
                    "accepted_values",
                    column="weather_model",
                    values=("era5", "ecmwf_ifs"),
                ),
                _check(
                    "unique_combination",
                    columns=(
                        "_source_file",
                        "weather_model",
                        "grid_latitude",
                        "grid_longitude",
                        "valid_time_utc",
                    ),
                ),
            )
        )
    return SuiteConfig(
        name=f"quality_ingest_{process_key}",
        source=SourceConfig(type="duckdb", table=table),
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


def run_ingest_gate(process_key: str) -> None:
    settings = load_settings()
    control = connect_control_plane(settings.postgres.ducklake_connection_string)
    try:
        pipelines = [config.name for config in SOURCE_GROUPS[process_key]]
        rows = control.execute(
            """
            SELECT file.status, file.object_key
            FROM ingestion.ingestion_files AS file
            JOIN ingestion.ingestion_runs AS run USING (attempt_id)
            WHERE run.pipeline_name = ANY(%s) AND run.scope = 'production'
            ORDER BY file.object_key
            """,
            (pipelines,),
        ).fetchall()
    finally:
        control.close()
    files = committed_source_files(rows, bucket=settings.minio.bucket)
    run_suite_or_raise(build_ingest_suite(process_key, files))


def run_silver_gate(process_key: str) -> None:
    run_suite_or_raise(build_silver_suite(process_key))


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("gate", choices=("ingest", "silver-int"))
    parser.add_argument("process_key", choices=tuple(TABLES))
    args = parser.parse_args(argv)
    if args.gate == "ingest":
        run_ingest_gate(args.process_key)
    else:
        run_silver_gate(args.process_key)


if __name__ == "__main__":
    main()
