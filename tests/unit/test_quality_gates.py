"""Runtime data-quality gates backed by Provero."""

from __future__ import annotations

from collections.abc import Callable

import duckdb
import pytest

from vn_climate_risk_monitor.quality.gates import (
    DuckLakeConnector,
    QualityGateError,
    build_ingest_suite,
    build_silver_suite,
    committed_source_files,
    run_suite_or_raise,
)


def connector_with(*statements: str) -> DuckLakeConnector:
    connection = duckdb.connect()
    connection.execute("ATTACH ':memory:' AS catalog1")
    connection.execute("CREATE SCHEMA catalog1.silver")
    for statement in statements:
        connection.execute(statement)
    factory: Callable[..., duckdb.DuckDBPyConnection] = lambda **_: connection
    return DuckLakeConnector(factory)


def test_ingest_requires_every_registered_file_to_be_committed() -> None:
    with pytest.raises(QualityGateError, match="FAILED"):
        committed_source_files(
            [("COMMITTED", "bronze/good.json"), ("FAILED", "bronze/bad.json")],
            bucket="lake",
        )


def test_ingest_rejects_duplicate_grain_within_one_source_file() -> None:
    source_file = "s3://lake/bronze/forecast.json"
    connector = connector_with(
        f"""
        CREATE TABLE catalog1.silver.stg_weather_forecast AS
        SELECT
            21.0::DOUBLE AS grid_latitude,
            105.8::DOUBLE AS grid_longitude,
            10.0::DOUBLE AS elevation_m,
            TIMESTAMPTZ '2026-09-14 00:00:00+00' AS valid_time_utc,
            1.0::DOUBLE AS precipitation_mm,
            1.0::DOUBLE AS rain_mm,
            0.0::DOUBLE AS showers_mm,
            50::INTEGER AS precipitation_probability_pct,
            61::INTEGER AS weather_code,
            'UTC'::VARCHAR AS timezone,
            0::INTEGER AS utc_offset_seconds,
            '{{}}'::VARCHAR AS hourly_units_json,
            '{source_file}'::VARCHAR AS _source_file,
            TIMESTAMPTZ '2026-09-14 00:01:00+00' AS _ingested_at,
            NULL::VARCHAR AS _rescued_data
        FROM range(2)
        """
    )

    with pytest.raises(QualityGateError, match="quality_ingest_forecast"):
        run_suite_or_raise(
            build_ingest_suite("forecast", (source_file,)), connector=connector
        )


def test_ingest_rejects_non_timestamp_ingestion_clock() -> None:
    source_file = "s3://lake/bronze/forecast.json"
    connector = connector_with(
        f"""
        CREATE TABLE catalog1.silver.stg_weather_forecast AS
        SELECT
            21.0::DOUBLE AS grid_latitude,
            105.8::DOUBLE AS grid_longitude,
            TIMESTAMPTZ '2026-09-14 00:00:00+00' AS valid_time_utc,
            1.0::DOUBLE AS precipitation_mm,
            1.0::DOUBLE AS rain_mm,
            0.0::DOUBLE AS showers_mm,
            50::INTEGER AS precipitation_probability_pct,
            '{source_file}'::VARCHAR AS _source_file,
            'not-a-timestamp'::VARCHAR AS _ingested_at,
            NULL::VARCHAR AS _rescued_data
        """
    )

    with pytest.raises(QualityGateError, match="quality_ingest_forecast"):
        run_suite_or_raise(
            build_ingest_suite("forecast", (source_file,)), connector=connector
        )


def test_silver_forecast_requires_exactly_126_cells_by_72_hours() -> None:
    connector = connector_with(
        """
        CREATE TABLE catalog1.silver.stg_weather_forecast AS
        SELECT
            '/incremental/2026/09/14/00/run_20260914T000000/response.json'
                AS _source_file,
            TIMESTAMPTZ '2026-09-14 00:00:00+00' AS _ingested_at
        """,
        """
        CREATE TABLE catalog1.silver.int_weather_forecast_hourly AS
        SELECT
            'run_20260914T000000'::VARCHAR AS forecast_run_id,
            'cell-' || cell::VARCHAR AS grid_cell_id,
            TIMESTAMPTZ '2026-09-14 00:00:00+00' + hour * INTERVAL '1 hour'
                AS valid_time_utc,
            1.0::DOUBLE AS precipitation_mm,
            1.0::DOUBLE AS rain_mm,
            0.0::DOUBLE AS showers_mm,
            50::INTEGER AS precipitation_probability_pct
        FROM range(125) AS cells(cell)
        CROSS JOIN range(72) AS hours(hour)
        """,
    )

    with pytest.raises(QualityGateError, match="quality_silver_int_forecast"):
        run_suite_or_raise(build_silver_suite("forecast"), connector=connector)


def test_silver_forecast_rejects_source_path_without_run_id() -> None:
    connector = connector_with(
        """
        CREATE TABLE catalog1.silver.stg_weather_forecast AS
        SELECT '/forecast/malformed/response.json' AS _source_file,
               TIMESTAMPTZ '2026-09-14 00:01:00+00' AS _ingested_at
        """,
        """
        CREATE TABLE catalog1.silver.int_weather_forecast_hourly AS
        SELECT
            'run_20260914T000000'::VARCHAR AS forecast_run_id,
            'cell-' || cell::VARCHAR AS grid_cell_id,
            TIMESTAMPTZ '2026-09-14 00:00:00+00' + hour * INTERVAL '1 hour'
                AS valid_time_utc,
            1.0::DOUBLE AS precipitation_mm,
            1.0::DOUBLE AS rain_mm,
            0.0::DOUBLE AS showers_mm,
            50::INTEGER AS precipitation_probability_pct
        FROM range(126) AS cells(cell)
        CROSS JOIN range(72) AS hours(hour)
        """,
    )

    with pytest.raises(QualityGateError, match="quality_silver_int_forecast"):
        run_suite_or_raise(build_silver_suite("forecast"), connector=connector)


def test_silver_archive_rejects_duplicate_business_grain() -> None:
    connector = connector_with(
        """
        CREATE TABLE catalog1.silver.int_weather_archive_hourly AS
        SELECT
            'era5'::VARCHAR AS weather_model,
            'cell-1'::VARCHAR AS grid_cell_id,
            TIMESTAMPTZ '2026-09-14 00:00:00+00' AS valid_time_utc,
            1.0::DOUBLE AS precipitation_mm,
            1.0::DOUBLE AS rain_mm
        FROM range(2)
        """
    )

    with pytest.raises(QualityGateError, match="quality_silver_int_archive"):
        run_suite_or_raise(build_silver_suite("archive"), connector=connector)
