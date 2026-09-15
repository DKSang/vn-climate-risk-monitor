"""Runtime data-quality gates backed by Provero."""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, date, datetime
from pathlib import Path

import duckdb
import pytest

from vn_climate_risk_monitor.quality.gates import (
    DuckLakeConnector,
    QualityGateError,
    build_raw_suite,
    build_silver_suite,
    run_suite_or_raise,
    select_raw_keys,
)


def connector_with(*statements: str) -> DuckLakeConnector:
    connection = duckdb.connect()
    connection.execute("ATTACH ':memory:' AS catalog1")
    connection.execute("CREATE SCHEMA catalog1.silver")
    for statement in statements:
        connection.execute(statement)
    factory: Callable[..., duckdb.DuckDBPyConnection] = lambda **_: connection
    return DuckLakeConnector(factory)


def _write_forecast(path: Path, times: list[int | None]) -> str:
    hourly = {
        "time": times,
        "precipitation": [1.0] * len(times),
        "rain": [1.0] * len(times),
        "showers": [0.0] * len(times),
        "precipitation_probability": [50] * len(times),
        "weather_code": [61] * len(times),
    }
    path.write_text(
        json.dumps({
            "latitude": 21.0,
            "longitude": 105.8,
            "elevation": 10.0,
            "timezone": "UTC",
            "utc_offset_seconds": 0,
            "hourly_units": {name: "unit" for name in hourly},
            "hourly": hourly,
        }),
        encoding="utf-8",
    )
    return path.as_posix()


def test_raw_forecast_accepts_valid_landed_json(tmp_path: Path) -> None:
    source = _write_forecast(tmp_path / "response_000.json", [1_789_344_000])

    run_suite_or_raise(build_raw_suite("forecast", (source,)), connector=connector_with())


def test_raw_forecast_rejects_duplicate_grain_within_file(tmp_path: Path) -> None:
    source = _write_forecast(
        tmp_path / "response_000.json", [1_789_344_000, 1_789_344_000]
    )

    with pytest.raises(QualityGateError, match="quality_raw_forecast"):
        run_suite_or_raise(
            build_raw_suite("forecast", (source,)), connector=connector_with()
        )


def test_raw_forecast_rejects_missing_loader_schema_field(tmp_path: Path) -> None:
    source = Path(_write_forecast(tmp_path / "response_000.json", [1_789_344_000]))
    payload = json.loads(source.read_text(encoding="utf-8"))
    del payload["timezone"]
    source.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises((QualityGateError, duckdb.Error)):
        run_suite_or_raise(
            build_raw_suite("forecast", (source.as_posix(),)),
            connector=connector_with(),
        )


def test_raw_forecast_rejects_null_timestamp(tmp_path: Path) -> None:
    source = _write_forecast(
        tmp_path / "response_000.json", [1_789_344_000, None]
    )

    with pytest.raises(QualityGateError, match="quality_raw_forecast"):
        run_suite_or_raise(
            build_raw_suite("forecast", (source,)), connector=connector_with()
        )


def test_raw_forecast_rejects_uncastable_weather_code(tmp_path: Path) -> None:
    source = Path(_write_forecast(tmp_path / "response_000.json", [1_789_344_000]))
    payload = json.loads(source.read_text(encoding="utf-8"))
    payload["hourly"]["weather_code"] = ["bad"]
    source.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(QualityGateError, match="quality_raw_forecast"):
        run_suite_or_raise(
            build_raw_suite("forecast", (source.as_posix(),)),
            connector=connector_with(),
        )


@pytest.mark.parametrize(
    ("section", "field"),
    (("hourly", "precipitation_probability"), (None, "utc_offset_seconds")),
)
def test_raw_forecast_rejects_fractional_integer_fields(
    tmp_path: Path, section: str | None, field: str
) -> None:
    source = Path(_write_forecast(tmp_path / "response_000.json", [1_789_344_000]))
    payload = json.loads(source.read_text(encoding="utf-8"))
    target = payload if section is None else payload[section]
    target[field] = 0.5 if section is None else [50.5]
    source.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(QualityGateError, match="quality_raw_forecast"):
        run_suite_or_raise(
            build_raw_suite("forecast", (source.as_posix(),)),
            connector=connector_with(),
        )


def test_raw_archive_rejects_timestamp_outside_requested_month(tmp_path: Path) -> None:
    source = _write_forecast(tmp_path / "response_000.json", [1_788_134_400])
    payload = json.loads(Path(source).read_text(encoding="utf-8"))
    payload["hourly"]["soil_moisture_0_to_7cm"] = [0.2]
    payload["hourly"]["soil_moisture_7_to_28cm"] = [0.3]
    Path(source).write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(QualityGateError, match="quality_raw_archive"):
        run_suite_or_raise(
            build_raw_suite(
                "archive",
                (source,),
                window_start=datetime(2026, 9, 1, tzinfo=UTC),
                window_end=datetime(2026, 10, 1, tzinfo=UTC),
            ),
            connector=connector_with(),
        )


def test_raw_forecast_selects_only_latest_landed_run() -> None:
    old = (
        "bronze/files/open_meteo/forecast/incremental/2026/09/14/00/"
        "run_20260914T000000/response_000.json"
    )
    latest = (
        "bronze/files/open_meteo/forecast/incremental/2026/09/14/01/"
        "run_20260914T010000/response_000.json"
    )

    assert select_raw_keys("forecast", (latest, old), month=None) == (latest,)


def test_raw_archive_selects_only_requested_month() -> None:
    june = (
        "bronze/files/open_meteo/historical_weather_hourly/ifs/year=2026/month=06/"
        "run_20260701T000000/response_000.json"
    )
    july = june.replace("month=06", "month=07")

    assert select_raw_keys(
        "archive", (july, june), month=date(2026, 6, 1)
    ) == (june,)


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
