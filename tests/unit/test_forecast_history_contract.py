"""Contract tĩnh cho forecast history và current serving view."""

from __future__ import annotations

from pathlib import Path

MODELS = Path("transform/models")


def _sql(relative_path: str) -> str:
    return (MODELS / relative_path).read_text(encoding="utf-8")


def test_forecast_history_keys_include_retrieval_run() -> None:
    intermediate = _sql("intermediate/int_weather_forecast_hourly.sql")
    mart = _sql("marts/fct_rain_forecast_hourly.sql")

    assert "forecast_run_id" in intermediate
    assert "forecast_run_id" in mart
    assert "partition_by='forecast_run_id, grid_cell_id'" in mart
    assert "DELETE FROM" not in intermediate.upper()
    assert "DELETE FROM" not in mart.upper()


def test_current_view_is_separate_from_history_table() -> None:
    current_view = _sql("marts/fct_rain_forecast_current_hourly.sql")

    assert "materialized = 'view'" in current_view
    assert "ref('fct_rain_forecast_hourly')" in current_view
    assert "DATE_TRUNC('hour', CURRENT_TIMESTAMP)" in current_view
