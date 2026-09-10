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


def test_forecast_pressure_alert_uses_forward_windows_and_current_run() -> None:
    forecast = _sql("marts/fct_rain_forecast_hourly.sql")
    alert = _sql("marts/fct_rain_pressure_alert.sql")

    assert "forward_rain_sums" in forecast
    assert "forward_rain_columns" in forecast
    assert "fct_rain_forecast_current_hourly" in alert
    assert "persistence_runs" in alert
    assert "pressure_level" in alert
    assert "HIGH" in alert and "ELEVATED" in alert and "WATCH" in alert
    assert "coverage_status" in alert
    assert "THEN 'UNKNOWN'" in alert
    assert "incomplete_forecast_coverage" in alert


def test_legacy_forecast_risk_serving_models_are_removed() -> None:
    marts = MODELS / "marts"
    schema = _sql("marts/schema.yml")
    queries = Path("serving/dashboard/queries.py").read_text(encoding="utf-8")

    assert not (marts / "bridge_flood_point_grid.sql").exists()
    assert not (marts / "fct_flood_risk_score.sql").exists()
    assert "name: bridge_flood_point_grid" not in schema
    assert "name: fct_flood_risk_score" not in schema
    assert "gold.fct_flood_risk_score" not in queries
    assert "def load_forecast_risk_ranking" not in queries


def test_unused_gold_research_and_daily_models_are_removed() -> None:
    marts = MODELS / "marts"
    schema = _sql("marts/schema.yml")
    removed = {
        "fct_rain_archive_daily",
        "fct_ward_rain_archive_daily",
        "fct_rain_climatology",
        "fct_flood_training_feature",
        "fct_flood_backtest_metric",
    }

    for model in removed:
        assert not (marts / f"{model}.sql").exists()
        assert f"name: {model}" not in schema


def test_serving_uses_successful_processing_publication_not_catalog_head() -> None:
    queries = Path("serving/dashboard/queries.py").read_text(encoding="utf-8")
    runner = Path("scripts/run_processing.py").read_text(encoding="utf-8")
    state = Path("src/processing/state.py").read_text(encoding="utf-8")

    assert "published_snapshot_id" in queries
    assert "status = 'SUCCEEDED'" in queries
    assert "snapshot.snapshot_id <= catalog_snapshot.snapshot_id" in queries
    assert "SELECT MAX(snapshot_id)" in runner
    assert "published_snapshot_id = %s" in state
