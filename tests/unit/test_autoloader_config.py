"""Code-native source definitions and ingestion boundaries."""

from __future__ import annotations

from pathlib import Path

from vn_climate_risk_monitor import load
from vn_climate_risk_monitor.ingestion import loader as loader_module
from vn_climate_risk_monitor.ingestion import state as state_module
from vn_climate_risk_monitor.ingestion.loader import SourceConfig


def test_source_groups_are_code_native() -> None:
    assert tuple(load.SOURCE_GROUPS) == ("forecast", "archive")
    assert [config.name for config in load.source_configs()] == [
        "open_meteo_forecast",
        "open_meteo_archive",
        "open_meteo_ifs",
    ]
    assert not list(Path("sources").glob("*.yml"))


def test_source_definitions_preserve_physical_targets() -> None:
    configs = load.source_configs()
    assert configs[0].target == "catalog1.silver.stg_weather_forecast"
    assert {config.target for config in configs[1:]} == {
        "catalog1.silver.stg_weather_archive_hourly"
    }
    assert configs[0].sql_file == "open_meteo_forecast.sql"
    assert {config.sql_file for config in configs[1:]} == {
        "open_meteo_archive_hourly.sql"
    }


def test_source_sql_is_read_from_its_explicit_base_dir(tmp_path: Path) -> None:
    (tmp_path / "source.sql").write_text(
        "SELECT * FROM read_json_auto({{ files }})", encoding="utf-8"
    )
    config = SourceConfig(
        name="source",
        dataset="data",
        prefix="bronze/files/source",
        pattern="**/*.json",
        sql_file="source.sql",
        target="catalog1.silver.stg_source",
        batch_size=1,
        parameters={},
        base_dir=tmp_path,
    )
    assert "{{ files }}" in config.sql


def test_ingestion_modules_own_their_boundaries_without_barrel_exports() -> None:
    assert hasattr(loader_module, "SourceConfig")
    assert hasattr(loader_module, "AutoLoader")
    assert hasattr(state_module, "RunAttempt")
    assert hasattr(state_module, "ClaimedObject")
    assert hasattr(state_module, "ensure_ingestion_state")


def test_ingestion_state_uses_file_parameters() -> None:
    create_files = next(
        statement
        for statement in state_module.SCHEMA_STATEMENTS
        if "CREATE TABLE IF NOT EXISTS ingestion.ingestion_files" in statement
    )
    assert "file_parameters JSONB" in create_files
