"""Code-native processing definitions."""

from __future__ import annotations

import tomllib
from datetime import timedelta
from pathlib import Path

import pytest

from vn_climate_risk_monitor.processing.config import (
    DEFAULT_SAFETY_LAG,
    ProcessConfig,
    SourceBinding,
    load_active_config,
    load_active_configs,
)


def test_process_config_defaults() -> None:
    config = ProcessConfig("p", "gold.t", (SourceBinding("archive_hourly"),))
    assert config.scope == "production"
    assert config.safety_lag == DEFAULT_SAFETY_LAG
    assert config.source_refs == ("archive_hourly",)


def test_duplicate_source_ref_is_rejected() -> None:
    with pytest.raises(ValueError, match="bị trùng"):
        ProcessConfig("p", "gold.t", (SourceBinding("a"), SourceBinding("a")))


def test_negative_safety_lag_is_rejected() -> None:
    with pytest.raises(ValueError, match="safety_lag"):
        ProcessConfig(
            "p", "gold.t", (SourceBinding("a"),), safety_lag=timedelta(seconds=-1)
        )


def test_active_processes_are_code_native() -> None:
    configs = load_active_configs()
    assert tuple(configs) == ("archive", "forecast")
    assert configs["forecast"].source_refs == ("stg_weather_forecast",)
    assert configs["archive"].source_refs == ("stg_weather_archive_hourly",)
    assert load_active_config("forecast") is configs["forecast"]
    assert not list(Path("processing").glob("*.yml"))


def test_unknown_process_is_rejected() -> None:
    with pytest.raises(ValueError, match="Unknown active process"):
        load_active_config("retired")


def test_auto_process_is_the_console_entrypoint() -> None:
    project = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))
    assert (
        project["project"]["scripts"]["auto-process"]
        == "vn_climate_risk_monitor.processing.cli:main"
    )
