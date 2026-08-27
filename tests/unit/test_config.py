from pathlib import Path

import pytest

from vn_climate_risk_monitor.config import load_settings


def test_pyarrow_is_not_a_direct_dependency() -> None:
    text = Path("pyproject.toml").read_text(encoding="utf-8")
    assert "pyarrow" not in text


def test_default_archive_model_is_era5(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("OPEN_METEO_ARCHIVE_MODEL", raising=False)
    load_settings.cache_clear()

    assert load_settings().open_meteo.archive_model == "era5"

    load_settings.cache_clear()


def test_default_open_meteo_hourly_budget(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("OPEN_METEO_MAX_EFFECTIVE_CALLS_PER_HOUR", raising=False)
    load_settings.cache_clear()

    settings = load_settings().open_meteo

    assert settings.max_effective_calls_per_hour == 4500
    assert not hasattr(settings, "max_effective_calls_per_minute")
    assert not hasattr(settings, "request_timeout_seconds")
    assert not hasattr(settings, "max_attempts")
    load_settings.cache_clear()


def test_open_meteo_settings_are_loaded_from_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    values = {
        "OPEN_METEO_FORECAST_URL": "https://forecast.example/v1/forecast",
        "OPEN_METEO_ARCHIVE_MODEL": "era5_land",
        "OPEN_METEO_FORECAST_HOURS": "72",
        "OPEN_METEO_LOCATION_BATCH_SIZE": "25",
        "OPEN_METEO_MAX_EFFECTIVE_CALLS_PER_HOUR": "1200",
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)
    load_settings.cache_clear()

    settings = load_settings().open_meteo

    assert settings.forecast_url == values["OPEN_METEO_FORECAST_URL"]
    assert settings.archive_model == "era5_land"
    assert settings.forecast_hours == 72
    assert settings.location_batch_size == 25
    assert settings.max_effective_calls_per_hour == 1200
    load_settings.cache_clear()


def test_open_meteo_settings_reject_invalid_integer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OPEN_METEO_FORECAST_HOURS", "not-an-integer")
    load_settings.cache_clear()

    with pytest.raises(ValueError, match="OPEN_METEO_FORECAST_HOURS"):
        load_settings()

    load_settings.cache_clear()
