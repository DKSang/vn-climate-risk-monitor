from pathlib import Path

import pytest

from vn_climate_risk_monitor.config import load_settings


def test_pyarrow_is_not_a_direct_dependency() -> None:
    text = Path("pyproject.toml").read_text(encoding="utf-8")
    assert "pyarrow" not in text


def test_archive_model_is_not_configurable(monkeypatch: pytest.MonkeyPatch) -> None:
    """Model archive chọn theo thời kỳ (era5 <2017, ecmwf_ifs >=2017).

    Một biến môi trường sẽ âm thầm ghi đè logic đó và trộn hai lưới vào cùng bảng.
    """
    monkeypatch.setenv("OPEN_METEO_ARCHIVE_MODEL", "era5_land")
    load_settings.cache_clear()

    assert not hasattr(load_settings().open_meteo, "archive_model")

    load_settings.cache_clear()


def test_default_fetch_workers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("OPEN_METEO_FETCH_WORKERS", raising=False)
    load_settings.cache_clear()

    settings = load_settings().open_meteo

    assert settings.fetch_workers == 4
    assert not hasattr(settings, "max_effective_calls_per_hour")
    assert not hasattr(settings, "max_effective_calls_per_day")
    load_settings.cache_clear()


def test_open_meteo_settings_are_loaded_from_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    values = {
        "OPEN_METEO_FORECAST_URL": "https://forecast.example/v1/forecast",
        "OPEN_METEO_FORECAST_HOURS": "72",
        "OPEN_METEO_LOCATION_BATCH_SIZE": "25",
        "OPEN_METEO_FETCH_WORKERS": "8",
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)
    load_settings.cache_clear()

    settings = load_settings().open_meteo

    assert settings.forecast_url == values["OPEN_METEO_FORECAST_URL"]
    assert settings.forecast_hours == 72
    assert settings.location_batch_size == 25
    assert settings.fetch_workers == 8
    load_settings.cache_clear()


def test_open_meteo_settings_reject_invalid_integer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OPEN_METEO_FORECAST_HOURS", "not-an-integer")
    load_settings.cache_clear()

    with pytest.raises(ValueError, match="OPEN_METEO_FORECAST_HOURS"):
        load_settings()

    load_settings.cache_clear()
