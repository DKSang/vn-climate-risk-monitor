import pytest

from vn_climate_risk_monitor.config import load_settings


def test_default_archive_model_is_era5(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("OPEN_METEO_ARCHIVE_MODEL", raising=False)
    load_settings.cache_clear()

    assert load_settings().open_meteo.archive_model == "era5"

    load_settings.cache_clear()


def test_default_open_meteo_rate_budgets_keep_minute_and_hour_headroom(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name in (
        "OPEN_METEO_MAX_EFFECTIVE_CALLS_PER_MINUTE",
        "OPEN_METEO_MAX_EFFECTIVE_CALLS_PER_HOUR",
    ):
        monkeypatch.delenv(name, raising=False)
    load_settings.cache_clear()

    settings = load_settings().open_meteo

    assert settings.max_effective_calls_per_minute == 500
    assert settings.max_effective_calls_per_hour == 4500
    load_settings.cache_clear()


def test_open_meteo_settings_are_loaded_from_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    values = {
        "OPEN_METEO_FORECAST_URL": "https://forecast.example/v1/forecast",
        "OPEN_METEO_ARCHIVE_MODEL": "era5_land",
        "OPEN_METEO_FORECAST_HOURS": "72",
        "OPEN_METEO_LOCATION_BATCH_SIZE": "25",
        "OPEN_METEO_REQUEST_TIMEOUT_SECONDS": "60",
        "OPEN_METEO_MAX_ATTEMPTS": "5",
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)
    load_settings.cache_clear()

    settings = load_settings().open_meteo

    assert settings.forecast_url == values["OPEN_METEO_FORECAST_URL"]
    assert settings.archive_model == "era5_land"
    assert settings.forecast_hours == 72
    assert settings.location_batch_size == 25
    assert settings.max_attempts == 5
    load_settings.cache_clear()


def test_open_meteo_settings_reject_invalid_integer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OPEN_METEO_FORECAST_HOURS", "not-an-integer")
    load_settings.cache_clear()

    with pytest.raises(ValueError, match="OPEN_METEO_FORECAST_HOURS"):
        load_settings()

    load_settings.cache_clear()
