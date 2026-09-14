from pathlib import Path

import pytest

from vn_climate_risk_monitor.config import load_settings


@pytest.fixture(autouse=True)
def runtime_secrets(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_PASSWORD", "test-postgres")
    monkeypatch.setenv("MINIO_ACCESS_KEY", "test-minio-user")
    monkeypatch.setenv("MINIO_SECRET_KEY", "test-minio")
    monkeypatch.delenv("POSTGRES_PASSWORD_FILE", raising=False)
    monkeypatch.delenv("MINIO_SECRET_KEY_FILE", raising=False)
    load_settings.cache_clear()
    yield
    load_settings.cache_clear()


def test_pyarrow_is_not_a_direct_dependency() -> None:
    text = Path("pyproject.toml").read_text(encoding="utf-8")
    assert "pyarrow" not in text


def test_archive_model_is_not_configurable(monkeypatch: pytest.MonkeyPatch) -> None:
    """Archive model selection is fixed by historical period."""
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


def test_secrets_can_be_loaded_from_files(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    postgres = tmp_path / "postgres_password"
    minio = tmp_path / "minio_secret_key"
    postgres.write_text("postgres-from-file\n", encoding="utf-8")
    minio.write_text("minio-from-file\n", encoding="utf-8")
    monkeypatch.delenv("POSTGRES_PASSWORD", raising=False)
    monkeypatch.delenv("MINIO_SECRET_KEY", raising=False)
    monkeypatch.setenv("POSTGRES_PASSWORD_FILE", str(postgres))
    monkeypatch.setenv("MINIO_SECRET_KEY_FILE", str(minio))
    load_settings.cache_clear()

    settings = load_settings()

    assert settings.postgres.password == "postgres-from-file"
    assert settings.minio.secret_key == "minio-from-file"
    load_settings.cache_clear()


def test_secret_file_takes_precedence_over_environment(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    secret = tmp_path / "postgres_password"
    secret.write_text("from-file\n", encoding="utf-8")
    monkeypatch.setenv("POSTGRES_PASSWORD", "from-environment")
    monkeypatch.setenv("POSTGRES_PASSWORD_FILE", str(secret))
    load_settings.cache_clear()

    assert load_settings().postgres.password == "from-file"

    load_settings.cache_clear()


@pytest.mark.parametrize(
    "name", ["POSTGRES_PASSWORD", "MINIO_ACCESS_KEY", "MINIO_SECRET_KEY"]
)
def test_missing_secret_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
    name: str,
) -> None:
    monkeypatch.delenv(name, raising=False)
    monkeypatch.delenv(f"{name}_FILE", raising=False)
    load_settings.cache_clear()

    with pytest.raises(ValueError, match=name):
        load_settings()
