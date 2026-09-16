"""Typed runtime configuration shared by ingestion and lakehouse code."""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass


def _setting(name: str, default: str | None = None) -> str:
    """Read ``NAME_FILE`` first, then ``NAME``."""
    value = os.getenv(name)
    file_name = os.getenv(f"{name}_FILE")
    if file_name is not None:
        try:
            value = Path(file_name).read_text(encoding="utf-8").rstrip("\r\n")
        except OSError as error:
            raise ValueError(f"Cannot read {name}_FILE: {file_name}") from error
        if not value:
            raise ValueError(f"{name}_FILE must not be empty")
    if value is not None:
        return value
    if default is not None:
        return default
    raise ValueError(f"Set {name} or {name}_FILE")


def _as_bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _as_int(
    name: str,
    value: str,
    *,
    minimum: int = 1,
    maximum: int | None = None,
) -> int:
    try:
        parsed = int(value)
    except ValueError as error:
        raise ValueError(f"{name} must be an integer") from error
    if parsed < minimum:
        raise ValueError(f"{name} must be at least {minimum}")
    if maximum is not None and parsed > maximum:
        raise ValueError(f"{name} must be at most {maximum}")
    return parsed


@dataclass(frozen=True)
class MinioSettings:
    endpoint: str
    access_key: str
    secret_key: str
    bucket: str
    secure: bool

@dataclass(frozen=True)
class PostgresSettings:
    host: str
    port: int
    database: str
    user: str
    password: str

    @property
    def ducklake_connection_string(self) -> str:
        return (
            f"dbname={self.database} host={self.host} port={self.port} "
            f"user={self.user} password={self.password}"
        )

@dataclass(frozen=True)
class OpenMeteoSettings:
    forecast_url: str
    archive_url: str
    forecast_model: str
    forecast_hours: int
    location_batch_size: int
    fetch_workers: int


@dataclass(frozen=True)
class Settings:
    minio: MinioSettings
    postgres: PostgresSettings
    open_meteo: OpenMeteoSettings


@lru_cache(maxsize=1)
def load_settings() -> Settings:
    """Load settings once from ``.env`` and environment variables."""
    forecast_model = os.getenv("OPEN_METEO_FORECAST_MODEL", "ecmwf_ifs")
    if forecast_model != "ecmwf_ifs":
        raise ValueError("OPEN_METEO_FORECAST_MODEL must be 'ecmwf_ifs'")
    return Settings(
        minio=MinioSettings(
            endpoint=os.getenv("MINIO_ENDPOINT", "localhost:9000"),
            access_key=_setting("MINIO_ACCESS_KEY"),
            secret_key=_setting("MINIO_SECRET_KEY"),
            bucket=os.getenv("MINIO_BUCKET", "vn-climate"),
            secure=_as_bool(os.getenv("MINIO_SECURE", "false")),
        ),
        postgres=PostgresSettings(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            database=os.getenv("POSTGRES_DB", "vnclimate"),
            user=os.getenv("POSTGRES_USER", "vnclimate"),
            password=_setting("POSTGRES_PASSWORD"),
        ),
        open_meteo=OpenMeteoSettings(
            forecast_url=os.getenv(
                "OPEN_METEO_FORECAST_URL",
                "https://api.open-meteo.com/v1/forecast",
            ),
            archive_url=os.getenv(
                "OPEN_METEO_ARCHIVE_URL",
                "https://archive-api.open-meteo.com/v1/archive",
            ),
            # Archive model is period-based; see model_for_month().
            forecast_model=forecast_model,
            forecast_hours=_as_int(
                "OPEN_METEO_FORECAST_HOURS",
                os.getenv("OPEN_METEO_FORECAST_HOURS", "72"),
            ),
            location_batch_size=_as_int(
                "OPEN_METEO_LOCATION_BATCH_SIZE",
                os.getenv("OPEN_METEO_LOCATION_BATCH_SIZE", "25"),
            ),
            fetch_workers=_as_int(
                "OPEN_METEO_FETCH_WORKERS",
                os.getenv("OPEN_METEO_FETCH_WORKERS", "4"),
                maximum=16,
            ),
        ),
    )
