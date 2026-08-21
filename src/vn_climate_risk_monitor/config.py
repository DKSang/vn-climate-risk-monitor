"""Typed runtime configuration shared by ingestion and lakehouse code."""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache


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

    @property
    def scheme(self) -> str:
        return "https" if self.secure else "http"


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

    @property
    def connect_kwargs(self) -> dict[str, str | int]:
        """Return keyword arguments accepted by ``psycopg.connect``."""
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.database,
            "user": self.user,
            "password": self.password,
        }


@dataclass(frozen=True)
class OpenMeteoSettings:
    forecast_url: str
    archive_url: str
    forecast_model: str
    archive_model: str
    forecast_hours: int
    location_batch_size: int
    concurrency: int
    request_timeout_seconds: int
    max_attempts: int
    max_effective_calls_per_minute: int
    max_effective_calls_per_hour: int
    schedule_minute_utc: int
    collector_stale_after_seconds: int
    loader_batch_size: int
    loader_lease_seconds: int
    loader_max_retries: int
    max_load_batches: int
    stale_after_minutes: int


@dataclass(frozen=True)
class Settings:
    minio: MinioSettings
    postgres: PostgresSettings
    open_meteo: OpenMeteoSettings


@lru_cache(maxsize=1)
def load_settings() -> Settings:
    """Load settings once from ``.env`` and environment variables."""
    try:
        from dotenv import load_dotenv

        load_dotenv()
    except ImportError:
        pass

    return Settings(
        minio=MinioSettings(
            endpoint=os.getenv("MINIO_ENDPOINT", "localhost:9000"),
            access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
            bucket=os.getenv("MINIO_BUCKET", "vn-climate"),
            secure=_as_bool(os.getenv("MINIO_SECURE", "false")),
        ),
        postgres=PostgresSettings(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            database=os.getenv("POSTGRES_DB", "vnclimate"),
            user=os.getenv("POSTGRES_USER", "vnclimate"),
            password=os.getenv("POSTGRES_PASSWORD", "vnclimate"),
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
            forecast_model=os.getenv("OPEN_METEO_FORECAST_MODEL", "best_match"),
            archive_model=os.getenv("OPEN_METEO_ARCHIVE_MODEL", "era5"),
            forecast_hours=_as_int(
                "OPEN_METEO_FORECAST_HOURS",
                os.getenv("OPEN_METEO_FORECAST_HOURS", "72"),
            ),
            location_batch_size=_as_int(
                "OPEN_METEO_LOCATION_BATCH_SIZE",
                os.getenv("OPEN_METEO_LOCATION_BATCH_SIZE", "25"),
            ),
            concurrency=_as_int(
                "OPEN_METEO_CONCURRENCY",
                os.getenv("OPEN_METEO_CONCURRENCY", "1"),
            ),
            request_timeout_seconds=_as_int(
                "OPEN_METEO_REQUEST_TIMEOUT_SECONDS",
                os.getenv("OPEN_METEO_REQUEST_TIMEOUT_SECONDS", "60"),
            ),
            max_attempts=_as_int(
                "OPEN_METEO_MAX_ATTEMPTS",
                os.getenv("OPEN_METEO_MAX_ATTEMPTS", "5"),
            ),
            max_effective_calls_per_minute=_as_int(
                "OPEN_METEO_MAX_EFFECTIVE_CALLS_PER_MINUTE",
                os.getenv("OPEN_METEO_MAX_EFFECTIVE_CALLS_PER_MINUTE", "500"),
            ),
            max_effective_calls_per_hour=_as_int(
                "OPEN_METEO_MAX_EFFECTIVE_CALLS_PER_HOUR",
                os.getenv("OPEN_METEO_MAX_EFFECTIVE_CALLS_PER_HOUR", "4500"),
            ),
            schedule_minute_utc=_as_int(
                "OPEN_METEO_SCHEDULE_MINUTE_UTC",
                os.getenv("OPEN_METEO_SCHEDULE_MINUTE_UTC", "15"),
                minimum=0,
                maximum=59,
            ),
            collector_stale_after_seconds=_as_int(
                "OPEN_METEO_COLLECTOR_STALE_AFTER_SECONDS",
                os.getenv("OPEN_METEO_COLLECTOR_STALE_AFTER_SECONDS", "1800"),
            ),
            loader_batch_size=_as_int(
                "OPEN_METEO_LOADER_BATCH_SIZE",
                os.getenv("OPEN_METEO_LOADER_BATCH_SIZE", "10"),
            ),
            loader_lease_seconds=_as_int(
                "OPEN_METEO_LOADER_LEASE_SECONDS",
                os.getenv("OPEN_METEO_LOADER_LEASE_SECONDS", "300"),
            ),
            loader_max_retries=_as_int(
                "OPEN_METEO_LOADER_MAX_RETRIES",
                os.getenv("OPEN_METEO_LOADER_MAX_RETRIES", "3"),
                minimum=0,
            ),
            max_load_batches=_as_int(
                "OPEN_METEO_MAX_LOAD_BATCHES",
                os.getenv("OPEN_METEO_MAX_LOAD_BATCHES", "100"),
            ),
            stale_after_minutes=_as_int(
                "OPEN_METEO_STALE_AFTER_MINUTES",
                os.getenv("OPEN_METEO_STALE_AFTER_MINUTES", "120"),
            ),
        ),
    )
