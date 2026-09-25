"""Runtime settings, read once from environment variables (and `.env` locally)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import cache

from dotenv import load_dotenv


@dataclass(frozen=True)
class PostgresSettings:
    host: str
    port: int
    database: str
    user: str
    password: str

    @property
    def dsn(self) -> str:
        return (
            f"host={self.host} port={self.port} dbname={self.database} "
            f"user={self.user} password={self.password}"
        )


@dataclass(frozen=True)
class MinioSettings:
    endpoint: str
    access_key: str
    secret_key: str
    bucket: str
    secure: bool


@dataclass(frozen=True)
class OpenMeteoSettings:
    forecast_url: str
    archive_url: str
    forecast_hours: int
    location_batch_size: int


@dataclass(frozen=True)
class Settings:
    postgres: PostgresSettings
    minio: MinioSettings
    open_meteo: OpenMeteoSettings


@cache
def load_settings() -> Settings:
    load_dotenv()
    env = os.environ
    return Settings(
        postgres=PostgresSettings(
            host=env.get("POSTGRES_HOST", "localhost"),
            port=int(env.get("POSTGRES_PORT", "5432")),
            database=env.get("POSTGRES_DB", "vnclimate"),
            user=env.get("POSTGRES_USER", "vnclimate"),
            password=env["POSTGRES_PASSWORD"],
        ),
        minio=MinioSettings(
            endpoint=env.get("MINIO_ENDPOINT", "localhost:9000"),
            access_key=env["MINIO_ACCESS_KEY"],
            secret_key=env["MINIO_SECRET_KEY"],
            bucket=env.get("MINIO_BUCKET", "vn-climate"),
            secure=env.get("MINIO_SECURE", "false").lower() == "true",
        ),
        open_meteo=OpenMeteoSettings(
            forecast_url=env.get(
                "OPEN_METEO_FORECAST_URL", "https://api.open-meteo.com/v1/forecast"
            ),
            archive_url=env.get(
                "OPEN_METEO_ARCHIVE_URL",
                "https://archive-api.open-meteo.com/v1/archive",
            ),
            forecast_hours=int(env.get("OPEN_METEO_FORECAST_HOURS", "72")),
            location_batch_size=int(env.get("OPEN_METEO_LOCATION_BATCH_SIZE", "25")),
        ),
    )
