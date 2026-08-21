"""Typed runtime configuration shared by ingestion and lakehouse code."""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache


def _as_bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


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


@dataclass(frozen=True)
class Settings:
    minio: MinioSettings
    postgres: PostgresSettings


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
    )
