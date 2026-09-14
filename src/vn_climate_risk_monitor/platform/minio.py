"""MinIO client construction for Bronze storage."""

from __future__ import annotations

from minio import Minio

from vn_climate_risk_monitor.platform.settings import MinioSettings


def get_minio_client(settings: MinioSettings) -> Minio:
    return Minio(
        settings.endpoint,
        access_key=settings.access_key,
        secret_key=settings.secret_key,
        secure=settings.secure,
    )
