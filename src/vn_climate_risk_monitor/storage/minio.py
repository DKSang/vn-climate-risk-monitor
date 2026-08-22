"""MinIO client construction without dataset-specific behavior.

Integrity của bronze/files được uỷ cho MinIO (bitrot protection) cộng với
etag/size ghi trong ``file_parameters`` JSONB lúc discovery — không còn lớp
verify checksum Python; xem ghi chú ADR trong docs/04b-ingestion-runbook.md.
"""

from __future__ import annotations

from minio import Minio

from vn_climate_risk_monitor.config import MinioSettings, load_settings


def get_minio_client(settings: MinioSettings | None = None) -> Minio:
    config = settings or load_settings().minio
    return Minio(
        config.endpoint,
        access_key=config.access_key,
        secret_key=config.secret_key,
        secure=config.secure,
    )


def ensure_bucket(client: Minio, bucket: str) -> None:
    """Create the lakehouse bucket if it does not already exist."""
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
