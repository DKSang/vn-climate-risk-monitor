"""MinIO client construction without dataset-specific behavior.

Integrity của bronze/files được uỷ cho MinIO (bitrot protection) cộng với
metadata file ghi trong PostgreSQL lúc discovery; loader giữ lineage bằng
``_source_file``. Xem [Operations](../../../docs/05-operations.md).
"""

from __future__ import annotations

from minio import Minio

from vn_climate_risk_monitor.config import MinioSettings


def get_minio_client(settings: MinioSettings) -> Minio:
    return Minio(
        settings.endpoint,
        access_key=settings.access_key,
        secret_key=settings.secret_key,
        secure=settings.secure,
    )
