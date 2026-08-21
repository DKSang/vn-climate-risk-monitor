"""Storage adapters used by collectors and lakehouse loaders."""

from vn_climate_risk_monitor.storage.minio import (
    ImmutableObjectWriter,
    ObjectIntegrityError,
    ObjectWriteError,
    VerifiedObjectReader,
    ensure_bucket,
    get_minio_client,
)

__all__ = [
    "ImmutableObjectWriter",
    "ObjectIntegrityError",
    "ObjectWriteError",
    "VerifiedObjectReader",
    "ensure_bucket",
    "get_minio_client",
]
