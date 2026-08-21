"""Storage adapters used by collectors and lakehouse loaders."""

from vn_climate_risk_monitor.storage.minio import ensure_bucket, get_minio_client

__all__ = ["ensure_bucket", "get_minio_client"]
