"""Storage adapters dùng cho fetch và bootstrap lakehouse."""

from vn_climate_risk_monitor.storage.minio import ensure_bucket, get_minio_client

__all__ = ["ensure_bucket", "get_minio_client"]
