"""MinIO client construction without dataset-specific behavior."""

from __future__ import annotations

import hashlib
import io
from dataclasses import dataclass

from minio import Minio
from minio.error import S3Error

from vn_climate_risk_monitor.config import MinioSettings, load_settings
from vn_climate_risk_monitor.storage.models import SourceObjectMetadata


class ObjectWriteError(RuntimeError):
    """Raised when object storage rejects a collector write."""


class ObjectIntegrityError(RuntimeError):
    """Raised when a source object no longer matches its control metadata."""


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


@dataclass(frozen=True)
class ImmutableObjectWriter:
    """Write-once MinIO adapter used by collectors."""

    client: Minio
    bucket: str

    def write(
        self,
        object_key: str,
        content: bytes,
        *,
        content_type: str,
    ) -> SourceObjectMetadata:
        """Persist bytes once and return integrity metadata for PostgreSQL."""
        try:
            self.client.stat_object(self.bucket, object_key)
        except S3Error as error:
            if error.code not in {"NoSuchKey", "NoSuchObject", "NoSuchBucket"}:
                raise ObjectWriteError(
                    f"Could not inspect object: {object_key}"
                ) from error
        else:
            raise FileExistsError(f"Object already exists: {object_key}")

        try:
            result = self.client.put_object(
                self.bucket,
                object_key,
                io.BytesIO(content),
                len(content),
                content_type=content_type,
            )
        except S3Error as error:
            raise ObjectWriteError(f"Could not write object: {object_key}") from error
        return SourceObjectMetadata(
            object_key=object_key,
            size_bytes=len(content),
            sha256=hashlib.sha256(content).hexdigest(),
            content_type=content_type,
            etag=getattr(result, "etag", None),
        )


@dataclass(frozen=True)
class VerifiedObjectReader:
    """Read an object only when its size and SHA-256 match PostgreSQL metadata."""

    client: Minio
    bucket: str

    def read(
        self, object_key: str, *, expected_size: int, expected_sha256: str
    ) -> bytes:
        try:
            response = self.client.get_object(self.bucket, object_key)
            try:
                content = response.read()
            finally:
                response.close()
                response.release_conn()
        except S3Error as error:
            raise ObjectWriteError(f"Could not read object: {object_key}") from error

        actual_sha256 = hashlib.sha256(content).hexdigest()
        if len(content) != expected_size or actual_sha256 != expected_sha256.lower():
            raise ObjectIntegrityError(
                f"Object integrity mismatch: {object_key} "
                f"(size={len(content)}, sha256={actual_sha256})"
            )
        return content
