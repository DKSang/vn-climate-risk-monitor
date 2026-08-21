import hashlib
from dataclasses import dataclass
from typing import Any, cast

import pytest
from minio.error import S3Error

from vn_climate_risk_monitor.storage import (
    ImmutableObjectWriter,
    ObjectIntegrityError,
    VerifiedObjectReader,
)


@dataclass
class PutResult:
    etag: str


class MissingObjectClient:
    def __init__(self) -> None:
        self.writes: list[tuple[str, str, bytes, str]] = []

    def stat_object(self, bucket: str, object_key: str) -> None:
        raise S3Error(
            cast(Any, None),
            "NoSuchKey",
            "missing",
            object_key,
            "request-id",
            "host-id",
            bucket,
            object_key,
        )

    def put_object(
        self,
        bucket: str,
        object_key: str,
        stream: Any,
        length: int,
        *,
        content_type: str,
    ) -> PutResult:
        content = stream.read(length)
        self.writes.append((bucket, object_key, content, content_type))
        return PutResult(etag="etag-001")


class ExistingObjectClient:
    def stat_object(self, bucket: str, object_key: str) -> object:
        return object()


class ObjectResponse:
    def __init__(self, content: bytes) -> None:
        self.content = content
        self.closed = False
        self.released = False

    def read(self) -> bytes:
        return self.content

    def close(self) -> None:
        self.closed = True

    def release_conn(self) -> None:
        self.released = True


class ReadClient:
    def __init__(self, content: bytes) -> None:
        self.response = ObjectResponse(content)

    def get_object(self, bucket: str, object_key: str) -> ObjectResponse:
        assert bucket == "vn-climate"
        assert object_key.endswith("response_000.json")
        return self.response


def test_immutable_writer_returns_integrity_metadata() -> None:
    client = MissingObjectClient()
    writer = ImmutableObjectWriter(cast(Any, client), "vn-climate")
    content = b'{"source":"open-meteo"}'

    metadata = writer.write(
        "bronze/files/open_meteo/response_000.json",
        content,
        content_type="application/json",
    )

    assert metadata.size_bytes == len(content)
    assert metadata.sha256 == hashlib.sha256(content).hexdigest()
    assert metadata.etag == "etag-001"
    assert client.writes[0][2] == content


def test_immutable_writer_refuses_overwrite() -> None:
    writer = ImmutableObjectWriter(cast(Any, ExistingObjectClient()), "vn-climate")

    with pytest.raises(FileExistsError, match="already exists"):
        writer.write(
            "bronze/files/existing.json", b"data", content_type="application/json"
        )


def test_verified_reader_checks_control_plane_integrity_metadata() -> None:
    content = b'{"latitude":21.0}'
    client = ReadClient(content)
    reader = VerifiedObjectReader(cast(Any, client), "vn-climate")

    result = reader.read(
        "bronze/files/open_meteo/response_000.json",
        expected_size=len(content),
        expected_sha256=hashlib.sha256(content).hexdigest(),
    )

    assert result == content
    assert client.response.closed
    assert client.response.released


def test_verified_reader_rejects_changed_content() -> None:
    reader = VerifiedObjectReader(cast(Any, ReadClient(b"changed")), "vn-climate")

    with pytest.raises(ObjectIntegrityError, match="integrity mismatch"):
        reader.read(
            "bronze/files/open_meteo/response_000.json",
            expected_size=7,
            expected_sha256="0" * 64,
        )
