"""Storage-neutral metadata for immutable source objects."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SourceObjectMetadata:
    """Integrity metadata recorded in the PostgreSQL control plane."""

    object_key: str
    size_bytes: int
    sha256: str
    content_type: str
    etag: str | None = None

    def __post_init__(self) -> None:
        object_key = self.object_key.strip()
        content_type = self.content_type.strip()
        if not object_key:
            raise ValueError("object_key must not be empty")
        if not content_type:
            raise ValueError("content_type must not be empty")
        object.__setattr__(self, "object_key", object_key)
        object.__setattr__(self, "content_type", content_type)
        normalized_sha = self.sha256.strip().lower()
        if len(normalized_sha) != 64 or any(
            character not in "0123456789abcdef" for character in normalized_sha
        ):
            raise ValueError("sha256 must contain 64 hexadecimal characters")
        object.__setattr__(self, "sha256", normalized_sha)
        if self.size_bytes < 0:
            raise ValueError("size_bytes must not be negative")
