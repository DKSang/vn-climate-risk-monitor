"""Phát hiện file mới trên object storage — directory listing.

Tương ứng chế độ *directory listing* của Databricks Auto Loader.

Khác biệt quan trọng so với thiết kế cũ của dự án: collector trước đây tự đăng ký
file vào control plane NGAY LÚC GHI, nên chỉ nhặt được file do chính nó tạo ra, và
nếu tiến trình chết giữa lúc ghi xong và lúc đăng ký thì file đó mồ côi vĩnh viễn.

Module này tách hẳn hai việc: ai ghi file không quan trọng, engine chỉ LIỆT KÊ
những gì đang có trên storage rồi đối chiếu với checkpoint để biết cái nào mới.
Nhờ vậy file mồ côi tự được nhặt ở lần chạy sau, và có thể nạp cả file do công cụ
khác đổ vào.
"""

from __future__ import annotations

import fnmatch
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol


@dataclass(frozen=True)
class DiscoveredObject:
    """Một file thấy trên storage, chưa biết đã xử lý hay chưa."""

    object_key: str
    size_bytes: int
    etag: str | None = None
    last_modified: datetime | None = None


class ObjectLister(Protocol):
    """Ranh giới tối thiểu với object storage — dễ thay MinIO bằng S3/GCS/local."""

    def list_objects(
        self, bucket_name: str, prefix: str, recursive: bool
    ) -> Iterator[object]: ...


def discover(
    client: ObjectLister,
    bucket: str,
    prefix: str,
    pattern: str = "**/*.json",
) -> tuple[DiscoveredObject, ...]:
    """Liệt kê object khớp ``pattern`` dưới ``prefix``.

    ``pattern`` dùng cú pháp glob và so khớp với phần đuôi sau ``prefix``.
    """
    normalized_prefix = prefix.rstrip("/") + "/"
    found: list[DiscoveredObject] = []
    for entry in client.list_objects(bucket, prefix=normalized_prefix, recursive=True):
        object_key = getattr(entry, "object_name", None)
        if not object_key or object_key.endswith("/"):
            continue
        relative = object_key[len(normalized_prefix) :]
        if not _matches(relative, pattern):
            continue
        found.append(
            DiscoveredObject(
                object_key=object_key,
                size_bytes=getattr(entry, "size", 0) or 0,
                etag=getattr(entry, "etag", None),
                last_modified=getattr(entry, "last_modified", None),
            )
        )
    return tuple(sorted(found, key=lambda item: item.object_key))


def _matches(relative_key: str, pattern: str) -> bool:
    """Khớp glob, hiểu ``**`` là "bao nhiêu cấp thư mục cũng được"."""
    if pattern.startswith("**/"):
        tail = pattern[3:]
        return fnmatch.fnmatch(relative_key, pattern) or fnmatch.fnmatch(
            relative_key.rsplit("/", 1)[-1], tail
        )
    return fnmatch.fnmatch(relative_key, pattern)


def select_new(
    discovered: tuple[DiscoveredObject, ...],
    already_known: set[str],
) -> tuple[DiscoveredObject, ...]:
    """Giữ lại những file chưa có trong checkpoint."""
    return tuple(item for item in discovered if item.object_key not in already_known)
