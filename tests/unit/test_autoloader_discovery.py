"""Test directory listing — mặt đối chiếu của checkpoint."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from vn_climate_risk_monitor.ingestion.loader import (
    DiscoveredObject,
    discover,
    select_new,
)


@dataclass
class FakeObject:
    object_name: str
    size: int = 42
    etag: str = "e"
    last_modified: datetime | None = None


class FakeClient:
    def __init__(self, keys: list[str]) -> None:
        self.keys = keys
        self.calls: list[tuple[str, str, bool]] = []

    def list_objects(self, bucket_name: str, prefix: str, recursive: bool):
        self.calls.append((bucket_name, prefix, recursive))
        return [FakeObject(k) for k in self.keys if k.startswith(prefix)]


def test_finds_files_matching_pattern_at_any_depth() -> None:
    client = FakeClient(
        [
            "raw/2026/08/response_000.json",
            "raw/2026/08/21/09/response_001.json",
            "raw/response_002.json",
        ]
    )

    found = discover(client, "bkt", "raw", "**/response_*.json")

    assert [f.object_key for f in found] == [
        "raw/2026/08/21/09/response_001.json",
        "raw/2026/08/response_000.json",
        "raw/response_002.json",
    ]


def test_ignores_files_not_matching_pattern() -> None:
    client = FakeClient(["raw/a/response_000.json", "raw/a/_SUCCESS", "raw/a/note.txt"])

    found = discover(client, "bkt", "raw", "**/response_*.json")

    assert [f.object_key for f in found] == ["raw/a/response_000.json"]


def test_ignores_directory_markers() -> None:
    client = FakeClient(["raw/a/", "raw/a/response_000.json"])

    found = discover(client, "bkt", "raw", "**/response_*.json")

    assert len(found) == 1


def test_result_is_sorted_for_deterministic_batches() -> None:
    client = FakeClient(
        ["raw/c/response_1.json", "raw/a/response_1.json", "raw/b/response_1.json"]
    )

    found = discover(client, "bkt", "raw", "**/response_*.json")

    assert [f.object_key for f in found] == sorted(f.object_key for f in found)


def test_prefix_is_normalised_with_trailing_slash() -> None:
    client = FakeClient(["raw/a.json"])

    discover(client, "bkt", "raw", "**/*.json")

    assert client.calls == [("bkt", "raw/", True)]


def test_carries_size_and_etag_for_provenance() -> None:
    client = FakeClient(["raw/a.json"])

    found = discover(client, "bkt", "raw", "**/*.json")

    assert found[0].size_bytes == 42
    assert found[0].etag == "e"


def test_select_new_excludes_already_known_keys() -> None:
    discovered = (
        DiscoveredObject("raw/a.json", 1),
        DiscoveredObject("raw/b.json", 1),
    )

    fresh = select_new(discovered, {"raw/a.json"})

    assert [f.object_key for f in fresh] == ["raw/b.json"]


def test_select_new_returns_nothing_when_all_known() -> None:
    discovered = (DiscoveredObject("raw/a.json", 1),)

    assert select_new(discovered, {"raw/a.json"}) == ()
