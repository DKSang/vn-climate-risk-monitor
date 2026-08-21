"""Canonical object-key layout for immutable Bronze files."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime


def _segment(value: str, field: str) -> str:
    normalized = value.strip().lower().replace("-", "_")
    if not normalized or "/" in normalized or normalized in {".", ".."}:
        raise ValueError(f"Invalid {field}: {value!r}")
    return normalized


@dataclass(frozen=True)
class BronzeFilesLayout:
    """Build append-only MinIO keys below the ``bronze/files`` namespace."""

    source_system: str
    dataset: str
    load_type: str

    def run_prefix(self, ingested_at: datetime, run_id: str) -> str:
        if ingested_at.tzinfo is None:
            raise ValueError("ingested_at must be timezone-aware")

        timestamp = ingested_at.astimezone(UTC)
        parts = (
            "bronze",
            "files",
            _segment(self.source_system, "source_system"),
            _segment(self.dataset, "dataset"),
            _segment(self.load_type, "load_type"),
            timestamp.strftime("%Y"),
            timestamp.strftime("%m"),
            timestamp.strftime("%d"),
            timestamp.strftime("%H"),
            _segment(run_id, "run_id"),
        )
        return "/".join(parts)

    def object_key(self, ingested_at: datetime, run_id: str, filename: str) -> str:
        safe_filename = filename.strip()
        if not safe_filename or "/" in safe_filename or safe_filename in {".", ".."}:
            raise ValueError(f"Invalid filename: {filename!r}")
        return f"{self.run_prefix(ingested_at, run_id)}/{safe_filename}"
