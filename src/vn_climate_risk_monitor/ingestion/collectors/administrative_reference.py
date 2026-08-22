"""Collect versioned administrative reference files into ``bronze/files``.

This is the existing static geography collector moved into the application
package. It does not collect Open-Meteo data.
"""

from __future__ import annotations

import hashlib
import io
import json
from datetime import UTC, datetime

from requests import Session

from autoloader.http import build_http_session
from autoloader.layout import BronzeFilesLayout
from vn_climate_risk_monitor.config import load_settings
from vn_climate_risk_monitor.storage import ensure_bucket, get_minio_client

REPOSITORY = "https://github.com/ThangLeQuoc/vietnamese-provinces-database"
RAW_BASE = (
    "https://raw.githubusercontent.com/ThangLeQuoc/vietnamese-provinces-database/master"
)
COMMIT_API = "https://api.github.com/repos/ThangLeQuoc/vietnamese-provinces-database/commits/master"

SOURCE_FILES = (
    (
        "full_json_generated_data_vn_units.json",
        "json/full_json_generated_data_vn_units.json",
    ),
    (
        "postgres_CreateTables_vn_units.sql",
        "postgresql/postgres_CreateTables_vn_units.sql",
    ),
    ("postgres_ImportData_vn_units.sql", "postgresql/postgres_ImportData_vn_units.sql"),
)


def _sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _commit_sha(client: Session) -> str:
    response = client.get(COMMIT_API, timeout=60)
    response.raise_for_status()
    return str(response.json()["sha"])


def collect() -> str:
    """Persist one immutable, replayable Bronze file run and return its prefix."""
    settings = load_settings()
    minio = get_minio_client(settings.minio)
    ensure_bucket(minio, settings.minio.bucket)

    ingested_at = datetime.now(UTC)
    layout = BronzeFilesLayout(
        source_system="vietnamese_provinces_db",
        dataset="administrative_units",
        load_type="full",
    )

    with build_http_session() as client:
        commit_sha = _commit_sha(client)
        run_id = f"{ingested_at:%Y%m%dT%H%M%SZ}_{commit_sha[:8]}"
        prefix = layout.run_prefix(ingested_at, run_id)
        manifest_files: list[dict[str, object]] = []

        for filename, relative_url in SOURCE_FILES:
            source_url = f"{RAW_BASE}/{relative_url}"
            response = client.get(source_url, timeout=60)
            response.raise_for_status()
            content = response.content
            object_key = layout.object_key(ingested_at, run_id, filename)

            minio.put_object(
                settings.minio.bucket,
                object_key,
                io.BytesIO(content),
                len(content),
                content_type=response.headers.get(
                    "content-type", "application/octet-stream"
                ),
            )
            manifest_files.append(
                {
                    "filename": filename,
                    "object_key": object_key,
                    "source_url": source_url,
                    "size_bytes": len(content),
                    "sha256": _sha256(content),
                }
            )

    manifest = {
        "run_id": run_id,
        "source_system": "vietnamese_provinces_db",
        "dataset": "administrative_units",
        "load_type": "full",
        "ingested_at_utc": ingested_at.isoformat(),
        "repository": REPOSITORY,
        "commit_sha": commit_sha,
        "status": "success",
        "files": manifest_files,
    }
    manifest_bytes = json.dumps(manifest, indent=2, ensure_ascii=False).encode("utf-8")
    manifest_key = layout.object_key(ingested_at, run_id, "_manifest.json")
    minio.put_object(
        settings.minio.bucket,
        manifest_key,
        io.BytesIO(manifest_bytes),
        len(manifest_bytes),
        content_type="application/json",
    )
    success_key = layout.object_key(ingested_at, run_id, "_SUCCESS")
    minio.put_object(settings.minio.bucket, success_key, io.BytesIO(b""), 0)
    return prefix


def main() -> None:
    prefix = collect()
    print(f"Bronze file run committed: {prefix}")


if __name__ == "__main__":
    main()
