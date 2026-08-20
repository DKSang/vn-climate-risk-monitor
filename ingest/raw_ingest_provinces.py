#!/usr/bin/env python3
"""
Ingest Vietnamese Provinces Database raw data from GitHub (100% Nguyên bản / As-Is).
Repository: https://github.com/ThangLeQuoc/vietnamese-provinces-database/

Raw Folder Architecture:
  raw/
    └── {DOMAIN}           (geography)
        └── {SOURCE_SYSTEM}(vietnamese_provinces_db)
            └── {DATASET}  (administrative_units)
                └── {LOAD_TYPE} (full)
                    └── {YYYY}/{MM}/{DD}/
                        ├── full_json_generated_data_vn_units.json
                        ├── postgres_CreateTables_vn_units.sql
                        ├── postgres_ImportData_vn_units.sql
                        └── _manifest.json

Target Storage:
  1. Local Lake Mirror: lake/raw/...
  2. MinIO S3 Lake:     s3://vn-climate/raw/...
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import httpx
from minio import Minio

# ---------------------------------------------------------------------------
# Load environment variables
# ---------------------------------------------------------------------------
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "vn-climate")

# ---------------------------------------------------------------------------
# Hierarchy definition (Khớp 100% sơ đồ thiết kế)
# ---------------------------------------------------------------------------
LAYER = "raw"
DOMAIN = "geography"
SOURCE_SYSTEM = "vietnamese_provinces_db"
DATASET = "administrative_units"
LOAD_TYPE = "full"

REPO_RAW_BASE = "https://raw.githubusercontent.com/ThangLeQuoc/vietnamese-provinces-database/master"
REPO_API_COMMIT = "https://api.github.com/repos/ThangLeQuoc/vietnamese-provinces-database/commits/master"

# Các file dữ liệu RAW nguyên bản giữ nguyên định dạng nguồn (.json, .sql)
RAW_FILES_TO_INGEST = [
    {
        "url": f"{REPO_RAW_BASE}/json/full_json_generated_data_vn_units.json",
        "filename": "full_json_generated_data_vn_units.json",
        "content_type": "application/json",
        "description": "Full raw hierarchy JSON of Vietnamese administrative units (provinces, districts, wards)",
    },
    {
        "url": f"{REPO_RAW_BASE}/postgresql/postgres_CreateTables_vn_units.sql",
        "filename": "postgres_CreateTables_vn_units.sql",
        "content_type": "application/sql",
        "description": "Raw PostgreSQL DDL schema definition script",
    },
    {
        "url": f"{REPO_RAW_BASE}/postgresql/postgres_ImportData_vn_units.sql",
        "filename": "postgres_ImportData_vn_units.sql",
        "content_type": "application/sql",
        "description": "Raw PostgreSQL INSERT data statements",
    },
]


def sha256_checksum(data: bytes) -> str:
    """Calculate SHA256 checksum of raw content."""
    return hashlib.sha256(data).hexdigest()


def get_latest_commit_sha() -> str:
    """Get the latest commit SHA from the repository for lineage/version tracking."""
    try:
        resp = httpx.get(REPO_API_COMMIT, headers={"User-Agent": "vn-climate-risk-monitor"}, timeout=10)
        if resp.status_code == 200:
            return resp.json().get("sha", "unknown")
    except Exception as e:
        print(f"  ⚠ Could not fetch commit SHA from GitHub API: {e}")
    return "master"


def get_minio_client() -> Minio:
    """Initialize MinIO client and ensure target bucket exists."""
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )
    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)
        print(f"  ✓ Created MinIO bucket '{MINIO_BUCKET}'")
    return client


def main() -> None:
    now = datetime.now(timezone.utc)
    year_str = now.strftime("%Y")
    month_str = now.strftime("%m")
    day_str = now.strftime("%d")

    # Relative path inside the lake
    rel_folder = f"{LAYER}/{DOMAIN}/{SOURCE_SYSTEM}/{DATASET}/{LOAD_TYPE}/{year_str}/{month_str}/{day_str}"
    local_dir = Path("lake") / rel_folder

    print("=" * 75)
    print("  RAW INGESTION: Vietnamese Provinces Database (100% Nguyên bản)")
    print(f"  Source:          https://github.com/ThangLeQuoc/vietnamese-provinces-database")
    print(f"  Folder Layout:   /{rel_folder}/")
    print("=" * 75)

    local_dir.mkdir(parents=True, exist_ok=True)
    minio_client = get_minio_client()

    commit_sha = get_latest_commit_sha()
    print(f"\n── Tải và lưu trữ dữ liệu RAW nguyên bản (Commit: {commit_sha[:8] if commit_sha else 'latest'}) ──")

    downloaded_files_manifest = []

    with httpx.Client(timeout=60.0, follow_redirects=True) as http_client:
        for file_spec in RAW_FILES_TO_INGEST:
            url = file_spec["url"]
            filename = file_spec["filename"]
            local_path = local_dir / filename
            minio_object_path = f"{rel_folder}/{filename}"

            print(f"\n  → Đang tải: {filename}...")
            resp = http_client.get(url)
            resp.raise_for_status()
            content = resp.content

            # 1. Lưu bản copy nguyên bản vào local lake mirror
            with open(local_path, "wb") as f:
                f.write(content)

            # 2. Upload nguyên bản lên MinIO S3
            content_stream = io.BytesIO(content)
            minio_client.put_object(
                bucket_name=MINIO_BUCKET,
                object_name=minio_object_path,
                data=content_stream,
                length=len(content),
                content_type=file_spec["content_type"],
            )

            checksum = sha256_checksum(content)
            size_kb = len(content) / 1024

            downloaded_files_manifest.append({
                "filename": filename,
                "s3_path": f"s3://{MINIO_BUCKET}/{minio_object_path}",
                "local_path": str(local_path),
                "size_bytes": len(content),
                "size_kb": round(size_kb, 2),
                "sha256": checksum,
                "content_type": file_spec["content_type"],
                "description": file_spec["description"],
            })

            print(f"    ✓ Đã lưu local: {local_path} ({size_kb:.1f} KB)")
            print(f"    ✓ Đã upload MinIO: s3://{MINIO_BUCKET}/{minio_object_path}")
            print(f"    ✓ Checksum SHA256: {checksum[:16]}...")

    # 3. Tạo file _manifest.json chứa metadata audit của lần chạy
    manifest_data = {
        "layer": LAYER,
        "domain": DOMAIN,
        "source_system": SOURCE_SYSTEM,
        "dataset": DATASET,
        "load_type": LOAD_TYPE,
        "ingestion_date": {
            "year": int(year_str),
            "month": int(month_str),
            "day": int(day_str),
            "timestamp_utc": now.isoformat(),
        },
        "source_metadata": {
            "repository": "https://github.com/ThangLeQuoc/vietnamese-provinces-database",
            "commit_sha": commit_sha,
            "license": "MIT",
        },
        "files": downloaded_files_manifest,
    }

    manifest_bytes = json.dumps(manifest_data, indent=2, ensure_ascii=False).encode("utf-8")
    manifest_local_path = local_dir / "_manifest.json"
    manifest_minio_path = f"{rel_folder}/_manifest.json"

    with open(manifest_local_path, "wb") as f:
        f.write(manifest_bytes)

    minio_client.put_object(
        bucket_name=MINIO_BUCKET,
        object_name=manifest_minio_path,
        data=io.BytesIO(manifest_bytes),
        length=len(manifest_bytes),
        content_type="application/json",
    )

    print("\n── Tổng kết Ingestion RAW ──")
    print(f"  ✓ Manifest Local:  {manifest_local_path}")
    print(f"  ✓ Manifest MinIO:  s3://{MINIO_BUCKET}/{manifest_minio_path}")
    print(f"  ✓ Số file nguyên bản đã ingest: {len(downloaded_files_manifest)}")
    for f in downloaded_files_manifest:
        print(f"    • {f['filename']}: {f['size_kb']} KB")

    print("\n" + "=" * 75)
    print("  ✅ INGEST RAW HOÀN TẤT THÀNH CÔNG (100% NGUYÊN BẢN)")
    print("=" * 75)


if __name__ == "__main__":
    main()
