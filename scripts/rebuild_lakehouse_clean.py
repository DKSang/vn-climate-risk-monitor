"""
Script to cleanly rebuild Bronze and Gold layers on DuckLake + MinIO (Kiểu A Lakehouse).

Guarantees:
  1. Clean storage: No leftover temporary folders (no main_gold, no _dbt_tmp).
  2. Every Bronze table has its dedicated folder on MinIO:
       s3://vn-climate/bronze/raw_provinces/*.parquet
       s3://vn-climate/bronze/raw_wards/*.parquet
       s3://vn-climate/bronze/raw_administrative_units/*.parquet
       s3://vn-climate/bronze/raw_administrative_regions/*.parquet
       s3://vn-climate/bronze/raw_locations_coordinates/*.parquet
  3. Gold Dimension table has its dedicated clean folder:
       s3://vn-climate/gold/dim_hanoi_locations/*.parquet
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# Add project root to python path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import subprocess
from minio import Minio
import duckdb
from vn_climate_risk_monitor.lakehouse import get_connection

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "127.0.0.1:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "vn-climate")


def clean_stale_minio_folders():
    """Remove old/temporary folders from MinIO bucket."""
    print("── 1. Cleaning stale folders on MinIO... ──")
    client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)
    
    # Delete main_gold and old bronze/gold temp objects
    prefixes_to_clean = ["main_gold", "bronze", "gold"]
    for prefix in prefixes_to_clean:
        objects = list(client.list_objects(MINIO_BUCKET, prefix=f"{prefix}/", recursive=True))
        for obj in objects:
            client.remove_object(MINIO_BUCKET, obj.object_name)
            print(f"  ✗ Removed stale: {obj.object_name}")


def run_bronze_ingest():
    """Run dlt ingest pipeline to create Bronze tables."""
    print("\n── 2. Running dlt Bronze Ingestion... ──")
    from ingest.dlt_pipelines.pipelines.bronze_ingest_pipeline import run_pipeline
    run_pipeline()


def run_gold_transformation():
    """Run dbt transformation to build clean Gold tables."""
    print("\n── 3. Running dbt transformation to Gold... ──")
    res = subprocess.run(
        ["uv", "run", "dbt", "run", "--profiles-dir", "."],
        cwd="transform",
        capture_output=True,
        text=True,
    )
    print(res.stdout)
    if res.returncode != 0:
        print("dbt Error:", res.stderr)
        raise RuntimeError("dbt run failed")


def main():
    print("=" * 75)
    print("  LAKEHOUSE REBUILD: Clean Bronze & Gold (Kiểu A Layout)")
    print("=" * 75)

    clean_stale_minio_folders()
    run_bronze_ingest()
    run_gold_transformation()

    # List final MinIO objects to verify clean layout
    client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)
    print("\n── 4. Final MinIO Lakehouse Structure ──")
    objects = list(client.list_objects(MINIO_BUCKET, recursive=True))
    for obj in objects:
        print(f"  • s3://{MINIO_BUCKET}/{obj.object_name} ({obj.size / 1024:.1f} KB)")

    print("\n" + "=" * 75)
    print("  ✅ REBUILD COMPLETE & CLEAN")
    print("=" * 75)


if __name__ == "__main__":
    main()
