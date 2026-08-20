"""
dlt pipeline: Ingest from PostgreSQL and CSV to DuckLake Bronze Tables.

Destination:
  DuckLake Catalog 'catalog1', Schema 'bronze'
  (Metadata in Postgres 'ducklake', Data in MinIO 's3://vn-climate/bronze/...')

Tables Created in Bronze:
  - catalog1.bronze.raw_provinces
  - catalog1.bronze.raw_wards
  - catalog1.bronze.raw_administrative_units
  - catalog1.bronze.raw_administrative_regions
  - catalog1.bronze.raw_locations_coordinates
"""

from __future__ import annotations

import os
import dlt
from dlt.destinations import filesystem
from vn_climate_risk_monitor.lakehouse import get_connection

from ingest.dlt_pipelines.sources.postgres_and_csv_source import postgres_and_csv_source

# MinIO Config
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "vn-climate")


def run_pipeline() -> None:
    print("=" * 75)
    print("  STEP 1: DLT INGEST -> DuckLake BRONZE Layer")
    print(f"  Source A: PostgreSQL (provinces, wards, units, regions)")
    print(f"  Source B: CSV (dim_location.csv)")
    print(f"  Destination: DuckLake Catalog 'catalog1', Schema 'bronze'")
    print("=" * 75)

    # 1. Run dlt extraction and parquet staging to MinIO bronze
    bronze_s3_url = f"s3://{MINIO_BUCKET}/bronze"
    dest = filesystem(
        bucket_url=bronze_s3_url,
        credentials={
            "aws_access_key_id": MINIO_ACCESS_KEY,
            "aws_secret_access_key": MINIO_SECRET_KEY,
            "endpoint_url": f"{'https' if MINIO_SECURE else 'http'}://{MINIO_ENDPOINT}",
        },
        layout="{table_name}/{load_id}.{file_id}.{ext}",
        use_ssl=MINIO_SECURE,
    )

    pipeline = dlt.pipeline(
        pipeline_name="bronze_ingest",
        destination=dest,
        dataset_name="",
    )

    print("\n── 1. Running dlt extraction... ──")
    load_info = pipeline.run(
        postgres_and_csv_source(),
        loader_file_format="parquet",
    )
    print(load_info)

    # Đến đây là hết phần việc của dlt.
    #
    # Bước "CREATE OR REPLACE TABLE bronze.* AS SELECT * FROM read_parquet(...)"
    # trước đây nằm ở đây đã được CHUYỂN SANG dbt (transform/models/bronze/*.sql).
    # Lý do: đó là transform, mà transform phải do dbt + DuckDB đảm nhiệm, không
    # phải Python. Nhờ vậy bronze cũng có lineage, test và docs như silver/gold.
    #
    # Chạy tiếp:  cd transform && dbt build --profiles-dir .

    print("\n" + "=" * 75)
    print("  ✅ DLT LANDING COMPLETE")
    print(f"  Parquet đã land tại: {bronze_s3_url}/bronze_ingest_dataset/<table>/")
    print("  Bước tiếp theo (dbt): cd transform && dbt build --profiles-dir .")
    print("=" * 75)


if __name__ == "__main__":
    run_pipeline()
