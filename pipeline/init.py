"""Create everything the pipeline writes to. Safe to run before every DAG run."""

from __future__ import annotations

from pipeline import lake
from pipeline.job_run import create_meta_tables
from pipeline.settings import load_settings


def main() -> None:
    bucket = load_settings().minio.bucket
    client = lake.minio_client()
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

    with lake.connect() as con:
        for schema in ("silver", "gold"):
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    create_meta_tables()
    print(f"Lake ready: bucket '{bucket}', schemas silver/gold, meta tables")
