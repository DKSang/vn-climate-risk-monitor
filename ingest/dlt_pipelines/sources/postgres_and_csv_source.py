"""
dlt source extracting from PostgreSQL database and Raw CSV coordinates.

Resources:
  - raw_provinces (from PostgreSQL public.provinces)
  - raw_wards (from PostgreSQL public.wards)
  - raw_administrative_units (from PostgreSQL public.administrative_units)
  - raw_administrative_regions (from PostgreSQL public.administrative_regions)
  - raw_locations_coordinates (from CSV dim_location.csv)
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Iterator
import dlt
import duckdb
import pandas as pd
import pendulum

# PostgreSQL connection params
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "127.0.0.1")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "vnclimate")
POSTGRES_USER = os.getenv("POSTGRES_USER", "vnclimate")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "vnclimate")

CSV_COORDINATES_PATH = Path(
    "lake/raw/geography/crawl/coordinates_locations/full/2026/08/20/dim_location.csv"
)


def get_postgres_cursor() -> duckdb.DuckDBPyConnection:
    """Connect to PostgreSQL via DuckDB postgres scanner."""
    con = duckdb.connect()
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(
        f"ATTACH 'dbname={POSTGRES_DB} host={POSTGRES_HOST} port={POSTGRES_PORT} "
        f"user={POSTGRES_USER} password={POSTGRES_PASSWORD}' AS pg (TYPE postgres);"
    )
    return con


@dlt.source(name="postgres_and_csv_source")
def postgres_and_csv_source() -> Iterator[dlt.resource]:
    """Source extracting administrative tables from PostgreSQL and coordinates from CSV."""
    fetched_at = pendulum.now("UTC").isoformat()

    @dlt.resource(name="raw_provinces", write_disposition="replace")
    def raw_provinces() -> Iterator[dict]:
        con = get_postgres_cursor()
        df = con.sql("SELECT * FROM pg.public.provinces").df()
        for row in df.to_dict(orient="records"):
            row["_fetched_at"] = fetched_at
            row["_source"] = "postgres.public.provinces"
            yield row

    @dlt.resource(name="raw_wards", write_disposition="replace")
    def raw_wards() -> Iterator[dict]:
        con = get_postgres_cursor()
        df = con.sql("SELECT * FROM pg.public.wards").df()
        for row in df.to_dict(orient="records"):
            row["_fetched_at"] = fetched_at
            row["_source"] = "postgres.public.wards"
            yield row

    @dlt.resource(name="raw_administrative_units", write_disposition="replace")
    def raw_administrative_units() -> Iterator[dict]:
        con = get_postgres_cursor()
        df = con.sql("SELECT * FROM pg.public.administrative_units").df()
        for row in df.to_dict(orient="records"):
            row["_fetched_at"] = fetched_at
            row["_source"] = "postgres.public.administrative_units"
            yield row

    @dlt.resource(name="raw_administrative_regions", write_disposition="replace")
    def raw_administrative_regions() -> Iterator[dict]:
        con = get_postgres_cursor()
        df = con.sql("SELECT * FROM pg.public.administrative_regions").df()
        for row in df.to_dict(orient="records"):
            row["_fetched_at"] = fetched_at
            row["_source"] = "postgres.public.administrative_regions"
            yield row

    @dlt.resource(name="raw_locations_coordinates", write_disposition="replace")
    def raw_locations_coordinates() -> Iterator[dict]:
        if CSV_COORDINATES_PATH.exists():
            df = pd.read_csv(CSV_COORDINATES_PATH)
            for row in df.to_dict(orient="records"):
                row["_fetched_at"] = fetched_at
                row["_source"] = str(CSV_COORDINATES_PATH)
                yield row

    return (
        raw_provinces,
        raw_wards,
        raw_administrative_units,
        raw_administrative_regions,
        raw_locations_coordinates,
    )
