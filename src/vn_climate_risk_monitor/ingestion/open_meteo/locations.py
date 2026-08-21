"""Approved Open-Meteo request locations shared by source-specific pipelines."""

from __future__ import annotations

import duckdb

from vn_climate_risk_monitor.ingestion.open_meteo.contracts import (
    RequestedLocation,
    split_location_batches,
)

EXPECTED_HANOI_LOCATION_COUNT = 126


def load_hanoi_locations(
    connection: duckdb.DuckDBPyConnection,
    *,
    expected_count: int = EXPECTED_HANOI_LOCATION_COUNT,
) -> tuple[RequestedLocation, ...]:
    """Read the approved Gold dimension in deterministic business-key order."""
    rows = connection.execute(
        """
        SELECT ward_key, ward_code, latitude, longitude
        FROM gold.dim_hanoi_ward
        ORDER BY ward_key
        """
    ).fetchall()
    locations = tuple(
        RequestedLocation(
            ward_key=int(ward_key),
            ward_code=str(ward_code),
            latitude=float(latitude),
            longitude=float(longitude),
        )
        for ward_key, ward_code, latitude, longitude in rows
    )
    if len(locations) != expected_count:
        raise ValueError(
            f"Expected {expected_count} Hanoi locations, received {len(locations)}"
        )
    split_location_batches(locations, batch_size=max(1, len(locations)))
    return locations
