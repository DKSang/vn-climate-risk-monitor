"""Strict source-faithful parser for Open-Meteo Archive hourly JSON."""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from itertools import pairwise
from typing import Any

import pyarrow as pa

from vn_climate_risk_monitor.ingestion.open_meteo.contracts import (
    ARCHIVE_DATASET,
    REQUEST_CONTRACT_VERSION,
    SOURCE_NAME,
    ArchiveFileParameters,
    ArchiveRunParameters,
)
from vn_climate_risk_monitor.ingestion.state import ClaimedObject

ARCHIVE_PARSER_VERSION = "0.1.0"
ARCHIVE_KNOWN_HOURLY_FIELDS = (
    "precipitation",
    "rain",
    "weather_code",
    "soil_moisture_0_to_7cm",
    "soil_moisture_7_to_28cm",
)
KNOWN_ROOT_FIELDS = frozenset(
    {
        "latitude",
        "longitude",
        "elevation",
        "generationtime_ms",
        "utc_offset_seconds",
        "timezone",
        "timezone_abbreviation",
        "location_id",
        "hourly",
        "hourly_units",
    }
)

ARCHIVE_HOURLY_SCHEMA = pa.schema(
    [
        pa.field("bronze_row_id", pa.string(), nullable=False),
        pa.field("attempt_id", pa.string(), nullable=False),
        pa.field("logical_run_id", pa.string(), nullable=False),
        pa.field("file_id", pa.string(), nullable=False),
        pa.field("batch_index", pa.int32(), nullable=False),
        pa.field("request_location_index", pa.int32(), nullable=False),
        pa.field("source_location_id", pa.int32()),
        pa.field("hourly_index", pa.int32(), nullable=False),
        pa.field("ward_key", pa.int64(), nullable=False),
        pa.field("source_year", pa.int32(), nullable=False),
        pa.field("source_month", pa.int32(), nullable=False),
        pa.field("scheduled_at_utc", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field(
            "collection_started_at_utc",
            pa.timestamp("us", tz="UTC"),
            nullable=False,
        ),
        pa.field(
            "collection_completed_at_utc",
            pa.timestamp("us", tz="UTC"),
            nullable=False,
        ),
        pa.field("observed_time_utc", pa.timestamp("s", tz="UTC"), nullable=False),
        pa.field("interval_start_utc", pa.timestamp("s", tz="UTC"), nullable=False),
        pa.field("interval_end_utc", pa.timestamp("s", tz="UTC"), nullable=False),
        pa.field("requested_start_date", pa.date32(), nullable=False),
        pa.field("requested_end_date", pa.date32(), nullable=False),
        pa.field("grid_latitude", pa.float64()),
        pa.field("grid_longitude", pa.float64()),
        pa.field("grid_elevation", pa.float64()),
        pa.field("generationtime_ms", pa.float64()),
        pa.field("utc_offset_seconds", pa.int32()),
        pa.field("timezone", pa.string()),
        pa.field("timezone_abbreviation", pa.string()),
        pa.field("source_endpoint", pa.string(), nullable=False),
        pa.field("model_requested", pa.string(), nullable=False),
        pa.field("precipitation", pa.float64()),
        pa.field("rain", pa.float64()),
        pa.field("weather_code", pa.int32()),
        pa.field("soil_moisture_0_to_7cm", pa.float64()),
        pa.field("soil_moisture_7_to_28cm", pa.float64()),
        pa.field("hourly_units_json", pa.string(), nullable=False),
        pa.field("_source_file_path", pa.string(), nullable=False),
        pa.field("_source_file_sha256", pa.string(), nullable=False),
        pa.field("_collector_version", pa.string(), nullable=False),
        pa.field("_request_contract_version", pa.int32(), nullable=False),
        pa.field("_parser_version", pa.string(), nullable=False),
        pa.field("_rescued_data", pa.string()),
        pa.field("_ingested_at_utc", pa.timestamp("us", tz="UTC"), nullable=False),
    ]
)


class ArchiveParseError(ValueError):
    """Raised when an Archive response violates its minimum Bronze contract."""


@dataclass(frozen=True)
class ArchiveParseResult:
    table: pa.Table
    rescued_row_count: int

    @property
    def row_count(self) -> int:
        return self.table.num_rows


def _as_float(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    parsed = float(value)
    return parsed if math.isfinite(parsed) else None


def _as_int(value: Any) -> int | None:
    parsed = _as_float(value)
    if parsed is None or not parsed.is_integer():
        return None
    return int(parsed)


def _as_text(value: Any) -> str | None:
    return value if isinstance(value, str) else None


def _unix_time(value: Any, *, location_index: int, hourly_index: int) -> datetime:
    timestamp = _as_int(value)
    if timestamp is None:
        raise ArchiveParseError(
            "hourly.time must contain Unix integer timestamps "
            f"(location={location_index}, index={hourly_index})"
        )
    try:
        return datetime.fromtimestamp(timestamp, tz=UTC)
    except (OverflowError, OSError, ValueError) as error:
        raise ArchiveParseError(f"hourly.time is out of range: {timestamp}") from error


def _row_id(model: str, ward_key: int, observed_time: datetime) -> str:
    identity = f"{model}\x1f{ward_key}\x1f{int(observed_time.timestamp())}"
    return hashlib.sha256(identity.encode()).hexdigest()


def _root_value(
    payload: dict[str, Any],
    field: str,
    converter: Any,
    rescued: dict[str, Any],
) -> Any:
    value = payload.get(field)
    parsed = converter(value)
    if value is not None and parsed is None:
        rescued.setdefault("invalid_root_fields", {})[field] = value
    return parsed


def parse_archive_hourly(
    content: bytes,
    *,
    source: ClaimedObject,
    ingested_at_utc: datetime,
) -> ArchiveParseResult:
    """Parse one verified Archive response using its typed control context."""
    if ingested_at_utc.tzinfo is None or ingested_at_utc.utcoffset() is None:
        raise ValueError("ingested_at_utc must be timezone-aware")
    if source.source_name != SOURCE_NAME or source.dataset != ARCHIVE_DATASET:
        raise ArchiveParseError("Claimed object is not an Open-Meteo Archive file")
    if source.contract_version != str(REQUEST_CONTRACT_VERSION):
        raise ArchiveParseError(
            f"Unsupported Archive contract version: {source.contract_version}"
        )
    try:
        run_parameters = ArchiveRunParameters.from_mapping(source.run_parameters)
        file_parameters = ArchiveFileParameters.from_mapping(source.file_parameters)
    except (TypeError, ValueError) as error:
        raise ArchiveParseError(f"Invalid Archive control metadata: {error}") from error
    ward_keys = file_parameters.ward_keys
    if (
        source.expected_item_count != source.received_item_count
        or source.expected_item_count != len(ward_keys)
        or run_parameters.location_count < len(ward_keys)
    ):
        raise ArchiveParseError("Source file location metadata is inconsistent")
    if (
        file_parameters.start_date < run_parameters.start_date
        or file_parameters.end_date > run_parameters.end_date
    ):
        raise ArchiveParseError("Source file date window is outside its run window")
    requested_variables = run_parameters.hourly_variables
    unsupported = set(requested_variables) - set(ARCHIVE_KNOWN_HOURLY_FIELDS)
    if unsupported:
        raise ArchiveParseError(f"Unsupported Archive variables: {sorted(unsupported)}")

    try:
        payload = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ArchiveParseError("Open-Meteo response is not valid JSON") from error
    if isinstance(payload, dict):
        locations: list[Any] = [payload]
    elif isinstance(payload, list):
        locations = payload
    else:
        raise ArchiveParseError("Response root must be an object or array")
    if len(locations) != len(ward_keys):
        raise ArchiveParseError(
            f"Expected {len(ward_keys)} locations, received {len(locations)}"
        )

    expected_row_count = (
        (file_parameters.end_date - file_parameters.start_date).days + 1
    ) * 24
    expected_first = datetime.combine(
        file_parameters.start_date,
        datetime.min.time(),
        tzinfo=UTC,
    )
    expected_last = datetime.combine(
        file_parameters.end_date,
        datetime.min.time(),
        tzinfo=UTC,
    ) + timedelta(hours=23)
    ingested_at_utc = ingested_at_utc.astimezone(UTC)
    rows: list[dict[str, Any]] = []
    rescued_row_count = 0
    for location_index, (ward_key, location) in enumerate(
        zip(ward_keys, locations, strict=True)
    ):
        if not isinstance(location, dict):
            raise ArchiveParseError(
                f"Location response must be an object (index={location_index})"
            )
        if location.get("error") is True:
            raise ArchiveParseError(
                f"Open-Meteo error payload: {location.get('reason', 'unknown error')}"
            )
        hourly = location.get("hourly")
        if not isinstance(hourly, dict):
            raise ArchiveParseError(
                f"hourly must be an object (location={location_index})"
            )
        time_values = hourly.get("time")
        if not isinstance(time_values, list) or len(time_values) != expected_row_count:
            raise ArchiveParseError(
                f"Expected {expected_row_count} hourly timestamps "
                f"(location={location_index})"
            )
        parsed_times = [
            _unix_time(value, location_index=location_index, hourly_index=index)
            for index, value in enumerate(time_values)
        ]
        if any(current - previous != timedelta(hours=1) for previous, current in pairwise(parsed_times)):
            raise ArchiveParseError(
                f"hourly.time must be contiguous hourly (location={location_index})"
            )
        if parsed_times[0] != expected_first or parsed_times[-1] != expected_last:
            raise ArchiveParseError(
                f"hourly.time does not cover requested window (location={location_index})"
            )

        for field in requested_variables:
            values = hourly.get(field)
            if not isinstance(values, list) or len(values) != expected_row_count:
                raise ArchiveParseError(
                    f"{field} must contain {expected_row_count} values "
                    f"(location={location_index})"
                )
            if all(value is None for value in values):
                raise ArchiveParseError(
                    f"{field} contains only null values (location={location_index})"
                )

        unknown_root = {
            field: value for field, value in location.items() if field not in KNOWN_ROOT_FIELDS
        }
        unknown_hourly = {
            field: values
            for field, values in hourly.items()
            if field not in {*ARCHIVE_KNOWN_HOURLY_FIELDS, "time"}
        }
        units = location.get("hourly_units")
        common_rescued: dict[str, Any] = {}
        if unknown_root:
            common_rescued["unknown_root_fields"] = unknown_root
        if not isinstance(units, dict):
            common_rescued["invalid_hourly_units"] = units
            units = {}
        root_rescued = dict(common_rescued)
        source_location_id = _root_value(location, "location_id", _as_int, root_rescued)
        if source_location_id is not None and source_location_id != location_index:
            raise ArchiveParseError(
                "Open-Meteo location_id does not match response order "
                f"(expected={location_index}, received={source_location_id})"
            )
        grid_latitude = _root_value(location, "latitude", _as_float, root_rescued)
        grid_longitude = _root_value(location, "longitude", _as_float, root_rescued)
        grid_elevation = _root_value(location, "elevation", _as_float, root_rescued)
        generationtime_ms = _root_value(
            location, "generationtime_ms", _as_float, root_rescued
        )
        utc_offset_seconds = _root_value(
            location, "utc_offset_seconds", _as_int, root_rescued
        )
        timezone = _root_value(location, "timezone", _as_text, root_rescued)
        timezone_abbreviation = _root_value(
            location, "timezone_abbreviation", _as_text, root_rescued
        )
        if utc_offset_seconds != 0 or timezone != "GMT":
            raise ArchiveParseError("Archive response must use GMT with UTC offset zero")
        units_json = json.dumps(
            units, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )

        for hourly_index, observed_time in enumerate(parsed_times):
            rescued = dict(root_rescued)
            invalid_values: dict[str, Any] = {}
            weather_values: dict[str, Any] = {}
            for field in ARCHIVE_KNOWN_HOURLY_FIELDS:
                values = hourly.get(field)
                raw_value = values[hourly_index] if isinstance(values, list) else None
                converter = _as_int if field == "weather_code" else _as_float
                parsed = converter(raw_value)
                weather_values[field] = parsed
                if raw_value is not None and parsed is None:
                    invalid_values[field] = raw_value
            unknown_values = {
                field: values[hourly_index]
                for field, values in unknown_hourly.items()
                if isinstance(values, list) and hourly_index < len(values)
            }
            if unknown_values:
                rescued["unknown_hourly_values"] = unknown_values
            if invalid_values:
                rescued["invalid_hourly_values"] = invalid_values
            rescued_json = (
                json.dumps(
                    rescued,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                if rescued
                else None
            )
            if rescued_json is not None:
                rescued_row_count += 1
            rows.append(
                {
                    "bronze_row_id": _row_id(
                        run_parameters.model, ward_key, observed_time
                    ),
                    "attempt_id": str(source.attempt_id),
                    "logical_run_id": str(source.logical_run_id),
                    "file_id": str(source.file_id),
                    "batch_index": source.batch_index,
                    "request_location_index": location_index,
                    "source_location_id": source_location_id,
                    "hourly_index": hourly_index,
                    "ward_key": ward_key,
                    "source_year": file_parameters.year,
                    "source_month": file_parameters.month,
                    "scheduled_at_utc": source.scheduled_at_utc,
                    "collection_started_at_utc": source.collection_started_at_utc,
                    "collection_completed_at_utc": source.collection_completed_at_utc,
                    "observed_time_utc": observed_time,
                    "interval_start_utc": observed_time,
                    "interval_end_utc": observed_time + timedelta(hours=1),
                    "requested_start_date": file_parameters.start_date,
                    "requested_end_date": file_parameters.end_date,
                    "grid_latitude": grid_latitude,
                    "grid_longitude": grid_longitude,
                    "grid_elevation": grid_elevation,
                    "generationtime_ms": generationtime_ms,
                    "utc_offset_seconds": utc_offset_seconds,
                    "timezone": timezone,
                    "timezone_abbreviation": timezone_abbreviation,
                    "source_endpoint": source.source_uri,
                    "model_requested": run_parameters.model,
                    **weather_values,
                    "hourly_units_json": units_json,
                    "_source_file_path": source.object_key,
                    "_source_file_sha256": source.sha256,
                    "_collector_version": source.collector_version,
                    "_request_contract_version": int(source.contract_version),
                    "_parser_version": ARCHIVE_PARSER_VERSION,
                    "_rescued_data": rescued_json,
                    "_ingested_at_utc": ingested_at_utc,
                }
            )

    table = pa.Table.from_pylist(rows, schema=ARCHIVE_HOURLY_SCHEMA)
    if len(set(table.column("bronze_row_id").to_pylist())) != table.num_rows:
        raise ArchiveParseError("Parser generated duplicate bronze_row_id values")
    return ArchiveParseResult(table=table, rescued_row_count=rescued_row_count)
