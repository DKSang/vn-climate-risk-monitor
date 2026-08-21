"""Source-faithful Open-Meteo hourly JSON parser for Bronze."""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from itertools import pairwise
from typing import Any

import pyarrow as pa

from vn_climate_risk_monitor.ingestion.state import ClaimedFile

PARSER_VERSION = "0.2.0"
KNOWN_HOURLY_FIELDS = (
    "precipitation",
    "rain",
    "showers",
    "precipitation_probability",
    "weather_code",
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

FORECAST_HOURLY_SCHEMA = pa.schema(
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
        pa.field("valid_time_utc", pa.timestamp("s", tz="UTC")),
        pa.field("interval_start_utc", pa.timestamp("s", tz="UTC")),
        pa.field("interval_end_utc", pa.timestamp("s", tz="UTC")),
        pa.field("grid_latitude", pa.float64()),
        pa.field("grid_longitude", pa.float64()),
        pa.field("grid_elevation", pa.float64()),
        pa.field("generationtime_ms", pa.float64()),
        pa.field("utc_offset_seconds", pa.int32()),
        pa.field("timezone", pa.string()),
        pa.field("timezone_abbreviation", pa.string()),
        pa.field("source_endpoint", pa.string(), nullable=False),
        pa.field("model_requested", pa.string(), nullable=False),
        pa.field("forecast_hours", pa.int32(), nullable=False),
        pa.field("precipitation", pa.float64()),
        pa.field("rain", pa.float64()),
        pa.field("showers", pa.float64()),
        pa.field("precipitation_probability", pa.float64()),
        pa.field("weather_code", pa.int32()),
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


class ForecastParseError(ValueError):
    """Raised when a response cannot satisfy the minimum Bronze contract."""


@dataclass(frozen=True)
class ForecastParseResult:
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
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    parsed = float(value)
    if not math.isfinite(parsed) or not parsed.is_integer():
        return None
    return int(parsed)


def _as_text(value: Any) -> str | None:
    return value if isinstance(value, str) else None


def _unix_time(value: Any, *, location_index: int, hourly_index: int) -> datetime:
    timestamp = _as_int(value)
    if timestamp is None:
        raise ForecastParseError(
            "hourly.time must contain Unix integer timestamps "
            f"(location={location_index}, index={hourly_index})"
        )
    try:
        return datetime.fromtimestamp(timestamp, tz=UTC)
    except (OverflowError, OSError, ValueError) as error:
        raise ForecastParseError(f"hourly.time is out of range: {timestamp}") from error


def _array_value(values: Any, index: int) -> tuple[Any, bool]:
    if not isinstance(values, list) or index >= len(values):
        return None, True
    return values[index], False


def _row_id(source: ClaimedFile, location_index: int, hourly_index: int) -> str:
    identity = (
        f"{source.attempt_id}\x1f{source.file_id}\x1f{location_index}\x1f{hourly_index}"
    )
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


def parse_forecast_hourly(
    content: bytes,
    *,
    source: ClaimedFile,
    ingested_at_utc: datetime,
) -> ForecastParseResult:
    """Parse one verified response file without applying business rules."""
    if ingested_at_utc.tzinfo is None or ingested_at_utc.utcoffset() is None:
        raise ValueError("ingested_at_utc must be timezone-aware")
    if (
        source.expected_location_count != source.received_location_count
        or source.expected_location_count != len(source.ward_keys)
    ):
        raise ForecastParseError("Source file location metadata is inconsistent")
    ingested_at_utc = ingested_at_utc.astimezone(UTC)
    try:
        payload = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ForecastParseError("Open-Meteo response is not valid JSON") from error

    locations: list[Any]
    if isinstance(payload, dict):
        locations = [payload]
    elif isinstance(payload, list):
        locations = payload
    else:
        raise ForecastParseError("Response root must be an object or array")
    if len(locations) != len(source.ward_keys):
        raise ForecastParseError(
            f"Expected {len(source.ward_keys)} locations, received {len(locations)}"
        )

    rows: list[dict[str, Any]] = []
    rescued_row_count = 0
    for location_index, (ward_key, location) in enumerate(
        zip(source.ward_keys, locations, strict=True)
    ):
        if not isinstance(location, dict):
            raise ForecastParseError(
                f"Location response must be an object (index={location_index})"
            )
        if location.get("error") is True:
            raise ForecastParseError(
                f"Open-Meteo error payload: {location.get('reason', 'unknown error')}"
            )
        hourly = location.get("hourly")
        if not isinstance(hourly, dict):
            raise ForecastParseError(
                f"hourly must be an object (location={location_index})"
            )
        time_values = hourly.get("time")
        if not isinstance(time_values, list) or not time_values:
            raise ForecastParseError(
                f"hourly.time must be a non-empty array (location={location_index})"
            )
        parsed_times = [
            _unix_time(value, location_index=location_index, hourly_index=index)
            for index, value in enumerate(time_values)
        ]
        if any(current <= previous for previous, current in pairwise(parsed_times)):
            raise ForecastParseError(
                f"hourly.time must be strictly increasing (location={location_index})"
            )

        array_lengths = {
            field: len(values)
            for field, values in hourly.items()
            if isinstance(values, list)
        }
        row_count = max(array_lengths.values(), default=0)
        if row_count == 0:
            raise ForecastParseError(
                f"hourly contains no arrays (location={location_index})"
            )
        length_mismatch = (
            array_lengths if len(set(array_lengths.values())) > 1 else None
        )
        invalid_hourly_fields = {
            field: value
            for field, value in hourly.items()
            if not isinstance(value, list)
        }
        unknown_root = {
            field: value
            for field, value in location.items()
            if field not in KNOWN_ROOT_FIELDS
        }
        unknown_hourly = {
            field: values
            for field, values in hourly.items()
            if field not in {*KNOWN_HOURLY_FIELDS, "time"}
        }
        units = location.get("hourly_units")
        common_rescued: dict[str, Any] = {}
        if length_mismatch is not None:
            common_rescued["array_lengths"] = length_mismatch
        if invalid_hourly_fields:
            common_rescued["invalid_hourly_fields"] = invalid_hourly_fields
        if unknown_root:
            common_rescued["unknown_root_fields"] = unknown_root
        if not isinstance(units, dict):
            common_rescued["invalid_hourly_units"] = units
            units = {}

        root_rescued = dict(common_rescued)
        source_location_id = _root_value(location, "location_id", _as_int, root_rescued)
        if source_location_id is not None and source_location_id != location_index:
            raise ForecastParseError(
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
        units_json = json.dumps(
            units, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )

        for hourly_index in range(row_count):
            rescued = dict(root_rescued)
            missing_fields: list[str] = []
            invalid_values: dict[str, Any] = {}
            time_value = (
                parsed_times[hourly_index] if hourly_index < len(parsed_times) else None
            )
            if time_value is None:
                missing_fields.append("time")

            weather_values: dict[str, Any] = {}
            for field in KNOWN_HOURLY_FIELDS:
                raw_value, missing = _array_value(hourly.get(field), hourly_index)
                if missing:
                    missing_fields.append(field)
                    weather_values[field] = None
                    continue
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
            if missing_fields:
                rescued["missing_hourly_values"] = missing_fields
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
                    "bronze_row_id": _row_id(source, location_index, hourly_index),
                    "attempt_id": str(source.attempt_id),
                    "logical_run_id": str(source.logical_run_id),
                    "file_id": str(source.file_id),
                    "batch_index": source.batch_index,
                    "request_location_index": location_index,
                    "source_location_id": source_location_id,
                    "hourly_index": hourly_index,
                    "ward_key": ward_key,
                    "scheduled_at_utc": source.scheduled_at_utc,
                    "collection_started_at_utc": source.collection_started_at_utc,
                    "collection_completed_at_utc": source.collection_completed_at_utc,
                    "valid_time_utc": time_value,
                    "interval_start_utc": time_value,
                    "interval_end_utc": (
                        time_value + timedelta(hours=1)
                        if time_value is not None
                        else None
                    ),
                    "grid_latitude": grid_latitude,
                    "grid_longitude": grid_longitude,
                    "grid_elevation": grid_elevation,
                    "generationtime_ms": generationtime_ms,
                    "utc_offset_seconds": utc_offset_seconds,
                    "timezone": timezone,
                    "timezone_abbreviation": timezone_abbreviation,
                    "source_endpoint": source.source_endpoint,
                    "model_requested": source.model_requested,
                    "forecast_hours": source.forecast_hours,
                    **weather_values,
                    "hourly_units_json": units_json,
                    "_source_file_path": source.object_key,
                    "_source_file_sha256": source.sha256,
                    "_collector_version": source.collector_version,
                    "_request_contract_version": source.request_contract_version,
                    "_parser_version": PARSER_VERSION,
                    "_rescued_data": rescued_json,
                    "_ingested_at_utc": ingested_at_utc,
                }
            )

    table = pa.Table.from_pylist(rows, schema=FORECAST_HOURLY_SCHEMA)
    if len(set(table.column("bronze_row_id").to_pylist())) != table.num_rows:
        raise ForecastParseError("Parser generated duplicate bronze_row_id values")
    return ForecastParseResult(table=table, rescued_row_count=rescued_row_count)
