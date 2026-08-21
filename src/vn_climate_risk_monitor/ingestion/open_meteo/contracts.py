"""Pure, versioned data contracts for Open-Meteo collection."""

from __future__ import annotations

import json
import math
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, ClassVar
from urllib.parse import urlparse

REQUEST_CONTRACT_VERSION = 1
COLLECTOR_VERSION = "0.2.0"
FORECAST_HOURLY_VARIABLES = (
    "precipitation",
    "rain",
    "showers",
    "precipitation_probability",
    "weather_code",
)


def _required_text(value: str, field: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field} must not be empty")
    return normalized


def _http_url(value: str, field: str) -> str:
    normalized = _required_text(value, field)
    parsed = urlparse(normalized)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError(f"{field} must be an HTTP(S) URL")
    return normalized


def _utc_datetime(value: datetime, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(UTC)


def _utc_iso(value: datetime, field: str) -> str:
    return (
        _utc_datetime(value, field).isoformat(timespec="seconds").replace("+00:00", "Z")
    )


def _json_bytes(payload: dict[str, Any]) -> bytes:
    return (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode()


def _coordinate(value: float) -> str:
    return f"{value:.6f}".rstrip("0").rstrip(".")


@dataclass(frozen=True)
class RequestedLocation:
    """One approved location from the ordered ward snapshot."""

    ward_key: int
    ward_code: str
    latitude: float
    longitude: float

    def __post_init__(self) -> None:
        if self.ward_key < 1:
            raise ValueError("ward_key must be positive")
        object.__setattr__(
            self, "ward_code", _required_text(self.ward_code, "ward_code")
        )
        if not math.isfinite(self.latitude) or not -90 <= self.latitude <= 90:
            raise ValueError("latitude must be finite and between -90 and 90")
        if not math.isfinite(self.longitude) or not -180 <= self.longitude <= 180:
            raise ValueError("longitude must be finite and between -180 and 180")

    def to_dict(self, request_location_index: int) -> dict[str, int | float | str]:
        return {
            "request_location_index": request_location_index,
            "ward_key": self.ward_key,
            "ward_code": self.ward_code,
            "latitude": self.latitude,
            "longitude": self.longitude,
        }


def split_location_batches(
    locations: Sequence[RequestedLocation],
    batch_size: int,
) -> tuple[tuple[RequestedLocation, ...], ...]:
    """Sort the approved snapshot by ward key and return deterministic batches."""
    if batch_size < 1:
        raise ValueError("batch_size must be at least 1")
    ordered = tuple(sorted(locations, key=lambda location: location.ward_key))
    if not ordered:
        raise ValueError("locations must not be empty")
    ward_keys = [location.ward_key for location in ordered]
    ward_codes = [location.ward_code for location in ordered]
    if len(ward_keys) != len(set(ward_keys)):
        raise ValueError("ward_key values must be unique")
    if len(ward_codes) != len(set(ward_codes)):
        raise ValueError("ward_code values must be unique")
    return tuple(
        ordered[offset : offset + batch_size]
        for offset in range(0, len(ordered), batch_size)
    )


@dataclass(frozen=True)
class ForecastRequestContract:
    """Audit envelope plus HTTP parameters for one ordered location batch."""

    endpoint: str
    model: str
    forecast_hours: int
    batch_index: int
    scheduled_at_utc: datetime
    requested_at_utc: datetime
    locations: tuple[RequestedLocation, ...]
    hourly_variables: tuple[str, ...] = FORECAST_HOURLY_VARIABLES
    request_contract_version: int = REQUEST_CONTRACT_VERSION

    method: ClassVar[str] = "GET"
    timeformat: ClassVar[str] = "unixtime"
    timezone: ClassVar[str] = "GMT"
    precipitation_unit: ClassVar[str] = "mm"
    cell_selection: ClassVar[str] = "land"

    def __post_init__(self) -> None:
        object.__setattr__(self, "endpoint", _http_url(self.endpoint, "endpoint"))
        object.__setattr__(self, "model", _required_text(self.model, "model"))
        if self.request_contract_version < 1:
            raise ValueError("request_contract_version must be positive")
        if self.forecast_hours < 1:
            raise ValueError("forecast_hours must be positive")
        if self.batch_index < 0:
            raise ValueError("batch_index must not be negative")
        if not self.locations:
            raise ValueError("locations must not be empty")
        ward_keys = [location.ward_key for location in self.locations]
        ward_codes = [location.ward_code for location in self.locations]
        if ward_keys != sorted(ward_keys):
            raise ValueError("locations must be ordered by ward_key")
        if len(ward_keys) != len(set(ward_keys)):
            raise ValueError("ward_key values must be unique within a request")
        if len(ward_codes) != len(set(ward_codes)):
            raise ValueError("ward_code values must be unique within a request")
        if not self.hourly_variables:
            raise ValueError("hourly_variables must not be empty")
        if len(self.hourly_variables) != len(set(self.hourly_variables)):
            raise ValueError("hourly_variables must not contain duplicates")
        scheduled = _utc_datetime(self.scheduled_at_utc, "scheduled_at_utc")
        requested = _utc_datetime(self.requested_at_utc, "requested_at_utc")
        if requested < scheduled:
            raise ValueError("requested_at_utc must not precede scheduled_at_utc")

    @property
    def parameters(self) -> dict[str, object]:
        return {
            "hourly": list(self.hourly_variables),
            "models": self.model,
            "forecast_hours": self.forecast_hours,
            "timeformat": self.timeformat,
            "timezone": self.timezone,
            "precipitation_unit": self.precipitation_unit,
            "cell_selection": self.cell_selection,
        }

    @property
    def api_query_params(self) -> dict[str, str | int]:
        """Return values accepted directly by ``requests`` as query parameters."""
        return {
            "latitude": ",".join(_coordinate(item.latitude) for item in self.locations),
            "longitude": ",".join(
                _coordinate(item.longitude) for item in self.locations
            ),
            "hourly": ",".join(self.hourly_variables),
            "models": self.model,
            "forecast_hours": self.forecast_hours,
            "timeformat": self.timeformat,
            "timezone": self.timezone,
            "precipitation_unit": self.precipitation_unit,
            "cell_selection": self.cell_selection,
        }

    def to_dict(self) -> dict[str, object]:
        return {
            "request_contract_version": self.request_contract_version,
            "endpoint": self.endpoint,
            "method": self.method,
            "batch_index": self.batch_index,
            "scheduled_at_utc": _utc_iso(self.scheduled_at_utc, "scheduled_at_utc"),
            "requested_at_utc": _utc_iso(self.requested_at_utc, "requested_at_utc"),
            "parameters": self.parameters,
            "locations": [
                location.to_dict(index) for index, location in enumerate(self.locations)
            ],
        }

    def to_json_bytes(self) -> bytes:
        return _json_bytes(self.to_dict())
