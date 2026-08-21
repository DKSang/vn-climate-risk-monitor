"""Pure, versioned data contracts for Open-Meteo collection."""

from __future__ import annotations

import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
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
ARCHIVE_HOURLY_VARIABLES = (
    "precipitation",
    "rain",
    "weather_code",
    "soil_moisture_0_to_7cm",
    "soil_moisture_7_to_28cm",
)
SOURCE_NAME = "open_meteo"
FORECAST_DATASET = "forecast"
ARCHIVE_DATASET = "historical_weather_hourly"
ARCHIVE_MIN_YEAR = 1940


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


def _date_from_mapping(value: object, field: str) -> date:
    if not isinstance(value, str):
        raise TypeError(f"{field} must be an ISO date string")
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise ValueError(f"{field} must be a valid ISO date") from error


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


@dataclass(frozen=True)
class ForecastRunParameters:
    """Forecast-specific run context persisted in generic control metadata."""

    model: str
    forecast_hours: int
    hourly_variables: tuple[str, ...]
    location_count: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "model", _required_text(self.model, "model"))
        if self.forecast_hours < 1:
            raise ValueError("forecast_hours must be positive")
        if self.location_count < 1:
            raise ValueError("location_count must be positive")
        if not self.hourly_variables:
            raise ValueError("hourly_variables must not be empty")
        if len(self.hourly_variables) != len(set(self.hourly_variables)):
            raise ValueError("hourly_variables must not contain duplicates")

    def to_mapping(self) -> dict[str, object]:
        return {
            "model": self.model,
            "forecast_hours": self.forecast_hours,
            "hourly_variables": list(self.hourly_variables),
            "location_count": self.location_count,
        }

    @classmethod
    def from_mapping(cls, values: Mapping[str, object]) -> ForecastRunParameters:
        hourly_variables = values.get("hourly_variables")
        if not isinstance(hourly_variables, list) or not all(
            isinstance(value, str) for value in hourly_variables
        ):
            raise TypeError("hourly_variables must be an array of strings")
        model = values.get("model")
        forecast_hours = values.get("forecast_hours")
        location_count = values.get("location_count")
        if not isinstance(model, str):
            raise TypeError("model must be a string")
        if isinstance(forecast_hours, bool) or not isinstance(forecast_hours, int):
            raise TypeError("forecast_hours must be an integer")
        if isinstance(location_count, bool) or not isinstance(location_count, int):
            raise TypeError("location_count must be an integer")
        return cls(
            model=model,
            forecast_hours=forecast_hours,
            hourly_variables=tuple(hourly_variables),
            location_count=location_count,
        )


@dataclass(frozen=True)
class ForecastFileParameters:
    """Ordered location identity needed only by the forecast parser."""

    ward_keys: tuple[int, ...]

    def __post_init__(self) -> None:
        if not self.ward_keys:
            raise ValueError("ward_keys must not be empty")
        if any(isinstance(value, bool) or value < 1 for value in self.ward_keys):
            raise ValueError("ward_keys must contain positive integers")
        if len(self.ward_keys) != len(set(self.ward_keys)):
            raise ValueError("ward_keys must be unique")

    def to_mapping(self) -> dict[str, object]:
        return {"ward_keys": list(self.ward_keys)}

    @classmethod
    def from_mapping(cls, values: Mapping[str, object]) -> ForecastFileParameters:
        ward_keys = values.get("ward_keys")
        if not isinstance(ward_keys, list) or not all(
            isinstance(value, int) and not isinstance(value, bool)
            for value in ward_keys
        ):
            raise TypeError("ward_keys must be an array of integers")
        return cls(ward_keys=tuple(ward_keys))


@dataclass(frozen=True)
class ArchiveRunParameters:
    """Archive-specific context for one full- or partial-year source period."""

    year: int
    start_date: date
    end_date: date
    model: str
    hourly_variables: tuple[str, ...]
    location_count: int
    request_granularity: str = "month"

    def __post_init__(self) -> None:
        object.__setattr__(self, "model", _required_text(self.model, "model"))
        if self.year < ARCHIVE_MIN_YEAR:
            raise ValueError(f"year must be at least {ARCHIVE_MIN_YEAR}")
        if self.start_date.year != self.year or self.end_date.year != self.year:
            raise ValueError("archive run dates must stay within year")
        if self.end_date < self.start_date:
            raise ValueError("end_date must not precede start_date")
        if self.location_count < 1:
            raise ValueError("location_count must be positive")
        if not self.hourly_variables:
            raise ValueError("hourly_variables must not be empty")
        if len(self.hourly_variables) != len(set(self.hourly_variables)):
            raise ValueError("hourly_variables must not contain duplicates")
        if self.request_granularity != "month":
            raise ValueError("request_granularity must be month")

    def to_mapping(self) -> dict[str, object]:
        return {
            "year": self.year,
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
            "model": self.model,
            "hourly_variables": list(self.hourly_variables),
            "location_count": self.location_count,
            "request_granularity": self.request_granularity,
        }

    @classmethod
    def from_mapping(cls, values: Mapping[str, object]) -> ArchiveRunParameters:
        year = values.get("year")
        model = values.get("model")
        hourly_variables = values.get("hourly_variables")
        location_count = values.get("location_count")
        request_granularity = values.get("request_granularity")
        if isinstance(year, bool) or not isinstance(year, int):
            raise TypeError("year must be an integer")
        if not isinstance(model, str):
            raise TypeError("model must be a string")
        if not isinstance(hourly_variables, list) or not all(
            isinstance(value, str) for value in hourly_variables
        ):
            raise TypeError("hourly_variables must be an array of strings")
        if isinstance(location_count, bool) or not isinstance(location_count, int):
            raise TypeError("location_count must be an integer")
        if not isinstance(request_granularity, str):
            raise TypeError("request_granularity must be a string")
        return cls(
            year=year,
            start_date=_date_from_mapping(values.get("start_date"), "start_date"),
            end_date=_date_from_mapping(values.get("end_date"), "end_date"),
            model=model,
            hourly_variables=tuple(hourly_variables),
            location_count=location_count,
            request_granularity=request_granularity,
        )


@dataclass(frozen=True)
class ArchiveFileParameters:
    """Archive parser context for one within-month period and location batch."""

    year: int
    month: int
    start_date: date
    end_date: date
    location_batch_index: int
    ward_keys: tuple[int, ...]

    def __post_init__(self) -> None:
        if self.year < ARCHIVE_MIN_YEAR:
            raise ValueError(f"year must be at least {ARCHIVE_MIN_YEAR}")
        if not 1 <= self.month <= 12:
            raise ValueError("month must be between 1 and 12")
        if self.start_date.year != self.year or self.end_date.year != self.year:
            raise ValueError("archive file dates must stay within year")
        if self.start_date.month != self.month or self.end_date.month != self.month:
            raise ValueError("archive file dates must stay within month")
        if self.end_date < self.start_date:
            raise ValueError("end_date must not precede start_date")
        if self.location_batch_index < 0:
            raise ValueError("location_batch_index must not be negative")
        if not self.ward_keys:
            raise ValueError("ward_keys must not be empty")
        if any(isinstance(value, bool) or value < 1 for value in self.ward_keys):
            raise ValueError("ward_keys must contain positive integers")
        if len(self.ward_keys) != len(set(self.ward_keys)):
            raise ValueError("ward_keys must be unique")

    def to_mapping(self) -> dict[str, object]:
        return {
            "year": self.year,
            "month": self.month,
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
            "location_batch_index": self.location_batch_index,
            "ward_keys": list(self.ward_keys),
        }

    @classmethod
    def from_mapping(cls, values: Mapping[str, object]) -> ArchiveFileParameters:
        year = values.get("year")
        month = values.get("month")
        location_batch_index = values.get("location_batch_index")
        ward_keys = values.get("ward_keys")
        if isinstance(year, bool) or not isinstance(year, int):
            raise TypeError("year must be an integer")
        if isinstance(month, bool) or not isinstance(month, int):
            raise TypeError("month must be an integer")
        if isinstance(location_batch_index, bool) or not isinstance(
            location_batch_index, int
        ):
            raise TypeError("location_batch_index must be an integer")
        if not isinstance(ward_keys, list) or not all(
            isinstance(value, int) and not isinstance(value, bool)
            for value in ward_keys
        ):
            raise TypeError("ward_keys must be an array of integers")
        return cls(
            year=year,
            month=month,
            start_date=_date_from_mapping(values.get("start_date"), "start_date"),
            end_date=_date_from_mapping(values.get("end_date"), "end_date"),
            location_batch_index=location_batch_index,
            ward_keys=tuple(ward_keys),
        )


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


@dataclass(frozen=True)
class ArchiveRequestContract:
    """HTTP contract for one within-month archive window and location batch."""

    endpoint: str
    model: str
    start_date: date
    end_date: date
    batch_index: int
    requested_at_utc: datetime
    locations: tuple[RequestedLocation, ...]
    hourly_variables: tuple[str, ...] = ARCHIVE_HOURLY_VARIABLES
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
        if self.end_date < self.start_date:
            raise ValueError("end_date must not precede start_date")
        if self.start_date.year != self.end_date.year:
            raise ValueError("archive request must stay within one year")
        if self.start_date.month != self.end_date.month:
            raise ValueError("archive request must stay within one month")
        if self.batch_index < 0:
            raise ValueError("batch_index must not be negative")
        if not self.locations:
            raise ValueError("locations must not be empty")
        ward_keys = [location.ward_key for location in self.locations]
        if ward_keys != sorted(ward_keys):
            raise ValueError("locations must be ordered by ward_key")
        if len(ward_keys) != len(set(ward_keys)):
            raise ValueError("ward_key values must be unique within a request")
        if not self.hourly_variables:
            raise ValueError("hourly_variables must not be empty")
        if len(self.hourly_variables) != len(set(self.hourly_variables)):
            raise ValueError("hourly_variables must not contain duplicates")
        _utc_datetime(self.requested_at_utc, "requested_at_utc")

    @property
    def parameters(self) -> dict[str, object]:
        return {
            "hourly": list(self.hourly_variables),
            "models": self.model,
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
            "timeformat": self.timeformat,
            "timezone": self.timezone,
            "precipitation_unit": self.precipitation_unit,
            "cell_selection": self.cell_selection,
        }

    @property
    def api_query_params(self) -> dict[str, str]:
        return {
            "latitude": ",".join(_coordinate(item.latitude) for item in self.locations),
            "longitude": ",".join(
                _coordinate(item.longitude) for item in self.locations
            ),
            "hourly": ",".join(self.hourly_variables),
            "models": self.model,
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
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
            "requested_at_utc": _utc_iso(self.requested_at_utc, "requested_at_utc"),
            "parameters": self.parameters,
            "locations": [
                location.to_dict(index) for index, location in enumerate(self.locations)
            ],
        }

    def to_json_bytes(self) -> bytes:
        return _json_bytes(self.to_dict())
