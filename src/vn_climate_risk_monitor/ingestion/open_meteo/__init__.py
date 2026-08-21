"""Versioned contracts for Open-Meteo ingestion."""

from vn_climate_risk_monitor.ingestion.open_meteo.contracts import (
    COLLECTOR_VERSION,
    FORECAST_HOURLY_VARIABLES,
    REQUEST_CONTRACT_VERSION,
    ForecastRequestContract,
    RequestedLocation,
    split_location_batches,
)
from vn_climate_risk_monitor.ingestion.open_meteo.parser import (
    FORECAST_HOURLY_SCHEMA,
    PARSER_VERSION,
    ForecastParseError,
    ForecastParseResult,
    parse_forecast_hourly,
)
from vn_climate_risk_monitor.storage.models import SourceObjectMetadata

__all__ = [
    "COLLECTOR_VERSION",
    "FORECAST_HOURLY_SCHEMA",
    "FORECAST_HOURLY_VARIABLES",
    "PARSER_VERSION",
    "REQUEST_CONTRACT_VERSION",
    "ForecastParseError",
    "ForecastParseResult",
    "ForecastRequestContract",
    "RequestedLocation",
    "SourceObjectMetadata",
    "parse_forecast_hourly",
    "split_location_batches",
]
