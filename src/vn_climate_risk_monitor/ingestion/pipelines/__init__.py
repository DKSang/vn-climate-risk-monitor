"""Operational entrypoints that compose collectors and loaders."""

from vn_climate_risk_monitor.ingestion.pipelines.open_meteo_forecast import (
    ForecastPipeline,
    PipelineRunSummary,
)

__all__ = ["ForecastPipeline", "PipelineRunSummary"]
