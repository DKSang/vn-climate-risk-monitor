"""Incremental ingestion primitives.

Collectors persist immutable source payloads under ``bronze/files``. Loaders
parse completed source runs into DuckLake Bronze tables. No source-specific
network collector is implemented here yet.
"""

from vn_climate_risk_monitor.ingestion.layout import BronzeFilesLayout

__all__ = ["BronzeFilesLayout"]
