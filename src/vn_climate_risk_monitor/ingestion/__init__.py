"""Incremental ingestion primitives.

Collectors persist immutable source payloads under ``bronze/files`` and record
run/file state in native PostgreSQL tables. Loaders claim completed source files
and parse them into DuckLake Bronze tables.
"""

from vn_climate_risk_monitor.ingestion.layout import BronzeFilesLayout

__all__ = ["BronzeFilesLayout"]
