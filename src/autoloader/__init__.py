"""Auto Loader: file mới trên object storage → bảng, đúng một lần mỗi file.

Không biết nguồn REST. Thêm nguồn file = 1 YAML + 1 SQL trong ``sources/``.
"""

from autoloader.checkpoint import (
    PostgresIngestionRepository,
    connect_control_plane,
)
from autoloader.config import LoaderConfig, SourceConfig
from autoloader.discovery import DiscoveredObject, discover, select_new
from autoloader.engine import AutoLoader, LoadResult
from autoloader.schema import ensure_ingestion_state

__all__ = [
    "AutoLoader",
    "DiscoveredObject",
    "LoadResult",
    "LoaderConfig",
    "PostgresIngestionRepository",
    "SourceConfig",
    "connect_control_plane",
    "discover",
    "ensure_ingestion_state",
    "select_new",
]
