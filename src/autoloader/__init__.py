"""autoloader — nạp file mới từ object storage vào bảng, đúng một lần mỗi file.

Mô phỏng Databricks Auto Loader (``cloudFiles``) trên nền DuckDB + Postgres:

  * **Directory listing** phát hiện file mới, độc lập với việc ai ghi ra file
  * **Checkpoint** theo object key trong Postgres — exactly-once
  * **Lease + retry** theo từng file, một file hỏng không chặn cả lô
  * **Micro-batch** giới hạn bởi ``batch_size`` (~ ``maxFilesPerTrigger``)
  * **Transform bằng SQL** — DuckDB đọc file, Python chỉ điều phối

Package này KHÔNG phụ thuộc dự án nào. Thêm nguồn mới = thêm 1 file YAML +
1 file SQL, không viết Python.

Chiều ngược (API → file) nằm ở package ``activities``.

    from autoloader import AutoLoader, SourceConfig

    loader = AutoLoader(
        config=SourceConfig.from_yaml("sources/my_source.yml"),
        checkpoint=PostgresIngestionRepository(pg_connection),
        object_client=minio_client,
        sql=duckdb_connection,
        bucket="my-bucket",
    )
    print(loader.load())
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
