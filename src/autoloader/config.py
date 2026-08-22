"""Khai báo nguồn bằng YAML — không phải bằng code.

Mỗi nguồn dữ liệu là một file YAML + một file SQL. Thêm nguồn mới KHÔNG cần
viết Python: chỉ thêm hai file đó rồi trỏ engine vào.

Ví dụ ``sources/open_meteo_forecast.yml``::

    name: open_meteo_forecast
    dataset: forecast
    scope: production

    discovery:
      prefix: bronze/files/open_meteo/forecast
      pattern: "**/response_*.json"

    transform:
      sql_file: open_meteo_forecast.sql
      target: bronze_store.tables.open_meteo_forecast

    loader:
      batch_size: 10
      lease_seconds: 300
      max_retries: 3
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class DiscoveryConfig:
    """Nơi tìm file mới trên object storage."""

    prefix: str
    pattern: str = "**/*.json"


@dataclass(frozen=True)
class TransformConfig:
    """SQL chạy trên từng file đã claim, và bảng đích."""

    sql_file: str
    target: str

    def read_sql(self, base_dir: Path) -> str:
        path = base_dir / self.sql_file
        if not path.is_file():
            raise FileNotFoundError(f"Không tìm thấy file SQL transform: {path}")
        return path.read_text(encoding="utf-8")


@dataclass(frozen=True)
class LoaderConfig:
    """Tham số vận hành của loader — tương ứng cloudFiles.* của Auto Loader."""

    batch_size: int = 10  # ~ cloudFiles.maxFilesPerTrigger
    lease_seconds: int = 300
    max_retries: int = 3
    max_batches: int = 100


@dataclass(frozen=True)
class SourceConfig:
    """Một nguồn dữ liệu hoàn chỉnh, nạp từ YAML."""

    name: str
    dataset: str
    discovery: DiscoveryConfig
    transform: TransformConfig
    scope: str = "production"
    loader: LoaderConfig = field(default_factory=LoaderConfig)
    base_dir: Path = field(default=Path("."))

    @classmethod
    def from_yaml(cls, path: str | Path) -> SourceConfig:
        config_path = Path(path)
        raw: dict[str, Any] = yaml.safe_load(config_path.read_text(encoding="utf-8"))
        missing = {"name", "dataset", "discovery", "transform"} - raw.keys()
        if missing:
            raise ValueError(f"{config_path}: thiếu khoá bắt buộc {sorted(missing)}")
        return cls(
            name=raw["name"],
            dataset=raw["dataset"],
            scope=raw.get("scope", "production"),
            discovery=DiscoveryConfig(**raw["discovery"]),
            transform=TransformConfig(**raw["transform"]),
            loader=LoaderConfig(**raw.get("loader", {})),
            base_dir=config_path.parent,
        )

    @property
    def sql(self) -> str:
        return self.transform.read_sql(self.base_dir)
