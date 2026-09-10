"""Khai báo nguồn bằng YAML — không phải bằng code.

Mỗi nguồn dữ liệu là một file YAML + một file SQL. Thêm nguồn mới KHÔNG cần
viết Python: chỉ thêm hai file đó rồi trỏ engine vào.

Ví dụ ``sources/open_meteo_forecast.yml``::

    name: open_meteo_forecast
    dataset: forecast

    discovery:
      prefix: bronze/files/open_meteo/forecast
      pattern: "**/response_*.json"

    transform:
      sql_file: open_meteo_forecast.sql
      target: catalog1.silver.stg_weather_forecast

``parameters`` cho phép NHIỀU NGUỒN DÙNG CHUNG MỘT FILE SQL. Ví dụ ERA5 và
ECMWF IFS là hai endpoint khác nhau nhưng trả đúng một bộ cột; thay vì hai file
SQL lệch nhau đúng một dòng (chúng SẼ trôi khỏi nhau), viết một contract::

    parameters:
      weather_model: era5

rồi trong SQL: ``'{{ weather_model }}' AS weather_model``. Giá trị được chèn
thẳng dạng text — YAML là config tin cậy, cùng mức tin cậy với file SQL — nên
tác giả SQL tự quyết định có bọc nháy hay không.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

# Engine tự cấp hai placeholder này; nguồn không được ghi đè.
RESERVED_PLACEHOLDERS = frozenset({"files", "ingested_at"})


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
    parameters: Mapping[str, str] = field(default_factory=dict)
    base_dir: Path = field(default=Path("."))

    def __post_init__(self) -> None:
        for key, value in self.parameters.items():
            if key in RESERVED_PLACEHOLDERS:
                raise ValueError(
                    f"{self.name}: parameter {key!r} trùng placeholder engine"
                )
            if "'" in str(value):
                raise ValueError(
                    f"{self.name}: parameter {key!r} chứa dấu nháy đơn — "
                    "giá trị được chèn thẳng vào SQL nên sẽ làm hỏng câu lệnh"
                )

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
            parameters={
                key: str(value) for key, value in (raw.get("parameters") or {}).items()
            },
            base_dir=config_path.parent,
        )

    @property
    def sql(self) -> str:
        return self.transform.read_sql(self.base_dir)
