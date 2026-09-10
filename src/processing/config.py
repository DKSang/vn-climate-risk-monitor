"""Khai báo một incremental process bằng YAML.

Ví dụ ``processing/forecast_gold.yml``::

    process_key: forecast_gold
    target: gold.fct_rain_forecast_hourly

    sources:
      - ref: int_weather_forecast_hourly
        change_column: _updated_at

    checkpoint:
      safety_lag: 15 minutes

    runner:
      select: "bridge_ward_grid fct_rain_forecast_hourly fct_rain_forecast_current_hourly fct_rain_pressure_alert"

CỐ Ý không có ``recompute_scope`` ở đây. Độ rộng cửa sổ (lookback) là thuộc tính
của TỪNG MODEL, không phải của process: selector của runner chỉ chọn nhóm model
cần build. Khai báo phạm vi dữ liệu trong chính model qua
macro ``incremental_input_scope`` — versioned cùng SQL sinh ra nó.

``soft_delete`` khai báo các bảng cần đồng bộ cờ active với nguồn sau khi
transform xong — xem :mod:`processing.softdelete`.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Any

import yaml

from processing.softdelete import SoftDeleteConfig

_UNITS = {
    "second": 1,
    "seconds": 1,
    "minute": 60,
    "minutes": 60,
    "hour": 3600,
    "hours": 3600,
    "day": 86400,
    "days": 86400,
}

_DURATION = re.compile(r"^\s*(\d+)\s*([a-z]+)\s*$", re.IGNORECASE)

# Khoảng cách tối đa giữa lúc `_ingested_at` được đóng dấu và lúc row thật sự
# visible. Autoloader đo 0,4s cho lô 50 file archive; 15 phút là biên rộng gấp
# nghìn lần mà chi phí chỉ là rescan 15 phút Bronze mỗi lần chạy.
DEFAULT_SAFETY_LAG = timedelta(minutes=15)


def parse_duration(value: str | float | timedelta) -> timedelta:
    """``"15 minutes"`` → ``timedelta``. Số trần được hiểu là giây."""
    if isinstance(value, timedelta):
        return value
    if isinstance(value, int | float):
        return timedelta(seconds=float(value))
    match = _DURATION.match(str(value))
    if not match:
        raise ValueError(f"Không hiểu duration {value!r}; ví dụ hợp lệ: '15 minutes'")
    amount, unit = match.groups()
    seconds = _UNITS.get(unit.lower())
    if seconds is None:
        raise ValueError(f"Đơn vị không hỗ trợ {unit!r}; dùng {sorted(set(_UNITS))}")
    return timedelta(seconds=int(amount) * seconds)


@dataclass(frozen=True)
class SourceBinding:
    """Một input của process, kèm cột timestamp do PLATFORM sinh.

    ``change_column`` phải là timestamp kỹ thuật của layer nguồn
    (``_ingested_at``), KHÔNG BAO GIỜ là business timestamp của hệ nguồn:
    cái sau có thể null, backdated, sai timezone, và không nằm dưới quyền kiểm
    soát của pipeline.
    """

    ref: str
    change_column: str = "_ingested_at"

    def __post_init__(self) -> None:
        if not self.ref.strip():
            raise ValueError("source.ref must not be empty")


@dataclass(frozen=True)
class CheckpointConfig:
    """Checkpoint = start time của lần chạy thành công gần nhất, trừ safety lag."""

    safety_lag: timedelta = DEFAULT_SAFETY_LAG

    def __post_init__(self) -> None:
        if self.safety_lag < timedelta(0):
            raise ValueError("safety_lag must not be negative")


@dataclass(frozen=True)
class RunnerConfig:
    """Cách chạy transform. Hiện chỉ có dbt."""

    select: str | None = None


@dataclass(frozen=True)
class ProcessConfig:
    process_key: str
    target: str
    sources: tuple[SourceBinding, ...]
    scope: str = "production"
    checkpoint: CheckpointConfig = field(default_factory=CheckpointConfig)
    runner: RunnerConfig = field(default_factory=RunnerConfig)
    # Chạy SAU khi transform thành công, TRƯỚC khi advance checkpoint: soft
    # delete lỗi thì run FAILED và checkpoint không nhích.
    soft_delete: tuple[SoftDeleteConfig, ...] = ()

    def __post_init__(self) -> None:
        if not self.process_key.strip() or not self.target.strip():
            raise ValueError("process_key and target must not be empty")
        if not self.sources:
            raise ValueError(f"{self.process_key}: cần ít nhất một source")
        refs = [source.ref for source in self.sources]
        if len(set(refs)) != len(refs):
            raise ValueError(f"{self.process_key}: source.ref bị trùng: {refs}")

    @property
    def source_refs(self) -> tuple[str, ...]:
        return tuple(source.ref for source in self.sources)

    @classmethod
    def from_yaml(cls, path: str | Path) -> ProcessConfig:
        config_path = Path(path)
        raw: dict[str, Any] = yaml.safe_load(config_path.read_text(encoding="utf-8"))
        missing = {"process_key", "target", "sources"} - raw.keys()
        if missing:
            raise ValueError(f"{config_path}: thiếu khoá bắt buộc {sorted(missing)}")

        checkpoint_raw = dict(raw.get("checkpoint") or {})
        if "safety_lag" in checkpoint_raw:
            checkpoint_raw["safety_lag"] = parse_duration(checkpoint_raw["safety_lag"])

        soft_delete = tuple(
            SoftDeleteConfig(
                target=item["target"],
                business_key=tuple(item["business_key"]),
                key_source_sql=item["key_source_sql"],
                **{
                    key: value
                    for key, value in item.items()
                    if key not in {"target", "business_key", "key_source_sql"}
                },
            )
            for item in (raw.get("soft_delete") or [])
        )

        return cls(
            process_key=raw["process_key"],
            target=raw["target"],
            sources=tuple(SourceBinding(**source) for source in raw["sources"]),
            scope=raw.get("scope", "production"),
            checkpoint=CheckpointConfig(**checkpoint_raw),
            runner=RunnerConfig(**(raw.get("runner") or {})),
            soft_delete=soft_delete,
        )
