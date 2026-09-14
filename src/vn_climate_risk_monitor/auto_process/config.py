"""Code-native definitions for the two incremental processing flows."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SourceBinding:
    """Input của process và cột timestamp dùng để checkpoint."""

    ref: str
    change_column: str = "_ingested_at"

    def __post_init__(self) -> None:
        if not self.ref.strip():
            raise ValueError("source.ref must not be empty")


@dataclass(frozen=True)
class ProcessConfig:
    process_key: str
    target: str
    sources: tuple[SourceBinding, ...]
    scope: str = "production"

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

PROCESS_CONFIGS = {
    "archive": ProcessConfig(
        process_key="archive",
        target="gold.fct_rain_archive_hourly",
        sources=(SourceBinding("stg_weather_archive_hourly"),),
    ),
    "forecast": ProcessConfig(
        process_key="forecast",
        target="gold.fct_rain_forecast_hourly",
        sources=(SourceBinding("stg_weather_forecast"),),
    ),
}
ACTIVE_PROCESS_KEYS = tuple(PROCESS_CONFIGS)


def load_active_configs() -> dict[str, ProcessConfig]:
    return dict(PROCESS_CONFIGS)


def load_active_config(process_key: str) -> ProcessConfig:
    try:
        return PROCESS_CONFIGS[process_key]
    except KeyError as error:
        raise ValueError(
            f"Unknown active process {process_key!r}; "
            f"choose one of {', '.join(ACTIVE_PROCESS_KEYS)}"
        ) from error
