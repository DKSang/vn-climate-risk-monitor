"""Pure Open-Meteo forecast and archive request planning."""

from __future__ import annotations

import calendar
import math
from collections.abc import Iterator, Mapping, Sequence
from collections.abc import Set as AbstractSet
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from itertools import batched
from urllib.parse import urlencode, urlparse, urlunparse

from pipeline.settings import OpenMeteoSettings

FORECAST_PREFIX = "bronze/files/open_meteo/forecast/incremental"
FORECAST_FIELDS = "precipitation,rain,showers,precipitation_probability,weather_code"
ARCHIVE_FIELDS = (
    "precipitation,rain,weather_code,soil_moisture_0_to_7cm,soil_moisture_7_to_28cm"
)
IFS_START = date(2017, 1, 1)
STAGING_HOURLY = "catalog1.silver.stg_weather_archive_hourly"


@dataclass(frozen=True)
class ArchiveModel:
    name: str
    prefix: str


@dataclass(frozen=True)
class FetchTask:
    url: str
    key: str
    units: int


ERA5 = ArchiveModel(
    name="era5",
    prefix="bronze/files/open_meteo/historical_weather_hourly/backfill",
)
IFS = ArchiveModel(
    name="ecmwf_ifs",
    prefix="bronze/files/open_meteo/historical_weather_hourly/ifs",
)
ARCHIVE_MODELS = (ERA5, IFS)


@dataclass(frozen=True)
class Location:
    ward_code: str
    latitude: float
    longitude: float


def model_for_month(month: date) -> ArchiveModel:
    return ERA5 if month < IFS_START else IFS


def effective_call_units(*, locations: int, days: int, variables: int) -> int:
    return math.ceil(locations * max(1.0, days / 14) * max(1.0, variables / 10))


def months_between(start: date, end: date) -> Iterator[date]:
    current = start.replace(day=1)
    while current <= end:
        yield current
        current = (current.replace(day=28) + timedelta(days=4)).replace(day=1)


def days_in_month(month: date) -> int:
    return calendar.monthrange(month.year, month.month)[1]


def request_url(base: str, params: dict[str, str]) -> str:
    return urlunparse(urlparse(base)._replace(query=urlencode(params, safe=",")))


def base_params(points: Sequence[object]) -> dict[str, str]:
    return {
        "latitude": ",".join(f"{point.latitude:.6f}" for point in points),  # type: ignore[attr-defined]
        "longitude": ",".join(f"{point.longitude:.6f}" for point in points),  # type: ignore[attr-defined]
        "timezone": "UTC",
        "timeformat": "unixtime",
    }


def tasks_for_prefix(
    *,
    points: Sequence[object],
    batch_size: int,
    prefix: str,
    url: str,
    extra_params: dict[str, str],
    existing: AbstractSet[str],
    run: str,
    days: int,
    variables: int,
) -> list[FetchTask]:
    tasks: list[FetchTask] = []
    for index, batch in enumerate(batched(points, batch_size)):
        name = f"response_{index:03}.json"
        if name in existing:
            continue
        tasks.append(
            FetchTask(
                url=request_url(url, base_params(batch) | extra_params),
                key=f"{prefix}/{run}/{name}",
                units=effective_call_units(
                    locations=len(batch), days=days, variables=variables
                ),
            )
        )
    return tasks


def month_params(month: date, model: ArchiveModel) -> tuple[str, dict[str, str]]:
    last = month.replace(day=days_in_month(month))
    prefix = f"{model.prefix}/year={month.year:04}/month={month.month:02}"
    return prefix, {
        "hourly": ARCHIVE_FIELDS,
        "models": model.name,
        "start_date": month.isoformat(),
        "end_date": last.isoformat(),
    }


def slot_params(
    slot: datetime, settings: OpenMeteoSettings
) -> tuple[str, dict[str, str]]:
    prefix = f"{FORECAST_PREFIX}/model={settings.forecast_model}/{slot:%Y/%m/%d/%H}"
    return prefix, {
        "hourly": FORECAST_FIELDS,
        "models": settings.forecast_model,
        "forecast_hours": str(settings.forecast_hours),
    }


def forecast_run_id(slot: datetime) -> str:
    """Return the stable Bronze vintage id for one hourly forecast slot."""
    hour = slot.replace(minute=0, second=0, microsecond=0)
    return f"run_{hour:%Y%m%dT%H%M%S}"


def archive_tasks(
    *,
    months: Sequence[date],
    settings: OpenMeteoSettings,
    run: str,
    covered: Mapping[str, frozenset[date]],
    cells_by_model: Mapping[str, Sequence[object]],
    existing: Mapping[str, AbstractSet[str]],
) -> list[FetchTask]:
    variables = len(ARCHIVE_FIELDS.split(","))
    tasks: list[FetchTask] = []
    for month in months:
        model = model_for_month(month)
        if month in covered.get(model.name, frozenset()):
            continue
        prefix, extra = month_params(month, model)
        tasks.extend(
            tasks_for_prefix(
                points=cells_by_model[model.name],
                batch_size=settings.location_batch_size,
                prefix=prefix,
                url=settings.archive_url,
                extra_params=extra,
                existing=existing.get(prefix, frozenset()),
                run=run,
                days=days_in_month(month),
                variables=variables,
            )
        )
    return tasks


def forecast_tasks(
    *,
    slots: Sequence[datetime],
    locations: Sequence[Location],
    settings: OpenMeteoSettings,
    run: str,
    existing: Mapping[str, AbstractSet[str]],
) -> list[FetchTask]:
    variables = len(FORECAST_FIELDS.split(","))
    days = max(1, settings.forecast_hours // 24)
    tasks: list[FetchTask] = []
    for slot in slots:
        prefix, extra = slot_params(slot, settings)
        tasks.extend(
            tasks_for_prefix(
                points=locations,
                batch_size=settings.location_batch_size,
                prefix=prefix,
                url=settings.forecast_url,
                extra_params=extra,
                existing=existing.get(prefix, frozenset()),
                run=run,
                days=days,
                variables=variables,
            )
        )
    return tasks
