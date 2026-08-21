"""Deterministic yearly backfill plan for Open-Meteo Archive data."""

from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

from vn_climate_risk_monitor.ingestion.open_meteo.contracts import (
    ARCHIVE_HOURLY_VARIABLES,
    ARCHIVE_MIN_YEAR,
    ArchiveFileParameters,
    ArchiveRequestContract,
    ArchiveRunParameters,
    RequestedLocation,
    split_location_batches,
)

DEFAULT_ARCHIVE_START_YEAR = 2000
ARCHIVE_AVAILABILITY_LAG_DAYS = 5


def latest_complete_archive_date(
    as_of_date: date,
    *,
    lag_days: int = ARCHIVE_AVAILABILITY_LAG_DAYS,
) -> date:
    """Return the conservative reanalysis availability boundary."""
    if lag_days < 0:
        raise ValueError("lag_days must not be negative")
    return as_of_date - timedelta(days=lag_days)


@dataclass(frozen=True)
class ArchiveMonthWindow:
    """One inclusive source-time window, always contained in one month."""

    start_date: date
    end_date: date

    def __post_init__(self) -> None:
        if self.end_date < self.start_date:
            raise ValueError("end_date must not precede start_date")
        if (self.start_date.year, self.start_date.month) != (
            self.end_date.year,
            self.end_date.month,
        ):
            raise ValueError("archive window must stay within one month")

    @property
    def expected_hour_count(self) -> int:
        return ((self.end_date - self.start_date).days + 1) * 24


@dataclass(frozen=True)
class ArchiveRequestTask:
    """One physical request: a monthly window times one location batch."""

    batch_index: int
    location_batch_index: int
    window: ArchiveMonthWindow
    locations: tuple[RequestedLocation, ...]
    hourly_variables: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.batch_index < 0:
            raise ValueError("batch_index must not be negative")
        if self.location_batch_index < 0:
            raise ValueError("location_batch_index must not be negative")
        if not self.locations:
            raise ValueError("locations must not be empty")

    @property
    def file_parameters(self) -> ArchiveFileParameters:
        return ArchiveFileParameters(
            year=self.window.start_date.year,
            month=self.window.start_date.month,
            start_date=self.window.start_date,
            end_date=self.window.end_date,
            location_batch_index=self.location_batch_index,
            ward_keys=tuple(location.ward_key for location in self.locations),
        )

    @property
    def expected_row_count(self) -> int:
        return self.window.expected_hour_count * len(self.locations)

    def request_contract(
        self,
        *,
        endpoint: str,
        model: str,
        requested_at_utc: datetime | None = None,
    ) -> ArchiveRequestContract:
        return ArchiveRequestContract(
            endpoint=endpoint,
            model=model,
            start_date=self.window.start_date,
            end_date=self.window.end_date,
            batch_index=self.batch_index,
            requested_at_utc=requested_at_utc or datetime.now(UTC),
            locations=self.locations,
            hourly_variables=self.hourly_variables,
        )


@dataclass(frozen=True)
class ArchiveYearPlan:
    """One idempotent logical run and future Bronze year partition."""

    year: int
    start_date: date
    end_date: date
    model: str
    hourly_variables: tuple[str, ...]
    location_count: int
    tasks: tuple[ArchiveRequestTask, ...]

    @property
    def logical_key(self) -> str:
        prefix = f"model={self.model}/year={self.year}"
        if self.start_date != date(self.year, 1, 1):
            return (
                f"{prefix}/from={self.start_date.isoformat()}"
                f"/through={self.end_date.isoformat()}"
            )
        if self.end_date != date(self.year, 12, 31):
            return f"{prefix}/through={self.end_date.isoformat()}"
        return prefix

    @property
    def expected_file_count(self) -> int:
        return len(self.tasks)

    @property
    def expected_row_count(self) -> int:
        return sum(task.expected_row_count for task in self.tasks)

    @property
    def run_parameters(self) -> ArchiveRunParameters:
        return ArchiveRunParameters(
            year=self.year,
            start_date=self.start_date,
            end_date=self.end_date,
            model=self.model,
            hourly_variables=self.hourly_variables,
            location_count=self.location_count,
        )


@dataclass(frozen=True)
class ArchiveBackfillPlan:
    """Year-grouped plan whose runtime can split into monthly logical runs."""

    start_date: date
    end_date: date
    years: tuple[ArchiveYearPlan, ...]

    @property
    def request_count(self) -> int:
        return sum(year.expected_file_count for year in self.years)

    @property
    def expected_row_count(self) -> int:
        return sum(year.expected_row_count for year in self.years)


def _month_windows(start_date: date, end_date: date) -> tuple[ArchiveMonthWindow, ...]:
    windows: list[ArchiveMonthWindow] = []
    cursor = start_date
    while cursor <= end_date:
        last_day = calendar.monthrange(cursor.year, cursor.month)[1]
        month_end = min(date(cursor.year, cursor.month, last_day), end_date)
        windows.append(ArchiveMonthWindow(cursor, month_end))
        cursor = month_end + timedelta(days=1)
    return tuple(windows)


def plan_archive_year(
    locations: tuple[RequestedLocation, ...],
    *,
    start_date: date,
    end_date: date,
    model: str,
    location_batch_size: int,
    hourly_variables: tuple[str, ...] = ARCHIVE_HOURLY_VARIABLES,
) -> ArchiveYearPlan:
    """Plan one full or partial year with monthly physical request tasks."""
    if start_date.year < ARCHIVE_MIN_YEAR:
        raise ValueError(f"start_date must be in or after {ARCHIVE_MIN_YEAR}")
    if start_date.year != end_date.year:
        raise ValueError("archive year plan must stay within one year")
    if end_date < start_date:
        raise ValueError("end_date must not precede start_date")
    location_batches = split_location_batches(locations, location_batch_size)
    if not hourly_variables:
        raise ValueError("hourly_variables must not be empty")
    if len(hourly_variables) != len(set(hourly_variables)):
        raise ValueError("hourly_variables must not contain duplicates")

    tasks: list[ArchiveRequestTask] = []
    for window in _month_windows(start_date, end_date):
        for location_batch_index, location_batch in enumerate(location_batches):
            tasks.append(
                ArchiveRequestTask(
                    batch_index=len(tasks),
                    location_batch_index=location_batch_index,
                    window=window,
                    locations=location_batch,
                    hourly_variables=hourly_variables,
                )
            )
    return ArchiveYearPlan(
        year=start_date.year,
        start_date=start_date,
        end_date=end_date,
        model=model,
        hourly_variables=hourly_variables,
        location_count=len(locations),
        tasks=tuple(tasks),
    )


def plan_archive_backfill(
    locations: tuple[RequestedLocation, ...],
    *,
    start_year: int = DEFAULT_ARCHIVE_START_YEAR,
    end_date: date,
    model: str,
    location_batch_size: int,
    hourly_variables: tuple[str, ...] = ARCHIVE_HOURLY_VARIABLES,
) -> ArchiveBackfillPlan:
    """Plan year groups with monthly, deterministic physical requests."""
    if start_year < ARCHIVE_MIN_YEAR:
        raise ValueError(f"start_year must be at least {ARCHIVE_MIN_YEAR}")
    start_date = date(start_year, 1, 1)
    if end_date < start_date:
        raise ValueError("end_date must not precede the requested start year")
    years: list[ArchiveYearPlan] = []
    for year in range(start_year, end_date.year + 1):
        year_start = date(year, 1, 1)
        year_end = min(date(year, 12, 31), end_date)
        years.append(
            plan_archive_year(
                locations,
                start_date=year_start,
                end_date=year_end,
                model=model,
                location_batch_size=location_batch_size,
                hourly_variables=hourly_variables,
            )
        )

    return ArchiveBackfillPlan(
        start_date=start_date,
        end_date=end_date,
        years=tuple(years),
    )
