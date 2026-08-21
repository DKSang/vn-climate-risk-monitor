from datetime import UTC, date, datetime

import pytest

from vn_climate_risk_monitor.ingestion.open_meteo import (
    ARCHIVE_HOURLY_VARIABLES,
    ArchiveFileParameters,
    ArchiveRunParameters,
    RequestedLocation,
    latest_complete_archive_date,
    plan_archive_backfill,
    plan_archive_year,
)


def _locations(count: int) -> tuple[RequestedLocation, ...]:
    return tuple(
        RequestedLocation(
            ward_key=index,
            ward_code=f"{index:05}",
            latitude=21.0 + index / 1000,
            longitude=105.8 + index / 1000,
        )
        for index in range(1, count + 1)
    )


def test_archive_availability_uses_conservative_five_day_lag() -> None:
    assert latest_complete_archive_date(date(2026, 8, 21)) == date(2026, 8, 16)


def test_archive_plan_uses_year_runs_and_month_location_tasks() -> None:
    plan = plan_archive_backfill(
        _locations(3),
        start_year=2000,
        end_date=date(2000, 12, 31),
        model="era5",
        location_batch_size=2,
    )

    assert len(plan.years) == 1
    year = plan.years[0]
    assert year.logical_key == "model=era5/year=2000"
    assert year.expected_file_count == 24
    assert [task.batch_index for task in year.tasks] == list(range(24))
    assert year.tasks[0].window.start_date == date(2000, 1, 1)
    assert year.tasks[0].window.end_date == date(2000, 1, 31)
    assert year.tasks[0].location_batch_index == 0
    assert year.tasks[0].file_parameters.ward_keys == (1, 2)
    assert year.tasks[1].location_batch_index == 1
    assert year.tasks[1].file_parameters.ward_keys == (3,)
    assert year.expected_row_count == 366 * 24 * 3


def test_archive_plan_keeps_current_year_partial() -> None:
    plan = plan_archive_backfill(
        _locations(1),
        start_year=2026,
        end_date=date(2026, 8, 16),
        model="era5",
        location_batch_size=25,
    )

    year = plan.years[0]
    assert year.logical_key == "model=era5/year=2026/through=2026-08-16"
    assert year.end_date == date(2026, 8, 16)
    assert year.expected_file_count == 8
    assert year.tasks[-1].window.start_date == date(2026, 8, 1)
    assert year.tasks[-1].window.end_date == date(2026, 8, 16)
    assert year.expected_row_count == 228 * 24


def test_archive_year_plan_can_target_one_month_without_planning_prior_months() -> None:
    plan = plan_archive_year(
        _locations(1),
        start_date=date(2000, 2, 1),
        end_date=date(2000, 2, 29),
        model="era5",
        location_batch_size=25,
    )

    assert plan.logical_key == (
        "model=era5/year=2000/from=2000-02-01/through=2000-02-29"
    )
    assert plan.expected_file_count == 1
    assert plan.tasks[0].file_parameters.month == 2
    assert plan.expected_row_count == 29 * 24


def test_archive_logical_identity_changes_when_model_contract_changes() -> None:
    values = {
        "locations": _locations(1),
        "start_date": date(2000, 1, 1),
        "end_date": date(2000, 1, 31),
        "location_batch_size": 25,
    }

    era5 = plan_archive_year(model="era5", **values)
    era5_land = plan_archive_year(model="era5_land", **values)

    assert era5.logical_key != era5_land.logical_key


def test_archive_parameters_round_trip_through_generic_json_mappings() -> None:
    run = ArchiveRunParameters(
        year=2000,
        start_date=date(2000, 1, 1),
        end_date=date(2000, 12, 31),
        model="era5",
        hourly_variables=ARCHIVE_HOURLY_VARIABLES,
        location_count=126,
    )
    file = ArchiveFileParameters(
        year=2000,
        month=1,
        start_date=date(2000, 1, 1),
        end_date=date(2000, 1, 31),
        location_batch_index=0,
        ward_keys=(1, 2),
    )

    assert ArchiveRunParameters.from_mapping(run.to_mapping()) == run
    assert ArchiveFileParameters.from_mapping(file.to_mapping()) == file


def test_archive_task_builds_exact_api_request_contract() -> None:
    plan = plan_archive_backfill(
        _locations(2),
        start_year=2001,
        end_date=date(2001, 1, 31),
        model="era5",
        location_batch_size=25,
    )

    contract = plan.years[0].tasks[0].request_contract(
        endpoint="https://archive-api.open-meteo.com/v1/archive",
        model="era5",
        requested_at_utc=datetime(2026, 8, 21, 12, tzinfo=UTC),
    )

    assert contract.api_query_params == {
        "latitude": "21.001,21.002",
        "longitude": "105.801,105.802",
        "hourly": ",".join(ARCHIVE_HOURLY_VARIABLES),
        "models": "era5",
        "start_date": "2001-01-01",
        "end_date": "2001-01-31",
        "timeformat": "unixtime",
        "timezone": "GMT",
        "precipitation_unit": "mm",
        "cell_selection": "land",
    }


def test_archive_plan_rejects_date_before_start_year() -> None:
    with pytest.raises(ValueError, match="end_date"):
        plan_archive_backfill(
            _locations(1),
            start_year=2000,
            end_date=date(1999, 12, 31),
            model="era5",
            location_batch_size=25,
        )
