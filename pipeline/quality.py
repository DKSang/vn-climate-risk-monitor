"""Great Expectations gates: data that fails them does not move on (docs/adr/0001).

dbt tests check the transformation logic; these check the data itself along the
usual quality dimensions: volume, completeness, uniqueness, validity, timeliness
and freshness.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import great_expectations as gx
import great_expectations.expectations as gxe
import pandas as pd

KEY = ["grid_latitude", "grid_longitude", "valid_at"]


def check_bronze_forecast(
    rows: pd.DataFrame, *, rows_per_run: int, now: datetime | None = None
) -> None:
    """Raise if forecast rows read from Bronze break an expectation.

    Each forecast run is checked on its own, because several can be waiting after
    an outage; freshness applies to the newest run only.
    """
    now = now or datetime.now(UTC)
    per_run = [
        gxe.ExpectTableRowCountToEqual(value=rows_per_run),
        *(
            gxe.ExpectColumnValuesToNotBeNull(column=column)
            for column in ["forecast_run", *KEY]
        ),
        gxe.ExpectCompoundColumnsToBeUnique(column_list=KEY),
        *(
            gxe.ExpectColumnValuesToBeBetween(column=column, min_value=0, max_value=500)
            for column in ["precipitation", "rain", "showers"]
        ),
        gxe.ExpectColumnValuesToBeBetween(
            column="precipitation_probability", min_value=0, max_value=100
        ),
        # 72 forecast hours, plus up to a day of fetch delay after the slot.
        gxe.ExpectColumnValuesToBeBetween(
            column="lead_hours", min_value=0, max_value=96
        ),
    ]
    rows = rows.assign(
        lead_hours=(rows["valid_at"] - rows["forecast_run"]) / pd.Timedelta(hours=1)
    )
    failures = []
    for run, run_rows in rows.groupby("forecast_run"):
        failures += [f"run {run:%Y%m%dT%H}: {f}" for f in _failures(run_rows, per_run)]
    newest = rows[rows["forecast_run"] == rows["forecast_run"].max()]
    fresh = gxe.ExpectColumnMaxToBeBetween(
        column="valid_at", min_value=pd.Timestamp(now + timedelta(hours=48))
    )
    failures += [f"newest run: {f}" for f in _failures(newest, [fresh])]
    if failures:
        raise RuntimeError(
            "Bronze forecast failed quality checks: " + "; ".join(failures)
        )


def check_bronze_archive(rows: pd.DataFrame, *, cells: int) -> None:
    """Raise if archive rows read from Bronze break an expectation.

    Each calendar month is checked on its own: every grid cell for every hour.
    No freshness check: the archive is history, often backfilled years later.
    """
    months = rows["valid_at"].dt.strftime("%Y-%m")
    failures = []
    for month, month_rows in rows.groupby(months):
        hours = pd.Period(month).days_in_month * 24
        expectations = [
            gxe.ExpectTableRowCountToEqual(value=cells * hours),
            *(gxe.ExpectColumnValuesToNotBeNull(column=column) for column in KEY),
            gxe.ExpectCompoundColumnsToBeUnique(column_list=KEY),
            *(
                gxe.ExpectColumnValuesToBeBetween(
                    column=column, min_value=0, max_value=500
                )
                for column in ["precipitation", "rain"]
            ),
        ]
        failures += [f"month {month}: {f}" for f in _failures(month_rows, expectations)]
    if failures:
        raise RuntimeError(
            "Bronze archive failed quality checks: " + "; ".join(failures)
        )


def check_silver_forecast(runs: pd.DataFrame, *, rows_per_run: int) -> None:
    """Raise if a forecast run touched in silver.clean_ breaks an expectation.

    `runs` holds every row of each touched run (grouped by its `period` column),
    so volume means the whole run. Keys (unique, not null) are dbt tests.
    """
    expectations = [
        gxe.ExpectTableRowCountToEqual(value=rows_per_run),
        *(
            gxe.ExpectColumnValuesToBeBetween(column=column, min_value=0, max_value=500)
            for column in ["precipitation_mm", "rain_mm", "showers_mm"]
        ),
        gxe.ExpectColumnValuesToBeBetween(
            column="precipitation_probability_pct", min_value=0, max_value=100
        ),
        # WMO weather interpretation codes.
        gxe.ExpectColumnValuesToBeBetween(
            column="weather_code", min_value=0, max_value=99
        ),
    ]
    failures = []
    for run, run_rows in runs.groupby("forecast_run"):
        failures += [
            f"run {run:%Y%m%dT%H}: {f}" for f in _failures(run_rows, expectations)
        ]
    if failures:
        raise RuntimeError(
            "Silver forecast failed quality checks: " + "; ".join(failures)
        )


def check_silver_archive(months: pd.DataFrame, *, cells: int) -> None:
    """Raise if a month touched in silver.clean_weather_archive_hourly breaks an expectation.

    `months` holds every row of each touched month (its `period` column).
    """
    failures = []
    for month, month_rows in months.groupby("period"):
        hours = month.days_in_month * 24
        expectations = [
            gxe.ExpectTableRowCountToEqual(value=cells * hours),
            *(
                gxe.ExpectColumnValuesToBeBetween(
                    column=column, min_value=0, max_value=500
                )
                for column in ["precipitation_mm", "rain_mm"]
            ),
            gxe.ExpectColumnValuesToBeBetween(
                column="weather_code", min_value=0, max_value=99
            ),
        ]
        failures += [
            f"month {month:%Y-%m}: {f}" for f in _failures(month_rows, expectations)
        ]
    if failures:
        raise RuntimeError(
            "Silver archive failed quality checks: " + "; ".join(failures)
        )


def _failures(rows: pd.DataFrame, expectations: list) -> list[str]:
    context = gx.get_context(mode="ephemeral")
    context.variables.progress_bars = {"globally": False}
    batch = (
        context.data_sources.add_pandas("rows")
        .add_dataframe_asset("rows")
        .add_batch_definition_whole_dataframe("all")
        .get_batch(batch_parameters={"dataframe": rows})
    )
    suite = gx.ExpectationSuite(name="checks", expectations=expectations)
    failed = []
    for result in batch.validate(suite).results:
        if result.success:
            continue
        config = result.expectation_config
        target = (
            config.kwargs.get("column") or config.kwargs.get("column_list") or "table"
        )
        observed = result.result.get(
            "unexpected_count", result.result.get("observed_value")
        )
        failed.append(f"{config.type} on {target} ({observed})")
    return failed
