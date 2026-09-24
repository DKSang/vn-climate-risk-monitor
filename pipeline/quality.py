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
