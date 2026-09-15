"""Task 3 contracts for the two dbt-native processing flows."""

from __future__ import annotations

import importlib
import json
from contextlib import nullcontext
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import duckdb
import pytest

from vn_climate_risk_monitor.auto_process import config as processing_config
from vn_climate_risk_monitor.auto_process.dbt import build_command
from vn_climate_risk_monitor.auto_process.runner import (
    Bounds,
    SourceBounds,
    compute_bounds,
    run_process,
)
from vn_climate_risk_monitor.auto_process.state import (
    ProcessingRepository,
    ProcessingStateError,
)

ROOT = Path(__file__).parents[2]
TRANSFORM_DIR = ROOT / "transform"


def at(hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 9, 13, hour, minute, tzinfo=UTC)


class FakeRepository:
    """A stateful control-plane double with the repository boundary intact."""

    def __init__(
        self,
        *,
        clock: list[datetime],
        checkpoints: dict[str, datetime | None] | None = None,
    ) -> None:
        self.clock = clock
        self.clock_index = 0
        self.checkpoints = dict(checkpoints or {})
        self.begun: list[dict[str, Any]] = []
        self.completed: list[dict[str, Any]] = []
        self.failed: list[dict[str, Any]] = []

    def control_now(self) -> datetime:
        value = self.clock[min(self.clock_index, len(self.clock) - 1)]
        self.clock_index += 1
        return value

    def read_checkpoints(
        self, *, process_key: str, scope: str, source_refs: tuple[str, ...]
    ) -> dict[str, datetime | None]:
        del process_key, scope
        return {source_ref: self.checkpoints.get(source_ref) for source_ref in source_refs}

    def begin_run(self, **kwargs: Any) -> UUID:
        run_id = kwargs.pop("run_id", None) or uuid4()
        self.begun.append({"run_id": run_id, **kwargs})
        return run_id

    def complete_run(self, run_id: UUID, **kwargs: Any) -> None:
        self.completed.append({"run_id": run_id, **kwargs})
        for source_ref in kwargs["source_refs"]:
            self.checkpoints[source_ref] = kwargs["checkpoint"]

    def fail_run(self, run_id: UUID, **kwargs: Any) -> None:
        self.failed.append({"run_id": run_id, **kwargs})


class ExplodingConnection:
    """Any mutating SQL proves the active-key guard ran too late."""

    def transaction(self) -> Any:
        return nullcontext()

    def execute(self, *_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("retired process key reached mutating SQL")


class ReadOnlyConnection:
    def execute(self, query: str, *_args: Any, **_kwargs: Any) -> Any:
        assert query.lstrip().startswith("SELECT")
        return self

    def fetchall(self) -> list[Any]:
        return []


class RunningConnection:
    def __init__(self, row: tuple[Any, ...]) -> None:
        self.row = row
        self.query = ""
        self.params: tuple[Any, ...] | None = None

    def execute(self, query: str, params: tuple[Any, ...]) -> RunningConnection:
        self.query = query
        self.params = params
        return self

    def fetchone(self) -> tuple[Any, ...]:
        return self.row


def test_retired_process_keys_are_rejected_by_runner_before_mutation() -> None:
    retired = processing_config.ProcessConfig(
        process_key="rain_gold",
        target="gold.rain_gold",
        sources=(processing_config.SourceBinding(ref="stg_weather_archive_hourly"),),
        scope="vn",
    )
    repository = FakeRepository(clock=[at(10)])

    with pytest.raises(ProcessingStateError, match="active"):
        run_process(config=retired, repository=repository, execute=lambda _: None)

    assert repository.begun == []


def test_retired_process_keys_are_rejected_on_all_mutating_repository_paths() -> None:
    repository = ProcessingRepository(ExplodingConnection())  # type: ignore[arg-type]
    retired = "rain_gold"
    run_id = uuid4()
    common = {
        "process_key": retired,
        "scope": "vn",
        "source_refs": ("stg_weather_archive_hourly",),
    }

    mutators = (
        lambda: repository.begin_run(
            process_key=retired,
            scope="vn",
            target_ref="gold.rain_gold",
            started_at=at(10),
            bounds={**common},
        ),
        lambda: repository._upsert_checkpoints(
            **common, checkpoint=at(10), run_id=run_id
        ),
        lambda: repository.complete_run(
            run_id,
            **common,
            checkpoint=at(10),
            completed_at=at(10, 1),
        ),
        lambda: repository.fail_run(
            run_id,
            process_key=retired,
            error=RuntimeError("boom"),
            completed_at=at(10, 1),
        ),
        lambda: repository.abandon_running(
            process_key=retired,
            scope="vn",
            actor="test",
            reason="cleanup",
            completed_at=at(10, 1),
        ),
        lambda: repository.rewind(
            process_key=retired,
            scope="vn",
            target_ref="gold.rain_gold",
            source_refs=common["source_refs"],
            checkpoint=at(9),
            actor="test",
            reason="repair",
            now=at(10),
        ),
    )

    for mutate in mutators:
        with pytest.raises(ProcessingStateError, match="active"):
            mutate()


def test_retired_rows_remain_available_to_read_only_audit_queries() -> None:
    repository = ProcessingRepository(ReadOnlyConnection())  # type: ignore[arg-type]

    assert repository.read_checkpoints(
        process_key="rain_gold",
        scope="vn",
        source_refs=("stg_weather_archive_hourly",),
    ) == {"stg_weather_archive_hourly": None}
    assert repository.recent_runs(process_key="rain_gold", scope="vn") == []


def test_running_process_context_is_read_for_later_airflow_phases() -> None:
    run_id = uuid4()
    connection = RunningConnection((run_id, at(10), {"archive_hourly": {}}))
    repository = ProcessingRepository(connection)  # type: ignore[arg-type]

    assert repository.read_running_run(
        run_id=run_id, process_key="archive", scope="production"
    ) == (
        run_id,
        at(10),
        {"archive_hourly": {}},
    )
    assert "status = 'RUNNING'" in connection.query
    assert connection.params == (run_id, "archive", "production")


def test_exact_processing_run_status_supports_idempotent_publish_retry() -> None:
    run_id = uuid4()
    connection = RunningConnection(("SUCCEEDED",))
    repository = ProcessingRepository(connection)  # type: ignore[arg-type]

    assert repository.read_run_status(
        run_id=run_id, process_key="forecast", scope="production"
    ) == "SUCCEEDED"
    assert connection.params == (run_id, "forecast", "production")


def test_only_forecast_and_archive_are_active_process_keys() -> None:
    configs = processing_config.load_active_configs()

    assert tuple(configs) == ("archive", "forecast")
    assert set(configs) == {"forecast", "archive"}
    assert not list((ROOT / "auto_process").glob("*.yml"))
    assert configs["forecast"].source_refs == ("stg_weather_forecast",)
    assert configs["archive"].source_refs == ("stg_weather_archive_hourly",)


def test_first_run_uses_controlled_full_refresh_and_audits_active_flow() -> None:
    config = processing_config.load_active_config("forecast")
    repository = FakeRepository(clock=[at(10), at(10, 5)])
    seen: list[Bounds] = []

    run_process(config=config, repository=repository, execute=seen.append)

    assert seen[0].is_incremental is False
    assert repository.begun[0]["process_key"] == "forecast"
    assert repository.completed[0]["checkpoint"] == at(10)


def test_checkpoint_is_used_as_the_incremental_lower_bound() -> None:
    config = processing_config.load_active_config("archive")

    bounds = compute_bounds(
        config,
        {"stg_weather_archive_hourly": at(10)},
        at(11),
    )

    assert bounds.sources[0].lower_bound == at(10)


def test_failed_dbt_run_does_not_advance_checkpoint_or_publish_a_snapshot() -> None:
    config = processing_config.load_active_config("archive")
    repository = FakeRepository(
        clock=[at(11), at(11, 2)],
        checkpoints={"stg_weather_archive_hourly": at(10)},
    )

    def failed(_bounds: Bounds) -> None:
        raise RuntimeError("dbt build failed")

    with pytest.raises(RuntimeError, match="dbt build failed"):
        run_process(config=config, repository=repository, execute=failed)

    assert repository.checkpoints["stg_weather_archive_hourly"] == at(10)
    assert repository.completed == []
    assert repository.failed[0]["error"].args == ("dbt build failed",)


def test_successful_build_audits_row_count_and_published_snapshot() -> None:
    config = processing_config.load_active_config("forecast")
    repository = FakeRepository(clock=[at(12), at(12, 3)])

    run_process(
        config=config,
        repository=repository,
        execute=lambda _bounds: {
            "target_row_count": 126 * 72,
            "published_snapshot_id": 42,
        },
    )

    metrics = repository.completed[0]["metrics"]
    assert metrics == {"target_row_count": 9072, "published_snapshot_id": 42}
    assert repository.completed[0]["checkpoint"] == at(12)


def test_reprocess_and_abandon_are_exposed_for_active_processes() -> None:
    cli = importlib.import_module("vn_climate_risk_monitor.auto_process.cli")
    parser = cli.build_parser()

    for argv in (
        ["reprocess-from", "forecast", "--from", "2026-09-13T10:00:00", "--reason", "repair"],
        ["abandon", "archive", "--reason", "worker stopped"],
    ):
        args = parser.parse_args(argv)
        assert args.process_key in {"forecast", "archive"}
        assert args.command in {"reprocess-from", "abandon"}


def test_dbt_build_uses_native_tag_selection_and_full_refresh_on_first_run() -> None:
    bounds = Bounds(
        run_started_at=at(13),
        sources=(
            SourceBounds(
                source_ref="stg_weather_forecast",
                change_column="_ingested_at",
                checkpoint_before=None,
                lower_bound=None,
            ),
        ),
    )
    command = build_command(bounds, selection="tag:forecast")

    assert command[1] == "build"
    assert command[command.index("--select") + 1] == "tag:forecast"
    assert "--selector" not in command
    assert "--full-refresh" in command
    assert json.loads(command[command.index("--vars") + 1])["processing_incremental"] is False


def test_active_graphs_use_model_tags_without_selector_yaml() -> None:
    assert not (TRANSFORM_DIR / "selectors.yml").exists()
    forecast = (
        TRANSFORM_DIR / "models/intermediate/int_weather_forecast_hourly.sql"
    ).read_text()
    archive = (
        TRANSFORM_DIR / "models/intermediate/int_weather_archive_hourly.sql"
    ).read_text()
    assert "'forecast'" in forecast
    assert "'archive'" in archive


def test_stock_duckdb_materializations_and_delete_insert_are_used() -> None:
    materializations = TRANSFORM_DIR / "macros/materializations.sql"
    assert not materializations.exists()

    for path in TRANSFORM_DIR.glob("models/**/*.sql"):
        body = path.read_text(encoding="utf-8")
        if "materialized = 'incremental'" in body:
            assert "incremental_strategy = 'delete+insert'" in body, path

    assert "{% materialization" not in "\n".join(
        path.read_text(encoding="utf-8")
        for path in TRANSFORM_DIR.glob("macros/*.sql")
    )
    assert "{% macro is_incremental" not in "\n".join(
        path.read_text(encoding="utf-8")
        for path in TRANSFORM_DIR.glob("macros/*.sql")
    )


def test_weather_intermediate_models_read_physical_silver_staging() -> None:
    archive = (TRANSFORM_DIR / "models/intermediate/int_weather_archive_hourly.sql").read_text()
    forecast = (TRANSFORM_DIR / "models/intermediate/int_weather_forecast_hourly.sql").read_text()

    assert "source('silver_staging', 'stg_weather_archive_hourly')" in archive
    assert "source('silver_staging', 'stg_weather_forecast')" in forecast
    assert "stg_open_meteo__weather_archive_hourly" not in archive
    assert "stg_open_meteo__weather_forecast_hourly" not in forecast
    assert not (TRANSFORM_DIR / "models/staging/stg_open_meteo__weather_archive_hourly.sql").exists()
    assert not (TRANSFORM_DIR / "models/staging/stg_open_meteo__weather_forecast_hourly.sql").exists()


def test_forecast_re_reads_all_rows_for_runs_touched_by_the_changed_window() -> None:
    sql = (TRANSFORM_DIR / "models/intermediate/int_weather_forecast_hourly.sql").read_text(
        encoding="utf-8"
    )

    assert "changed_rows AS" in sql
    assert "changed_runs AS" in sql
    assert "incremental_changed_filter" in sql
    assert sql.index("changed_runs AS") < sql.index("staged AS")
    assert "FROM raw_rows" in sql
    assert "forecast_run_id IN (SELECT forecast_run_id FROM changed_runs)" in sql


def test_forecast_completeness_counts_source_locations_before_grid_dedup() -> None:
    sql = (TRANSFORM_DIR / "models/intermediate/int_weather_forecast_hourly.sql").read_text(
        encoding="utf-8"
    )

    assert "COUNT(DISTINCT valid_time_utc)" in sql
    assert "COUNT(*) AS locations" in sql
    assert "COUNT(DISTINCT grid_cell_id) AS locations" not in sql
    assert "COUNT(*) = {{ var('forecast_expected_hours', 72) }}" in sql
    assert "MIN(locations) = {{ var('forecast_expected_locations', 126) }}" in sql
    assert "MAX(locations) = {{ var('forecast_expected_locations', 126) }}" in sql


@pytest.mark.parametrize("model", ["dim_ward", "bridge_ward_grid"])
def test_static_reference_dimensions_and_bridge_are_tables_without_soft_delete(
    model: str,
) -> None:
    sql = (TRANSFORM_DIR / f"models/marts/{model}.sql").read_text()

    assert "materialized = 'table'" in sql
    assert "materialized = 'incremental'" not in sql
    # Keep public Gold columns without soft-deleting static references.
    assert "_deactivated_at" in sql
    assert "soft_delete" not in sql.lower()


def test_processing_console_entry_point_is_package_owned() -> None:
    project = (ROOT / "pyproject.toml").read_text()

    assert 'auto-process = "vn_climate_risk_monitor.auto_process.cli:main"' in project


def _forecast_rows(
    *, omit: tuple[int, int] | None = None, canonical_cells: int = 126
) -> list[tuple[str, datetime, str]]:
    rows: list[tuple[str, datetime, str]] = []
    for hour in range(72):
        for location in range(126):
            if omit == (hour, location):
                continue
            rows.append(
                (
                    "run_20260913T100000",
                    at(0) + timedelta(hours=hour),
                    f"cell-{location % canonical_cells:03}",
                )
            )
    return rows


def _complete_forecast_runs(connection: duckdb.DuckDBPyConnection) -> set[str]:
    return {
        row[0]
        for row in connection.execute(
            """
            SELECT forecast_run_id
            FROM forecast_rows
            GROUP BY forecast_run_id
            HAVING COUNT(DISTINCT valid_time_utc) = 72
               AND MIN(location_count) = 126
               AND MAX(location_count) = 126
            """
        ).fetchall()
    }


def test_forecast_completeness_re_reads_all_rows_for_a_late_affected_run() -> None:
    """A late row identifies a run, then the gate sees its complete history."""
    connection = duckdb.connect()
    try:
        connection.execute(
            "CREATE TABLE raw_rows (forecast_run_id VARCHAR, valid_time_utc TIMESTAMPTZ, location_key VARCHAR, ingested_at TIMESTAMPTZ)"
        )
        rows = _forecast_rows(omit=(71, 125))
        connection.executemany(
            "INSERT INTO raw_rows VALUES (?, ?, ?, ?)",
            [(*row, at(9)) for row in rows],
        )
        connection.execute(
            "INSERT INTO raw_rows VALUES (?, ?, ?, ?)",
            ("run_20260913T100000", at(0) + timedelta(hours=71), "cell-125", at(10)),
        )
        connection.execute(
            """
            CREATE TABLE forecast_rows AS
            SELECT forecast_run_id, valid_time_utc, location_key,
                   COUNT(*) OVER (
                       PARTITION BY forecast_run_id, valid_time_utc
                   ) AS location_count
            FROM raw_rows
            WHERE forecast_run_id IN (
                SELECT DISTINCT forecast_run_id
                FROM raw_rows
                WHERE ingested_at > TIMESTAMPTZ '2026-09-13 09:45:00+00'
            )
            """
        )

        assert _complete_forecast_runs(connection) == {"run_20260913T100000"}
    finally:
        connection.close()


def test_forecast_completeness_accepts_source_locations_snapped_to_fewer_grids() -> None:
    connection = duckdb.connect()
    try:
        connection.execute(
            "CREATE TABLE forecast_input (forecast_run_id VARCHAR, valid_time_utc TIMESTAMPTZ, location_key VARCHAR)"
        )
        rows = _forecast_rows(canonical_cells=48)
        connection.executemany(
            "INSERT INTO forecast_input VALUES (?, ?, ?)",
            rows,
        )
        connection.execute(
            """
            CREATE TABLE forecast_rows AS
            SELECT *, COUNT(*) OVER (
                PARTITION BY forecast_run_id, valid_time_utc
            ) AS location_count
            FROM forecast_input
            """
        )

        assert _complete_forecast_runs(connection) == {"run_20260913T100000"}
    finally:
        connection.close()


def test_forecast_completeness_rejects_a_missing_source_location() -> None:
    connection = duckdb.connect()
    try:
        connection.execute(
            "CREATE TABLE forecast_input (forecast_run_id VARCHAR, valid_time_utc TIMESTAMPTZ, location_key VARCHAR)"
        )
        connection.executemany(
            "INSERT INTO forecast_input VALUES (?, ?, ?)",
            _forecast_rows(omit=(12, 125), canonical_cells=48),
        )
        connection.execute(
            """
            CREATE TABLE forecast_rows AS
            SELECT *, COUNT(*) OVER (
                PARTITION BY forecast_run_id, valid_time_utc
            ) AS location_count
            FROM forecast_input
            """
        )

        assert _complete_forecast_runs(connection) == set()
    finally:
        connection.close()
