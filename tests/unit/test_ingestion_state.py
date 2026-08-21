from datetime import datetime
from typing import Any

import pytest

from vn_climate_risk_monitor.ingestion.state import (
    build_logical_run_id,
    ensure_ingestion_state,
    logical_schedule_key,
)


class RecordingConnection:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.commit_count = 0

    def execute(self, query: str, params: object | None = None) -> object:
        assert params is None
        self.statements.append(query)
        return object()

    def commit(self) -> None:
        self.commit_count += 1


def test_ingestion_state_schema_is_idempotent_and_postgres_native() -> None:
    connection = RecordingConnection()

    ensure_ingestion_state(connection)
    ensure_ingestion_state(connection)

    ddl = "\n".join(connection.statements)
    assert "ingestion.ingestion_runs" in ddl
    assert "ingestion.ingestion_files" in ddl
    assert "ingestion_one_running_attempt" in ddl
    assert "CHECK (status IN ('PENDING', 'PROCESSING', 'COMMITTED', 'FAILED'))" in ddl
    assert connection.commit_count == 2


def test_logical_run_id_is_stable_across_attempts_but_separates_scope() -> None:
    values: dict[str, Any] = {
        "pipeline_name": "open_meteo_forecast",
        "dataset": "forecast",
        "logical_key": "2026-08-21T10:15:00Z",
    }

    production = build_logical_run_id(scope="production", **values)

    assert production == build_logical_run_id(scope="production", **values)
    assert production != build_logical_run_id(scope="canary_1", **values)


def test_logical_schedule_key_normalizes_to_utc() -> None:
    value = datetime.fromisoformat("2026-08-21T17:15:00+07:00")

    assert logical_schedule_key(value) == "2026-08-21T10:15:00Z"


def test_logical_schedule_key_rejects_naive_timestamp() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        logical_schedule_key(datetime(2026, 8, 21, 10, 15))  # noqa: DTZ001
