import os
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from vn_climate_risk_monitor.ingestion.state import (
    PostgresIngestionRepository,
    StateConflictError,
    connect_control_plane,
    ensure_ingestion_state,
)
from vn_climate_risk_monitor.storage.models import SourceObjectMetadata

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_POSTGRES_INTEGRATION") != "1",
    reason="set RUN_POSTGRES_INTEGRATION=1 with PostgreSQL running",
)


class RollbackIntegrationCheck(Exception):
    pass


def _run_values(
    pipeline_name: str,
    timestamp: datetime,
    *,
    started_at_utc: datetime | None = None,
) -> dict[str, object]:
    return {
        "pipeline_name": pipeline_name,
        "source_name": "integration_source",
        "dataset": "events",
        "scope": "test",
        "logical_key": timestamp.isoformat(),
        "scheduled_at_utc": timestamp,
        "started_at_utc": started_at_utc or timestamp,
        "expected_file_count": 1,
        "source_uri": "https://example.test",
        "collector_version": "integration",
        "contract_version": "1",
        "run_parameters": {"test_kind": "repository"},
    }


def _register_file(
    repository: PostgresIngestionRepository,
    *,
    attempt_id: object,
    object_key: str,
):
    return repository.register_file(
        attempt_id=attempt_id,
        batch_index=0,
        object_key=object_key,
        expected_item_count=1,
        file_parameters={"entity_keys": [1]},
    )


def test_repository_run_claim_retry_and_commit_workflow() -> None:
    connection = connect_control_plane()
    ensure_ingestion_state(connection)
    repository = PostgresIngestionRepository(connection)
    suffix = uuid4().hex
    pipeline_name = f"integration_{suffix}"
    timestamp = datetime.now(UTC).replace(microsecond=0)

    try:
        try:
            with connection.transaction():
                run = repository.start_run(**_run_values(pipeline_name, timestamp))
                object_key = f"bronze/files/integration/{run.attempt_id}/response.json"
                file_id = _register_file(
                    repository,
                    attempt_id=run.attempt_id,
                    object_key=object_key,
                )
                repository.record_file(
                    file_id=file_id,
                    metadata=SourceObjectMetadata(
                        object_key=object_key,
                        size_bytes=2,
                        sha256="a" * 64,
                        content_type="application/json",
                    ),
                    http_status=200,
                    request_attempt_count=1,
                )
                repository.validate_file(file_id, received_item_count=1)
                repository.succeed_run(run.attempt_id, completed_at_utc=timestamp)

                with pytest.raises(StateConflictError, match="already succeeded"):
                    repository.start_run(**_run_values(pipeline_name, timestamp))

                assert not repository.claim_files(
                    pipeline_name=pipeline_name,
                    dataset="events",
                    scope="production",
                    worker_id="worker-1",
                    limit=1,
                    lease_seconds=60,
                    max_retries=2,
                )
                claimed = repository.claim_files(
                    pipeline_name=pipeline_name,
                    dataset="events",
                    scope="test",
                    worker_id="worker-1",
                    limit=1,
                    lease_seconds=60,
                    max_retries=2,
                )
                assert len(claimed) == 1
                repository.fail_file(
                    file_id,
                    worker_id="worker-1",
                    error=ValueError("retry"),
                )
                retried = repository.claim_files(
                    pipeline_name=pipeline_name,
                    dataset="events",
                    scope="test",
                    worker_id="worker-2",
                    limit=1,
                    lease_seconds=60,
                    max_retries=2,
                )
                assert retried[0].retry_count == 1
                repository.commit_file(
                    file_id,
                    worker_id="worker-2",
                    committed_at_utc=timestamp,
                    rows_parsed=72,
                    rows_inserted=72,
                    rescued_rows=0,
                    parser_version="integration",
                )
                status = connection.execute(
                    "SELECT status FROM ingestion.ingestion_files WHERE file_id = %s",
                    (file_id,),
                ).fetchone()[0]
                assert status == "COMMITTED"
                metrics = repository.pipeline_metrics(
                    pipeline_name=pipeline_name,
                    dataset="events",
                    scope="test",
                    max_retries=2,
                    observed_at_utc=timestamp,
                )
                assert metrics.succeeded_runs_24h == 1
                assert metrics.committed_files_24h == 1
                assert metrics.rows_parsed_24h == 72
                assert metrics.pending_files == 0
                raise RollbackIntegrationCheck
        except RollbackIntegrationCheck:
            pass

        remaining = connection.execute(
            "SELECT count(*) FROM ingestion.ingestion_runs WHERE pipeline_name = %s",
            (pipeline_name,),
        ).fetchone()[0]
        assert remaining == 0
    finally:
        connection.close()


def test_repository_recovers_stale_collector_and_expired_file_lease() -> None:
    connection = connect_control_plane()
    ensure_ingestion_state(connection)
    repository = PostgresIngestionRepository(connection)
    suffix = uuid4().hex
    pipeline_name = f"recovery_{suffix}"
    timestamp = datetime.now(UTC).replace(microsecond=0)

    def start(started_at: datetime):
        return repository.start_run(
            **_run_values(
                pipeline_name,
                timestamp,
                started_at_utc=started_at,
            ),
            stale_after_seconds=60,
        )

    try:
        try:
            with connection.transaction():
                stale = start(timestamp - timedelta(hours=1))
                recovered = start(timestamp)
                assert recovered.attempt_number == 2
                stale_status = connection.execute(
                    "SELECT status FROM ingestion.ingestion_runs WHERE attempt_id = %s",
                    (stale.attempt_id,),
                ).fetchone()[0]
                assert stale_status == "FAILED"

                object_key = f"bronze/files/recovery/{recovered.attempt_id}.json"
                file_id = _register_file(
                    repository,
                    attempt_id=recovered.attempt_id,
                    object_key=object_key,
                )
                repository.record_file(
                    file_id=file_id,
                    metadata=SourceObjectMetadata(
                        object_key=object_key,
                        size_bytes=2,
                        sha256="b" * 64,
                        content_type="application/json",
                    ),
                    http_status=200,
                    request_attempt_count=1,
                )
                repository.validate_file(file_id, received_item_count=1)
                repository.succeed_run(recovered.attempt_id, completed_at_utc=timestamp)
                first_claim = repository.claim_files(
                    pipeline_name=pipeline_name,
                    dataset="events",
                    scope="test",
                    worker_id="dead-worker",
                    limit=1,
                    lease_seconds=60,
                    max_retries=2,
                )
                assert len(first_claim) == 1
                connection.execute(
                    """
                    UPDATE ingestion.ingestion_files
                    SET lease_expires_at_utc = CURRENT_TIMESTAMP - INTERVAL '1 second'
                    WHERE file_id = %s
                    """,
                    (file_id,),
                )
                reclaimed = repository.claim_files(
                    pipeline_name=pipeline_name,
                    dataset="events",
                    scope="test",
                    worker_id="recovery-worker",
                    limit=1,
                    lease_seconds=60,
                    max_retries=2,
                )
                assert len(reclaimed) == 1
                assert reclaimed[0].retry_count == 1
                raise RollbackIntegrationCheck
        except RollbackIntegrationCheck:
            pass
    finally:
        connection.close()


def test_control_plane_read_does_not_rollback_later_checkpoints_on_close() -> None:
    """Regression: a quota SELECT must not leave an implicit transaction open."""
    connection = connect_control_plane()
    ensure_ingestion_state(connection)
    repository = PostgresIngestionRepository(connection)
    pipeline_name = f"transaction_{uuid4().hex}"
    timestamp = datetime.now(UTC).replace(microsecond=0)
    run = repository.start_run(**_run_values(pipeline_name, timestamp))
    try:
        assert (
            repository.effective_call_count(
                pipeline_name=pipeline_name,
                since_utc=timestamp - timedelta(days=1),
            )
            == 0
        )
        object_key = f"bronze/files/transaction/{run.attempt_id}.json"
        file_id = _register_file(
            repository,
            attempt_id=run.attempt_id,
            object_key=object_key,
        )
        repository.record_file(
            file_id=file_id,
            metadata=SourceObjectMetadata(
                object_key=object_key,
                size_bytes=2,
                sha256="c" * 64,
                content_type="application/json",
            ),
            http_status=200,
            request_attempt_count=1,
        )
        repository.validate_file(file_id, received_item_count=1)
        repository.succeed_run(run.attempt_id, completed_at_utc=timestamp)
        connection.close()

        observer = connect_control_plane()
        try:
            persisted = observer.execute(
                "SELECT status FROM ingestion.ingestion_runs WHERE attempt_id = %s",
                (run.attempt_id,),
            ).fetchone()
            assert persisted == ("SUCCEEDED",)
        finally:
            observer.close()
    finally:
        if not connection.closed:
            connection.close()
        cleanup = connect_control_plane()
        try:
            with cleanup.transaction():
                cleanup.execute(
                    "DELETE FROM ingestion.ingestion_files WHERE attempt_id = %s",
                    (run.attempt_id,),
                )
                cleanup.execute(
                    "DELETE FROM ingestion.ingestion_runs WHERE attempt_id = %s",
                    (run.attempt_id,),
                )
        finally:
            cleanup.close()
