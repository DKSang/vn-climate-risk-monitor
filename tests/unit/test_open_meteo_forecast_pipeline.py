from datetime import UTC, datetime
from uuid import UUID

from vn_climate_risk_monitor.ingestion.loaders.open_meteo_forecast_hourly import (
    LoadFailure,
    LoadSummary,
)
from vn_climate_risk_monitor.ingestion.open_meteo import RequestedLocation
from vn_climate_risk_monitor.ingestion.pipelines.open_meteo_forecast import (
    CollectionOutcome,
    ForecastPipeline,
)
from vn_climate_risk_monitor.ingestion.state import (
    RunAlreadySucceededError,
    RunAttempt,
    RunStatus,
)

ATTEMPT_ID = UUID("00000000-0000-0000-0000-000000000001")


class SuccessfulCollector:
    def collect(
        self, locations: tuple[RequestedLocation, ...], **values: object
    ) -> object:
        return type(
            "Collection",
            (),
            {
                "attempt": RunAttempt(
                    attempt_id=ATTEMPT_ID,
                    logical_run_id=UUID(int=2),
                    attempt_number=1,
                    logical_key="2026-08-21T10:15:00Z",
                    status=RunStatus.SUCCEEDED,
                )
            },
        )()


class AlreadyCollected:
    def collect(
        self, locations: tuple[RequestedLocation, ...], **values: object
    ) -> object:
        raise RunAlreadySucceededError("already succeeded")


class SequenceLoader:
    def __init__(self, batches: list[LoadSummary]) -> None:
        self.batches = iter(batches)
        self.calls = 0

    def load_available(self, **values: object) -> LoadSummary:
        self.calls += 1
        return next(self.batches)


def _summary(
    claimed: int,
    *,
    committed: int | None = None,
    failures: tuple[LoadFailure, ...] = (),
) -> LoadSummary:
    committed = claimed if committed is None else committed
    return LoadSummary(
        claimed_files=claimed,
        committed_files=committed,
        rows_parsed=committed * 72,
        rows_inserted=committed * 72,
        rescued_rows=0,
        failures=failures,
    )


def _run(collector: object, loader: SequenceLoader):
    location = RequestedLocation(
        ward_key=1,
        ward_code="00001",
        latitude=21.0,
        longitude=105.8,
    )
    return ForecastPipeline(collector=collector, loader=loader).run(
        (location,),
        scheduled_at_utc=datetime(2026, 8, 21, 10, 15, tzinfo=UTC),
        scope="production",
        worker_id="worker-1",
        loader_batch_size=2,
        lease_seconds=300,
        max_retries=3,
        max_load_batches=10,
    )


def test_pipeline_collects_then_drains_loader_until_checkpoint_is_empty() -> None:
    loader = SequenceLoader([_summary(2), _summary(1), _summary(0)])

    result = _run(SuccessfulCollector(), loader)

    assert result.collection_outcome == CollectionOutcome.SUCCEEDED
    assert result.attempt_id == ATTEMPT_ID
    assert result.load_batches == 2
    assert result.committed_files == 3
    assert result.rows_inserted == 216
    assert loader.calls == 3


def test_pipeline_retry_skips_collection_but_resumes_loader_checkpoint() -> None:
    loader = SequenceLoader([_summary(1), _summary(0)])

    result = _run(AlreadyCollected(), loader)

    assert result.collection_outcome == CollectionOutcome.ALREADY_SUCCEEDED
    assert result.attempt_id is None
    assert result.committed_files == 1


def test_pipeline_stops_current_invocation_after_loader_failure() -> None:
    failure = LoadFailure(
        file_id=UUID(int=3),
        object_key="response.json",
        error_type="ValueError",
        error_message="broken",
    )
    loader = SequenceLoader([_summary(1, committed=0, failures=(failure,))])

    result = _run(SuccessfulCollector(), loader)

    assert result.committed_files == 0
    assert result.failures == (failure,)
    assert loader.calls == 1
