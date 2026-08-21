from datetime import UTC, date, datetime
from uuid import UUID

from vn_climate_risk_monitor.ingestion.loaders.open_meteo_archive_hourly import (
    LoadFailure,
    LoadSummary,
)
from vn_climate_risk_monitor.ingestion.open_meteo import (
    RequestedLocation,
    plan_archive_year,
)
from vn_climate_risk_monitor.ingestion.pipelines.open_meteo_archive import (
    ArchivePipeline,
    CollectionOutcome,
    _selected_plans,
)
from vn_climate_risk_monitor.ingestion.state import (
    RunAlreadySucceededError,
    RunAttempt,
    RunStatus,
)

ATTEMPT_ID = UUID("00000000-0000-0000-0000-000000000301")


class SuccessfulCollector:
    def collect_year(self, year_plan: object, **values: object) -> object:
        return type(
            "Collection",
            (),
            {
                "attempt": RunAttempt(
                    attempt_id=ATTEMPT_ID,
                    logical_run_id=UUID(int=302),
                    attempt_number=1,
                    logical_key="model=era5/year=2000",
                    status=RunStatus.SUCCEEDED,
                )
            },
        )()


class AlreadyCollected:
    def collect_year(self, year_plan: object, **values: object) -> object:
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
        rows_parsed=committed * 744,
        rows_inserted=committed * 744,
        rescued_rows=0,
        failures=failures,
    )


def _plan():
    locations = (
        RequestedLocation(
            ward_key=1,
            ward_code="00001",
            latitude=21.0,
            longitude=105.8,
        ),
    )
    return plan_archive_year(
        locations,
        start_date=date(2000, 1, 1),
        end_date=date(2000, 1, 31),
        model="era5",
        location_batch_size=25,
    )


def test_year_runtime_plan_uses_monthly_recovery_checkpoints() -> None:
    locations = (
        RequestedLocation(
            ward_key=1,
            ward_code="00001",
            latitude=21.0,
            longitude=105.8,
        ),
    )

    plans = _selected_plans(
        locations=locations,
        model="era5",
        location_batch_size=25,
        available_through=date(2026, 8, 16),
        start_year=2000,
        end_date=None,
        year=2000,
        month=None,
        tail=False,
    )

    assert len(plans) == 12
    assert plans[0].start_date == date(2000, 1, 1)
    assert plans[0].end_date == date(2000, 1, 31)
    assert plans[-1].start_date == date(2000, 12, 1)
    assert plans[-1].end_date == date(2000, 12, 31)


def test_tail_runtime_plan_contains_one_candidate_day() -> None:
    locations = (
        RequestedLocation(
            ward_key=1,
            ward_code="00001",
            latitude=21.0,
            longitude=105.8,
        ),
    )

    plans = _selected_plans(
        locations=locations,
        model="era5",
        location_batch_size=25,
        available_through=date(2026, 8, 16),
        start_year=2000,
        end_date=None,
        year=None,
        month=None,
        tail=True,
    )

    assert len(plans) == 1
    assert plans[0].start_date == plans[0].end_date == date(2026, 8, 16)


def _run(collector: object, loader: SequenceLoader):
    return ArchivePipeline(collector=collector, loader=loader).run(
        _plan(),
        scheduled_at_utc=datetime(2026, 8, 21, 12, tzinfo=UTC),
        scope="backfill",
        worker_id="worker-1",
        loader_batch_size=10,
        lease_seconds=300,
        max_retries=3,
        max_load_batches=10,
    )


def test_archive_pipeline_collects_then_drains_loader() -> None:
    loader = SequenceLoader([_summary(2), _summary(1), _summary(0)])

    result = _run(SuccessfulCollector(), loader)

    assert result.collection_outcome == CollectionOutcome.SUCCEEDED
    assert result.attempt_id == ATTEMPT_ID
    assert result.committed_files == 3
    assert result.rows_inserted == 2232
    assert loader.calls == 3


def test_archive_pipeline_retry_only_resumes_loader() -> None:
    loader = SequenceLoader([_summary(1), _summary(0)])

    result = _run(AlreadyCollected(), loader)

    assert result.collection_outcome == CollectionOutcome.ALREADY_SUCCEEDED
    assert result.attempt_id is None
    assert result.committed_files == 1


def test_archive_pipeline_stops_after_loader_failure() -> None:
    failure = LoadFailure(
        file_id=UUID(int=303),
        object_key="response.json",
        error_type="ValueError",
        error_message="broken",
    )
    loader = SequenceLoader([_summary(1, committed=0, failures=(failure,))])

    result = _run(SuccessfulCollector(), loader)

    assert result.committed_files == 0
    assert result.failures == (failure,)
    assert loader.calls == 1
