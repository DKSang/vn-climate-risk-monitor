from types import SimpleNamespace

import pytest
from requests import HTTPError

from vn_climate_risk_monitor.ingestion.fetch import Location, QuotaExhausted, _land

PREFIX = "bronze/files/open_meteo/historical_weather_hourly/backfill/year=2001/month=01"
SETTINGS = SimpleNamespace(location_batch_size=2, request_timeout_seconds=5)


class FakeMinio:
    def __init__(self, existing_keys: set[str]) -> None:
        self.objects: dict[str, bytes] = {key: b"old" for key in existing_keys}

    def list_objects(self, bucket: str, prefix: str, recursive: bool = True):
        for name in sorted(self.objects):
            if name.startswith(prefix):
                yield SimpleNamespace(object_name=name)

    def put_object(
        self,
        bucket: str,
        key: str,
        data: SimpleNamespace,
        length: int,
        content_type: str = "",
    ) -> None:
        self.objects[key] = data.read()


class FakeSession:
    def __init__(self) -> None:
        self.calls: list[dict[str, str]] = []

    def get(self, url: str, params: dict[str, str], timeout: int) -> SimpleNamespace:
        self.calls.append(params)
        return SimpleNamespace(
            raise_for_status=lambda: None, json=lambda: [{"hourly": {"time": []}}]
        )


def _land_with(client: FakeMinio, session: FakeSession) -> int:
    return _land(
        locations=tuple(Location(f"P{i:03}", 21.0, 105.8) for i in range(4)),
        settings=SETTINGS,
        session=session,
        pacer=SimpleNamespace(wait=lambda units: 0.0),
        client=client,
        bucket="vn-climate",
        prefix=PREFIX,
        extra_params={
            "hourly": "precipitation,rain",
            "start_date": "2001-01-01",
            "end_date": "2001-01-31",
        },
        url="https://archive-api.open-meteo.com/v1/archive",
        days=31,
    )


def test_resume_recognizes_old_collector_layout() -> None:
    old_key = f"{PREFIX}/archive_2001_20010131_a1_6f3c2f/response_000.json"
    client = FakeMinio({old_key})
    session = FakeSession()

    written = _land_with(client, session)

    assert written == 1
    assert len(session.calls) == 1
    new_keys = [key for key in client.objects if key != old_key]
    assert len(new_keys) == 1
    assert new_keys[0].startswith(f"{PREFIX}/run_")
    assert new_keys[0].endswith("/response_001.json")
    assert client.objects[old_key] == b"old"


def test_complete_month_makes_no_requests() -> None:
    client = FakeMinio(
        {
            f"{PREFIX}/archive_2001_20010131_a1_6f3c2f/response_000.json",
            f"{PREFIX}/run_20260821T090000/response_001.json",
        }
    )
    session = FakeSession()

    written = _land_with(client, session)

    assert written == 0
    assert session.calls == []


def test_crash_mid_month_resumes_only_missing_files() -> None:
    crashed_run = "run_20260821T090000"
    client = FakeMinio({f"{PREFIX}/{crashed_run}/response_000.json"})
    session = FakeSession()

    written = _land_with(client, session)

    assert written == 1
    new_key = next(key for key in client.objects if crashed_run not in key)
    assert new_key.startswith(f"{PREFIX}/run_")
    assert new_key.endswith("/response_001.json")


def test_429_surviving_retries_raises_quota_exhausted() -> None:
    class Fake429Session:
        def get(self, url: str, params: dict[str, str], timeout: int) -> SimpleNamespace:
            response = SimpleNamespace(status_code=429)

            def raise_for_status() -> None:
                raise HTTPError("429 Too Many Requests", response=response)

            return SimpleNamespace(raise_for_status=raise_for_status)

    client = FakeMinio(set())

    with pytest.raises(QuotaExhausted):
        _land_with(client, Fake429Session())  # type: ignore[arg-type]

    # Chưa ghi file nào trước khi dừng.
    assert client.objects == {}
