from datetime import date
from types import SimpleNamespace

from activities.copy import row_env
from vn_climate_risk_monitor import open_meteo
from vn_climate_risk_monitor.open_meteo import Location, missing_rows

PREFIX = "bronze/files/open_meteo/historical_weather_hourly/backfill/year=2001/month=01"
LOCATIONS = tuple(Location(f"P{i:03}", 21.0 + i * 0.01, 105.8) for i in range(4))
SETTINGS = SimpleNamespace(
    archive_url="https://archive-api.open-meteo.com/v1/archive",
    forecast_url="https://api.open-meteo.com/v1/forecast",
    archive_model="era5",
    forecast_model="best_match",
    forecast_hours=72,
    location_batch_size=2,
)


class FakeMinio:
    def __init__(self, existing_keys: set[str]) -> None:
        self.objects: dict[str, bytes] = {key: b"old" for key in existing_keys}

    def list_objects(self, bucket: str, prefix: str, recursive: bool = True):
        for name in sorted(self.objects):
            if name.startswith(prefix):
                yield SimpleNamespace(object_name=name)


def _lookup(existing: set[str], run: str = "run_test") -> list[dict[str, str]]:
    return missing_rows(
        targets=[date(2001, 1, 1)],
        locations=LOCATIONS,
        settings=SETTINGS,  # type: ignore[arg-type]
        client=FakeMinio(existing),  # type: ignore[arg-type]
        bucket="vn-climate",
        run=run,
        dataset="archive",
    )


def test_resume_recognizes_old_collector_layout() -> None:
    old_key = f"{PREFIX}/archive_2001_20010131_a1_6f3c2f/response_000.json"
    rows = _lookup({old_key}, run="run_new")
    assert [row["key"] for row in rows] == [f"{PREFIX}/run_new/response_001.json"]


def test_complete_month_makes_no_rows() -> None:
    assert (
        _lookup(
            {
                f"{PREFIX}/archive_2001_20010131_a1_6f3c2f/response_000.json",
                f"{PREFIX}/run_20260821T090000/response_001.json",
            }
        )
        == []
    )


def test_crash_mid_month_resumes_only_missing_files() -> None:
    rows = _lookup(
        {f"{PREFIX}/run_20260821T090000/response_000.json"}, run="run_resume"
    )
    assert rows[0]["key"] == f"{PREFIX}/run_resume/response_001.json"


def test_lookup_row_keeps_batch_contract() -> None:
    rows = _lookup(existing=set())
    assert len(rows) == 2
    assert "timeformat=unixtime" in rows[0]["url"]
    assert "latitude=21.000000,21.010000" in rows[0]["url"]
    assert rows[1]["key"].endswith("/response_001.json")


def test_forecast_lookup_uses_forecast_url_and_prefix() -> None:
    from datetime import UTC, datetime

    rows = missing_rows(
        targets=[datetime(2026, 8, 27, 9, tzinfo=UTC)],
        locations=LOCATIONS,
        settings=SETTINGS,  # type: ignore[arg-type]
        client=FakeMinio(set()),  # type: ignore[arg-type]
        bucket="vn-climate",
        run="run_fc",
        dataset="forecast",
    )
    assert rows[0]["url"].startswith("https://api.open-meteo.com/v1/forecast?")
    assert rows[0]["key"].startswith(
        "bronze/files/open_meteo/forecast/incremental/2026/08/27/09/run_fc/"
    )


def test_row_env_exposes_every_column() -> None:
    assert row_env({"url": "https://x", "key": "a/b.json"}) == {
        "ROW_URL": "https://x",
        "ROW_KEY": "a/b.json",
    }


def test_fetch_does_not_write_a_lookup_csv() -> None:
    assert not hasattr(open_meteo, "LOOKUP_FILE")
