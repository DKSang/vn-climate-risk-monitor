from datetime import UTC, datetime

import pytest

from vn_climate_risk_monitor.ingestion.layout import BronzeFilesLayout


def test_bronze_files_layout_is_utc_and_zero_padded() -> None:
    layout = BronzeFilesLayout("Open-Meteo", "Forecast", "Incremental")

    key = layout.object_key(
        datetime(2026, 8, 21, 10, 15, tzinfo=UTC),
        "run_01",
        "response_000.json",
    )

    assert key == (
        "bronze/files/open_meteo/forecast/incremental/"
        "2026/08/21/10/run_01/response_000.json"
    )


def test_bronze_files_layout_rejects_path_traversal() -> None:
    layout = BronzeFilesLayout("open_meteo", "../forecast", "incremental")

    with pytest.raises(ValueError, match="dataset"):
        layout.run_prefix(datetime.now(UTC), "run_01")


def test_bronze_files_layout_requires_aware_timestamp() -> None:
    layout = BronzeFilesLayout("open_meteo", "forecast", "incremental")

    with pytest.raises(ValueError, match="timezone-aware"):
        layout.run_prefix(datetime(2026, 8, 21, 10, 15), "run_01")  # noqa: DTZ001
