"""Dashboard query and publication contracts."""

from __future__ import annotations

import ast
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).parents[2]
DASHBOARD = ROOT / "serving" / "dashboard"


def _function_names(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    return {
        node.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }


def test_dashboard_queries_are_split_by_data_domain() -> None:
    common = DASHBOARD / "common.py"
    forecast = DASHBOARD / "forecast.py"
    archive = DASHBOARD / "archive.py"

    assert common.is_file()
    assert forecast.is_file()
    assert archive.is_file()

    assert {
        "load_serving_snapshot",
        "load_all_wards",
    } <= _function_names(common)
    assert {
        "load_forecast_metadata",
        "load_forecast_by_hour",
        "load_ward_forecast_timeseries",
    } <= _function_names(forecast)
    assert {
        "load_archive_models",
        "load_archive_by_hour",
    } <= _function_names(archive)


def test_dashboard_pages_import_queries_from_their_domain_modules() -> None:
    page_sources = {
        path.name: path.read_text(encoding="utf-8")
        for path in (DASHBOARD / "pages").glob("*.py")
    }

    assert "from serving.dashboard.common import" in page_sources["01_forecast_map.py"]
    assert "from serving.dashboard.forecast import" in page_sources["01_forecast_map.py"]
    assert "from serving.dashboard.common import" in page_sources["02_ward_drilldown.py"]
    assert "from serving.dashboard.forecast import" in page_sources["02_ward_drilldown.py"]
    assert "from serving.dashboard.archive import" in page_sources["03_archive_replay.py"]
    assert "sys.path.insert" not in "\n".join(page_sources.values())


def test_rain_bands_are_owned_by_gold() -> None:
    ui = (DASHBOARD / "ui.py").read_text(encoding="utf-8")
    forecast = (
        ROOT / "transform/models/marts/fct_rain_forecast_hourly.sql"
    ).read_text(encoding="utf-8")
    archive = (ROOT / "transform/models/marts/fct_rain_archive_hourly.sql").read_text(
        encoding="utf-8"
    )

    assert "classify_rain_band" not in ui
    assert "forecast_next_24h_band" in forecast
    assert "hanoi_rain_scenario_level" in forecast
    assert "hanoi_rain_scenario_level" in archive

    archive_queries = (DASHBOARD / "archive.py").read_text(encoding="utf-8")
    assert archive_queries.count("f.hanoi_rain_scenario_level") >= 2


def test_forecast_and_archive_keep_reader_return_semantics(monkeypatch) -> None:
    monkeypatch.syspath_prepend(str(ROOT))
    from serving.dashboard import archive, forecast

    metadata = {"horizon_hours": 72, "grid_count": 12}
    archive_models = [{"weather_model": "era5", "row_count": 24}]
    monkeypatch.setattr(forecast, "_read_record", lambda *args, **kwargs: metadata)
    monkeypatch.setattr(
        archive, "_read_records", lambda *args, **kwargs: archive_models
    )

    assert forecast.load_forecast_metadata(11) is metadata
    assert archive.load_archive_models(11) is archive_models


def test_forecast_hour_summary_returns_forward_rain_maxima(monkeypatch) -> None:
    monkeypatch.syspath_prepend(str(ROOT))
    from serving.dashboard import forecast

    captured: dict[str, str] = {}

    def read_record(query, *args, **kwargs):
        captured["query"] = query
        return {}

    monkeypatch.setattr(forecast, "_read_record", read_record)

    forecast.load_forecast_hour_summary("2026-09-14T09:00:00Z", 2366)

    assert "MAX(forecast_next_1h_mm) AS max_forecast_next_1h_mm" in captured["query"]
    assert "MAX(forecast_next_12h_mm) AS max_forecast_next_12h_mm" in captured["query"]
    assert "MAX(forecast_next_24h_mm) AS max_forecast_next_24h_mm" in captured["query"]


def test_query_fallbacks_keep_dashboard_component_types(monkeypatch) -> None:
    monkeypatch.syspath_prepend(str(ROOT))
    from serving.dashboard import archive, forecast

    def fail(*args, **kwargs):
        raise RuntimeError("catalog unavailable")

    monkeypatch.setattr(forecast, "_read_dataframe", fail)
    monkeypatch.setattr(archive, "_read_dataframe", fail)
    monkeypatch.setattr(forecast, "_read_records", fail)

    assert isinstance(forecast.load_forecast_hours(), list)
    assert isinstance(forecast.load_forecast_by_hour("2026-01-01"), pd.DataFrame)
    assert isinstance(archive.load_archive_models(), list)
    assert isinstance(archive.load_archive_by_hour("era5", "2026-01-01"), pd.DataFrame)


def test_serving_snapshot_requires_a_validated_processing_publication(monkeypatch) -> None:
    monkeypatch.syspath_prepend(str(ROOT))
    from serving.dashboard import common

    class Cursor:
        def __init__(self) -> None:
            self.query_count = 0

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, query, params):
            self.query_count += 1
            assert "processing.processing_runs" in query
            assert params == ("forecast", "gold", "fct_rain_forecast_hourly")

        def fetchone(self):
            return None

    class Connection:
        def __init__(self) -> None:
            self.cursor_instance = Cursor()

        def cursor(self):
            return self.cursor_instance

        def close(self):
            pass

    connection = Connection()
    postgres = type(
        "PostgresSettings",
        (),
        {"ducklake_connection_string": "postgresql://control"},
    )()
    settings = type("Settings", (), {"postgres": postgres})()
    monkeypatch.setattr(common, "load_settings", lambda: settings)
    monkeypatch.setattr(common, "connect_control_plane", lambda _dsn: connection)

    snapshot = common.load_serving_snapshot()

    assert snapshot == {}
    assert connection.cursor_instance.query_count == 1
