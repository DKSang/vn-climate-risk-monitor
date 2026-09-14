"""Test planner Open-Meteo — định tuyến model theo thời kỳ và fetch theo ô lưới."""

from __future__ import annotations

import importlib
import inspect
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from vn_climate_risk_monitor.sources.open_meteo import cli as open_meteo
from vn_climate_risk_monitor.sources.open_meteo import grid, planner
from vn_climate_risk_monitor.sources.open_meteo.planner import (
    ARCHIVE_MODELS,
    ERA5,
    IFS,
    Location,
    archive_tasks,
    days_in_month,
    forecast_run_id,
    forecast_tasks,
    model_for_month,
    month_params,
)

LOCATIONS = tuple(Location(f"P{i:03}", 21.0 + i * 0.01, 105.8) for i in range(4))
SETTINGS = SimpleNamespace(
    archive_url="https://archive-api.open-meteo.com/v1/archive",
    forecast_url="https://api.open-meteo.com/v1/forecast",
    forecast_model="best_match",
    forecast_hours=72,
    location_batch_size=2,
)


def seed(tmp_path: Path) -> Path:
    """Bản đồ ô lưới tối thiểu: 4 phường → 2 ô era5, 3 ô ecmwf_ifs."""
    rows = [
        grid.WardGrid(
            "era5",
            f"P{i:03}",
            21.0 + i * 0.01,
            105.8,
            21.0,
            105.75 + (i // 2) * 0.25,
            10.0,
        )
        for i in range(4)
    ] + [
        # P002/P003 share a cell, leaving 3 cells across 4 wards.
        grid.WardGrid(
            "ecmwf_ifs",
            f"P{i:03}",
            21.0 + i * 0.01,
            105.8,
            21.0 + min(i, 2) * 0.09,
            105.8,
            12.0,
        )
        for i in range(4)
    ]
    path = tmp_path / "ward_grid_map_seed.csv"
    grid.write_seed(rows, path)
    return path


def archive_plan(tmp_path: Path, months, *, existing=frozenset(), covered=None):
    months = list(months)
    seed_path = seed(tmp_path)
    prefixes = [month_params(month, model_for_month(month))[0] for month in months]
    return archive_tasks(
        months=months,
        settings=SETTINGS,  # type: ignore[arg-type]
        run="run_test",
        covered=covered or {},
        cells_by_model={
            model.name: grid.cells_for(model.name, seed_path)
            for model in ARCHIVE_MODELS
        },
        existing={
            prefix: {
                key.rsplit("/", 1)[-1]
                for key in existing
                if key.startswith(f"{prefix}/")
            }
            for prefix in prefixes
        },
    )


# ── định tuyến model (era5 <2017, ecmwf_ifs >=2017) ──────────────────────────────
def test_pre_2017_uses_era5_and_2017_onward_uses_ifs() -> None:
    assert model_for_month(date(2016, 12, 1)) is ERA5
    assert model_for_month(date(2017, 1, 1)) is IFS
    assert model_for_month(date(2026, 8, 1)) is IFS


def test_each_model_lands_under_its_own_prefix(tmp_path: Path) -> None:
    """Hai bảng bronze khác nhau, nên hai prefix phải tách hẳn."""
    tasks = archive_plan(tmp_path, [date(2016, 6, 1), date(2018, 6, 1)])

    keys = [t.key for t in tasks]
    assert any(k.startswith(f"{ERA5.prefix}/year=2016/month=06/") for k in keys)
    assert any(k.startswith(f"{IFS.prefix}/year=2018/month=06/") for k in keys)
    assert not any(k.startswith(IFS.prefix) for k in keys if "2016" in k)


# ── fetch theo ô, không theo phường ───────────────────────────────────────────
def test_archive_requests_grid_cells_not_wards(tmp_path: Path) -> None:
    """Điểm cốt lõi: 4 phường gộp thành 2 ô era5 → chỉ 1 request thay vì 2."""
    tasks = archive_plan(tmp_path, [date(2016, 6, 1)])

    assert len(tasks) == 1
    assert "latitude=21.000000,21.000000" in tasks[0].url
    assert tasks[0].url.count("%2C") + tasks[0].url.count(",") >= 1


def test_ifs_month_uses_the_finer_ifs_cells(tmp_path: Path) -> None:
    tasks = archive_plan(tmp_path, [date(2018, 6, 1)])

    # 3 ô ifs, batch_size=2 → 2 request
    assert len(tasks) == 2


# ── chi phí quota tính theo LÔ THẬT ───────────────────────────────────────────
def test_units_follow_actual_batch_length_not_nominal(tmp_path: Path) -> None:
    """Hồi quy: lô cuối ngắn hơn từng bị tính (và bị nghỉ) như lô đầy."""
    tasks = archive_plan(tmp_path, [date(2018, 6, 1)])

    days = days_in_month(date(2018, 6, 1))
    full = planner.effective_call_units(locations=2, days=days, variables=5)
    runt = planner.effective_call_units(locations=1, days=days, variables=5)
    assert sorted(t.units for t in tasks) == sorted([runt, full])
    assert runt < full


# ── resume ────────────────────────────────────────────────────────────────────
def test_month_already_complete_in_bronze_is_skipped(tmp_path: Path) -> None:
    """Skip archive months by staging coverage, not file names."""
    tasks = archive_plan(
        tmp_path, [date(2016, 6, 1)], covered={"era5": frozenset({date(2016, 6, 1)})}
    )

    assert tasks == []


def test_existing_object_in_this_run_layout_is_not_refetched(tmp_path: Path) -> None:
    prefix = f"{ERA5.prefix}/year=2016/month=06"
    tasks = archive_plan(
        tmp_path, [date(2016, 6, 1)], existing={f"{prefix}/run_old/response_000.json"}
    )

    assert tasks == []


def test_coverage_of_one_model_does_not_skip_the_other(tmp_path: Path) -> None:
    tasks = archive_plan(
        tmp_path,
        [date(2016, 6, 1), date(2018, 6, 1)],
        covered={"era5": frozenset({date(2016, 6, 1)})},
    )

    assert tasks and all(t.key.startswith(IFS.prefix) for t in tasks)


# ── forecast vẫn theo phường, cố ý ────────────────────────────────────────────
def test_forecast_still_batches_wards(tmp_path: Path) -> None:
    tasks = forecast_tasks(
        slots=[datetime(2026, 8, 27, 9, tzinfo=UTC)],
        locations=LOCATIONS,
        settings=SETTINGS,  # type: ignore[arg-type]
        run="run_fc",
        existing={},
    )

    assert len(tasks) == 2  # 4 phường / batch_size 2
    assert tasks[0].url.startswith("https://api.open-meteo.com/v1/forecast?")
    assert tasks[0].key.startswith(
        "bronze/files/open_meteo/forecast/incremental/2026/08/27/09/run_fc/"
    )
    assert "timeformat=unixtime" in tasks[0].url


def test_forecast_run_id_is_stable_for_retries_in_the_same_hour() -> None:
    first = datetime(2026, 8, 27, 9, 1, tzinfo=UTC)
    retry = datetime(2026, 8, 27, 9, 58, tzinfo=UTC)

    assert forecast_run_id(first) == "run_20260827T090000"
    assert forecast_run_id(retry) == forecast_run_id(first)


def test_archive_model_is_not_an_env_knob_anymore() -> None:
    """Model chọn theo thời kỳ; một biến môi trường sẽ âm thầm ghi đè logic đó."""
    from vn_climate_risk_monitor.platform.settings import OpenMeteoSettings

    assert "archive_model" not in OpenMeteoSettings.__dataclass_fields__


def test_fetch_does_not_write_a_lookup_csv() -> None:
    assert not hasattr(open_meteo, "LOOKUP_FILE")


def test_source_planner_boundary_has_no_storage_inputs() -> None:
    try:
        planner = importlib.import_module(
            "vn_climate_risk_monitor.sources.open_meteo.planner"
        )
    except ModuleNotFoundError:
        pytest.fail("source-specific planning module is missing")

    archive_parameters = inspect.signature(planner.archive_tasks).parameters
    forecast_parameters = inspect.signature(planner.forecast_tasks).parameters
    assert "client" not in archive_parameters
    assert "bucket" not in archive_parameters
    assert "seed_path" not in archive_parameters
    assert "client" not in forecast_parameters
    assert "bucket" not in forecast_parameters
