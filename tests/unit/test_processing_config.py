"""Process config nạp từ YAML, và duration parser."""

from __future__ import annotations

import subprocess
import sys
from datetime import timedelta
from pathlib import Path

import pytest

from processing.config import (
    DEFAULT_SAFETY_LAG,
    ProcessConfig,
    SourceBinding,
    parse_duration,
)

MINIMAL = """
process_key: p
target: gold.t
sources:
  - ref: archive_hourly
"""


def write(tmp_path: Path, body: str) -> Path:
    path = tmp_path / "p.yml"
    path.write_text(body, encoding="utf-8")
    return path


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("15 minutes", timedelta(minutes=15)),
        ("1 minute", timedelta(minutes=1)),
        ("71 hours", timedelta(hours=71)),
        ("2 days", timedelta(days=2)),
        ("30 seconds", timedelta(seconds=30)),
        (90, timedelta(seconds=90)),
    ],
)
def test_parse_duration(text: str | int, expected: timedelta) -> None:
    assert parse_duration(text) == expected


@pytest.mark.parametrize("text", ["", "soon", "15 fortnights", "minutes 15"])
def test_parse_duration_rejects_garbage(text: str) -> None:
    with pytest.raises(ValueError, match="duration|Đơn vị"):
        parse_duration(text)


def test_minimal_config_defaults(tmp_path: Path) -> None:
    config = ProcessConfig.from_yaml(write(tmp_path, MINIMAL))

    assert config.scope == "production"
    assert config.checkpoint.safety_lag == DEFAULT_SAFETY_LAG
    assert config.source_refs == ("archive_hourly",)
    assert config.sources[0].change_column == "_ingested_at"
    assert config.runner.select is None


def test_full_config(tmp_path: Path) -> None:
    config = ProcessConfig.from_yaml(
        write(
            tmp_path,
            """
process_key: rainfall_historical_hourly
target: gold.fct_rainfall_historical_hourly
scope: staging
sources:
  - ref: archive_hourly
    change_column: _ingested_at
  - ref: ifs_hourly
checkpoint:
  safety_lag: 30 minutes
runner:
  select: "+tag:historical"
""",
        )
    )

    assert config.scope == "staging"
    assert config.checkpoint.safety_lag == timedelta(minutes=30)
    assert config.source_refs == ("archive_hourly", "ifs_hourly")
    assert config.runner.select == "+tag:historical"


def test_missing_required_keys(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="thiếu khoá bắt buộc"):
        ProcessConfig.from_yaml(write(tmp_path, "process_key: p\n"))


def test_empty_sources_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="ít nhất một source"):
        ProcessConfig.from_yaml(
            write(tmp_path, "process_key: p\ntarget: gold.t\nsources: []\n")
        )


def test_duplicate_source_ref_is_rejected() -> None:
    """Hai binding cùng ref sẽ tranh nhau MỘT row checkpoint."""
    with pytest.raises(ValueError, match="bị trùng"):
        ProcessConfig(
            process_key="p",
            target="gold.t",
            sources=(SourceBinding(ref="a"), SourceBinding(ref="a")),
        )


def test_negative_safety_lag_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="safety_lag"):
        ProcessConfig.from_yaml(
            write(tmp_path, MINIMAL + "checkpoint:\n  safety_lag: -5\n")
        )


def test_every_shipped_config_is_loadable() -> None:
    """Mọi config thật trong repo phải parse được, không chỉ fixture trong test.

    Quét cả thư mục thay vì gọi đích danh một file: đổi tên process là chuyện
    thường, và test gãy vì đổi tên không nói lên điều gì.
    """
    paths = sorted(Path("processing").glob("*.yml"))

    assert paths, "không tìm thấy process config nào trong processing/"
    for path in paths:
        config = ProcessConfig.from_yaml(path)
        assert config.process_key == path.stem, path
        assert config.sources


def test_rain_gold_soft_delete_covers_forecast_bridge_keys() -> None:
    """Bridge có ba model; nguồn anti-join không được chỉ liệt kê hai archive."""
    config = ProcessConfig.from_yaml(Path("processing/rain_gold.yml"))
    bridge_rule = next(
        rule for rule in config.soft_delete if rule.target == "gold.bridge_ward_grid"
    )

    assert "ecmwf_ifs_fc" in bridge_rule.key_source_sql
    assert "LPAD(CAST(ward_code AS VARCHAR), 5, '0')" in bridge_rule.key_source_sql


def test_rain_gold_selects_only_archive_serving_contract() -> None:
    config = ProcessConfig.from_yaml(Path("processing/rain_gold.yml"))

    assert config.runner.select == (
        "dim_grid dim_ward dim_flood_point bridge_ward_grid "
        "fct_flood_event_observation fct_rain_archive_hourly"
    )


def test_forecast_processing_has_separate_silver_and_gold_checkpoints() -> None:
    silver = ProcessConfig.from_yaml(Path("processing/forecast_silver.yml"))
    gold = ProcessConfig.from_yaml(Path("processing/forecast_gold.yml"))

    assert silver.source_refs == ("stg_weather_forecast",)
    assert silver.runner.select == (
        "stg_open_meteo__weather_forecast_hourly int_weather_forecast_hourly"
    )
    assert gold.source_refs == ("int_weather_forecast_hourly",)
    assert gold.runner.select == (
        "bridge_ward_grid fct_rain_forecast_hourly "
        "fct_rain_forecast_current_hourly fct_rain_pressure_alert"
    )


def test_cli_module_is_importable() -> None:
    """Hồi quy: script từng tên `scripts/processing.py` và tự che package
    `processing` (thư mục script đứng đầu sys.path) — mọi lệnh đều ImportError."""
    result = subprocess.run(
        [sys.executable, "scripts/run_processing.py", "--help"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "reprocess-from" in result.stdout

    run_help = subprocess.run(
        [sys.executable, "scripts/run_processing.py", "run", "--help"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert run_help.returncode == 0, run_help.stderr
    assert "--full-refresh" in run_help.stdout
