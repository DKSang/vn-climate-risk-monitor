"""Test nạp cấu hình nguồn từ YAML — thêm nguồn mới không cần viết Python."""

from __future__ import annotations

from pathlib import Path

import pytest

from autoloader.config import SourceConfig

MINIMAL = """
name: my_source
dataset: my_dataset
discovery:
  prefix: bronze/files/my_source
transform:
  sql_file: my_source.sql
  target: catalog1.silver.stg_my_source
"""


def write(tmp_path: Path, body: str, *, sql: str = "SELECT 1") -> Path:
    (tmp_path / "my_source.sql").write_text(sql, encoding="utf-8")
    config_path = tmp_path / "my_source.yml"
    config_path.write_text(body, encoding="utf-8")
    return config_path


def test_loads_minimal_config_with_sensible_defaults(tmp_path: Path) -> None:
    config = SourceConfig.from_yaml(write(tmp_path, MINIMAL))

    assert config.name == "my_source"
    assert config.dataset == "my_dataset"
    assert config.scope == "production"
    assert config.discovery.pattern == "**/*.json"
    assert config.loader.batch_size == 10
    assert config.loader.max_retries == 3


def test_loader_settings_are_overridable(tmp_path: Path) -> None:
    body = MINIMAL + """
scope: canary
loader:
  batch_size: 1
  lease_seconds: 60
  max_retries: 5
  max_batches: 7
"""
    config = SourceConfig.from_yaml(write(tmp_path, body))

    assert config.scope == "canary"
    assert config.loader.batch_size == 1
    assert config.loader.lease_seconds == 60
    assert config.loader.max_retries == 5
    assert config.loader.max_batches == 7


def test_sql_is_read_relative_to_the_config_file(tmp_path: Path) -> None:
    config = SourceConfig.from_yaml(
        write(tmp_path, MINIMAL, sql="SELECT * FROM read_json_auto({{ files }})")
    )

    assert "{{ files }}" in config.sql


def test_missing_required_key_is_reported_with_the_file_name(tmp_path: Path) -> None:
    body = """
name: broken
discovery:
  prefix: x
transform:
  sql_file: my_source.sql
  target: t
"""
    path = write(tmp_path, body)

    with pytest.raises(ValueError, match="thiếu khoá bắt buộc"):
        SourceConfig.from_yaml(path)


def test_missing_sql_file_fails_loudly(tmp_path: Path) -> None:
    config_path = tmp_path / "my_source.yml"
    config_path.write_text(MINIMAL, encoding="utf-8")

    config = SourceConfig.from_yaml(config_path)

    with pytest.raises(FileNotFoundError, match="my_source.sql"):
        _ = config.sql


def test_project_sources_are_all_valid() -> None:
    """Cấu hình thật trong repo phải nạp được — bắt lỗi gõ sai sớm."""
    sources = sorted(Path("sources").glob("*.yml"))
    assert sources, "không tìm thấy nguồn nào"

    for path in sources:
        config = SourceConfig.from_yaml(path)
        assert config.name
        assert config.transform.target.count(".") == 2, (
            f"{path}: target phải dạng catalog.schema.table"
        )
        assert "{{ files }}" in config.sql, f"{path}: SQL thiếu placeholder"


def test_project_sources_never_restate_a_loader_default() -> None:
    """Khai báo lại giá trị mặc định là nhiễu — chỉ ghi knob thật sự lệch."""
    import yaml

    from autoloader.config import LoaderConfig

    default = LoaderConfig()
    for path in sorted(Path("sources").glob("*.yml")):
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        for key, value in (raw.get("loader") or {}).items():
            assert value != getattr(default, key), (
                f"{path}: loader.{key}={value} trùng LoaderConfig default"
            )
