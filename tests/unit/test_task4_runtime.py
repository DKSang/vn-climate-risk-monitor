"""Contract tests for the lean orchestration and runtime surface."""

from __future__ import annotations

import ast
import importlib.util
import shlex
import tomllib
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).parents[2]
TRANSFORM = ROOT / "transform"


def _literal_keyword(call: ast.Call, name: str) -> str:
    keyword = next(item for item in call.keywords if item.arg == name)
    value = keyword.value
    if isinstance(value, ast.Constant) and isinstance(value.value, str):
        return value.value
    return ast.unparse(value)


def _dag_tasks(path: Path) -> tuple[dict[str, str], str]:
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    tasks: dict[str, str] = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if not (
            isinstance(node.func, ast.Name) and node.func.id == "BashOperator"
        ):
            continue
        task_id = _literal_keyword(node, "task_id")
        tasks[task_id] = _literal_keyword(node, "bash_command")
    return tasks, source


def test_forecast_dag_is_the_four_task_explicit_pipeline() -> None:
    tasks, source = _dag_tasks(ROOT / "orchestration/dags/forecast_hourly_dag.py")

    assert list(tasks) == ["copy_raw", "autoload_staging", "process_dbt", "health"]
    assert "copy_raw >> autoload_staging >> process_dbt >> health" in source
    assert "fetch-open-meteo forecast --execute" in tasks["copy_raw"]
    assert "auto-loader forecast" in tasks["autoload_staging"]
    assert "auto-process run forecast" in tasks["process_dbt"]
    assert "pipeline-health --scope forecast --require-gold" in tasks["health"]


def test_archive_dag_is_the_four_task_pipeline_without_seed() -> None:
    tasks, source = _dag_tasks(ROOT / "orchestration/dags/archive_monthly_dag.py")

    assert list(tasks) == ["copy_raw", "autoload_staging", "process_dbt", "health"]
    assert "copy_raw >> autoload_staging >> process_dbt >> health" in source
    assert "fetch-open-meteo archive" in tasks["copy_raw"]
    assert "--start $MONTH --end $MONTH" in tasks["copy_raw"]
    assert "auto-loader archive" in tasks["autoload_staging"]
    assert "auto-process run archive" in tasks["process_dbt"]
    assert "pipeline-health --scope archive --require-gold" in tasks["health"]
    assert "dbt seed" not in source.lower()


def test_maintenance_dag_has_one_task_for_retention_and_cleanup() -> None:
    tasks, source = _dag_tasks(
        ROOT / "orchestration/dags/lakehouse_maintenance_dag.py"
    )

    assert list(tasks) == ["maintain_lakehouse"]
    assert "maintain-lakehouse" in tasks["maintain_lakehouse"]
    assert "snapshot-retention-days" in tasks["maintain_lakehouse"]
    assert "file-grace-days" in tasks["maintain_lakehouse"]
    assert "dbt" not in source.lower()


def _source_table(name: str) -> dict[str, Any]:
    sources = yaml.safe_load(
        (TRANSFORM / "models/sources.yml").read_text(encoding="utf-8")
    )
    return next(
        table
        for source in sources["sources"]
        for table in source["tables"]
        if table["name"] == name
    )


def _column(table: dict[str, Any], name: str) -> dict[str, Any]:
    return next(column for column in table["columns"] if column["name"] == name)


def test_dbt_quality_covers_forecast_source_contract() -> None:
    forecast = _source_table("stg_weather_forecast")
    columns = {column["name"]: column for column in forecast["columns"]}

    assert "not_empty" in forecast.get("tests", [])
    assert all("not_null" in columns[name]["tests"] for name in (
        "grid_latitude",
        "grid_longitude",
        "valid_time_utc",
    ))
    assert any("accepted_range" in test for test in columns["precipitation_mm"]["tests"])
    assert forecast["freshness"]["error_after"] == {"count": 24, "period": "hour"}


def test_dbt_quality_covers_archive_source_contract() -> None:
    archive = _source_table("stg_weather_archive_hourly")
    columns = {column["name"]: column for column in archive["columns"]}

    assert "not_empty" in archive.get("tests", [])
    assert all("not_null" in columns[name]["tests"] for name in (
        "grid_latitude",
        "grid_longitude",
        "valid_time_utc",
        "weather_model",
    ))
    assert any("accepted_range" in test for test in columns["precipitation_mm"]["tests"])
    accepted = next(
        test["accepted_values"]
        for test in columns["weather_model"]["tests"]
        if "accepted_values" in test
    )
    assert set(accepted["arguments"]["values"]) == {"era5", "ecmwf_ifs"}


def test_dbt_quality_macros_are_project_owned() -> None:
    macro_path = TRANSFORM / "macros/data_quality.sql"
    assert macro_path.exists()
    macro = macro_path.read_text(encoding="utf-8")
    assert "{% test not_empty" in macro
    assert "{% test accepted_range" in macro


class _Result:
    def __init__(self, row: tuple[Any, ...]) -> None:
        self.row = row

    def fetchone(self) -> tuple[Any, ...]:
        return self.row


class _HealthConnection:
    def execute(self, query: str, params: Any = None) -> _Result:
        del params
        if "WHERE false" in query:
            return _Result((1,))
        if "COUNT(*)" in query:
            return _Result((1, 0, 0, 0, datetime.now(UTC)))
        return _Result((datetime.now(UTC),))


def test_health_delegates_row_level_quality_to_dbt() -> None:
    from vn_climate_risk_monitor import health

    checks = health._check_weather_table(
        _HealthConnection(),
        "catalog1.silver.stg_weather_forecast",
        required=True,
        freshness_hours=24,
    )

    assert [check.name for check in checks] == ["stg_weather_forecast.freshness"]


def test_health_has_no_row_level_duplicate_or_contract_checks() -> None:
    from vn_climate_risk_monitor import health

    assert not hasattr(health, "_check_archive_duplicates")
    assert not hasattr(health, "_check_mapping")
    assert not hasattr(health, "_check_forecast_gold")


def test_approved_operational_console_entry_points_are_package_owned() -> None:
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    scripts = project["project"]["scripts"]

    assert scripts["pipeline-health"] == "vn_climate_risk_monitor.health:main"
    assert scripts["auto-loader"] == "vn_climate_risk_monitor.load:main"
    assert scripts["auto-process"] == "vn_climate_risk_monitor.processing.cli:main"
    assert scripts["maintain-lakehouse"] == "vn_climate_risk_monitor.maintenance:main"
    assert scripts["bootstrap-lakehouse"] == "vn_climate_risk_monitor.bootstrap:main"
    assert scripts["reset-lakehouse"] == "vn_climate_risk_monitor.reset:main"
    assert not (ROOT / "scripts/healthcheck.py").exists()
    assert not (ROOT / "scripts/maintain_lake.py").exists()
    assert not (ROOT / "scripts/bootstrap.py").exists()
    assert not (ROOT / "scripts/reset_lakehouse.py").exists()


def test_reset_scope_is_confirmation_protected_and_never_bronze() -> None:
    module_path = ROOT / "src/vn_climate_risk_monitor/reset.py"
    spec = importlib.util.spec_from_file_location("runtime_reset", module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    objects = [
        ("catalog1", "silver", "stg_weather_forecast", "BASE TABLE"),
        ("catalog1", "gold", "fct_rain_forecast_hourly", "BASE TABLE"),
        ("catalog1", "bronze", "raw_payload", "BASE TABLE"),
        ("catalog1", "seed", "ward", "BASE TABLE"),
    ]
    class FakeDuck:
        def __init__(self) -> None:
            self.statements: list[str] = []

        def execute(self, statement: str) -> _Result:
            self.statements.append(statement)
            return _Result(objects)

    duck = FakeDuck()
    assert module.drop_objects(duck, objects) == 2
    assert all('"silver"' in statement or '"gold"' in statement for statement in duck.statements)


def test_compose_has_lean_default_services_and_tools_profile() -> None:
    compose_text = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    compose = yaml.safe_load(compose_text)

    assert set(compose["services"]) == {
        "postgres",
        "minio",
        "bootstrap",
        "airflow",
        "streamlit",
        "pgadmin",
    }
    assert compose["services"]["pgadmin"]["profiles"] == ["tools"]
    assert "secret-init" not in compose_text
    assert "x-project-environment:" in compose_text
    assert "<<: *project-environment" in compose_text
    assert "minioadmin" not in compose_text
    assert "${POSTGRES_PASSWORD:-" not in compose_text
    assert "${MINIO_SECRET_KEY:-" not in compose_text


def test_bootstrap_partial_dbt_build_only_selects_tests_with_complete_parents() -> None:
    compose = yaml.safe_load(
        (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    )
    command = compose["services"]["bootstrap"]["command"][-1]
    build_line = next(
        line.strip() for line in command.splitlines() if " dbt build " in line
    )
    tokens = shlex.split(build_line)

    option = tokens.index("--indirect-selection")
    assert tokens[option + 1] == "cautious"


def test_compose_credentials_are_explicit_placeholders_and_images_are_pinned() -> None:
    env = (ROOT / ".env.example").read_text(encoding="utf-8")
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")

    for name in (
        "POSTGRES_PASSWORD",
        "MINIO_ACCESS_KEY",
        "MINIO_SECRET_KEY",
        "AIRFLOW_SECRET_KEY",
        "AIRFLOW_FERNET_KEY",
    ):
        assert f"{name}=<" in env
        assert f"${{{name}:?" in compose

    parsed = yaml.safe_load(compose)
    for service in ("postgres", "minio", "pgadmin"):
        assert "@sha256:" in parsed["services"][service]["image"]


def test_ci_supplies_compose_credentials_and_builds_real_service_names() -> None:
    workflow = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")

    assert "MINIO_ACCESS_KEY:" in workflow
    assert "AIRFLOW_FERNET_KEY:" in workflow
    assert "docker compose build airflow streamlit" in workflow
    assert "docker compose build airflow dashboard" not in workflow


def test_dbt_profile_has_no_default_minio_credential() -> None:
    profile = (TRANSFORM / "profiles.yml").read_text(encoding="utf-8")

    assert "env_var('MINIO_ACCESS_KEY')" in profile
    assert "minioadmin" not in profile
