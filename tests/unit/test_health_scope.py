from __future__ import annotations

from types import SimpleNamespace

import pytest

from vn_climate_risk_monitor import health


class _Result:
    def fetchone(self) -> tuple[int, int]:
        return (0, 0)


class _Connection:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def execute(
        self, query: str, parameters: tuple[object, ...] = ()
    ) -> _Result:
        self.calls.append((query, parameters))
        return _Result()

    def close(self) -> None:
        pass


@pytest.mark.parametrize(
    ("scope", "expected"),
    [
        ("forecast", ["open_meteo_forecast"]),
        ("archive", ["open_meteo_archive", "open_meteo_ifs"]),
    ],
)
def test_file_health_is_scoped_to_the_flow(
    monkeypatch: pytest.MonkeyPatch,
    scope: health.HealthScope,
    expected: list[str],
) -> None:
    connection = _Connection()
    settings = SimpleNamespace(
        postgres=SimpleNamespace(ducklake_connection_string="postgresql://control")
    )
    monkeypatch.setattr(health, "load_settings", lambda: settings)
    monkeypatch.setattr(health, "connect_control_plane", lambda _dsn: connection)

    checks = health._check_control_plane(scope=scope, require_gold=False)

    query, parameters = connection.calls[0]
    assert "run.pipeline_name = ANY(%s)" in query
    assert parameters == (expected,)
    assert all(check.status == "PASS" for check in checks)


def test_all_scope_checks_all_ingestion_files(monkeypatch: pytest.MonkeyPatch) -> None:
    connection = _Connection()
    settings = SimpleNamespace(
        postgres=SimpleNamespace(ducklake_connection_string="postgresql://control")
    )
    monkeypatch.setattr(health, "load_settings", lambda: settings)
    monkeypatch.setattr(health, "connect_control_plane", lambda _dsn: connection)

    health._check_control_plane(scope="all", require_gold=False)

    query, parameters = connection.calls[0]
    assert "run.pipeline_name = ANY(%s)" not in query
    assert parameters == ()
