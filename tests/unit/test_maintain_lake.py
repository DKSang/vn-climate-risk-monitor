"""Retention SQL phải giữ snapshot và grace period thay vì cleanup_all."""

from __future__ import annotations

import pytest

from vn_climate_risk_monitor.maintenance import build_statements


def test_maintenance_keeps_snapshot_and_file_grace_periods() -> None:
    expire, cleanup = build_statements(
        "catalog1",
        snapshot_retention_days=7,
        file_grace_days=2,
        dry_run=False,
    )

    assert "INTERVAL 7 DAY" in expire
    assert "INTERVAL 2 DAY" in cleanup
    assert "cleanup_all" not in cleanup
    assert "dry_run => false" in expire


def test_dry_run_is_forwarded_to_both_operations() -> None:
    statements = build_statements(
        "catalog1",
        snapshot_retention_days=7,
        file_grace_days=2,
        dry_run=True,
    )

    assert all("dry_run => true" in statement for statement in statements)


@pytest.mark.parametrize(("snapshots", "files"), [(0, 2), (7, 0), (-1, 2)])
def test_retention_must_be_positive(snapshots: int, files: int) -> None:
    with pytest.raises(ValueError):
        build_statements(
            "catalog1",
            snapshot_retention_days=snapshots,
            file_grace_days=files,
            dry_run=False,
        )
