"""Soft-delete adapter, test trên DuckDB in-memory THẬT.

Cố ý không mock connection: thứ dễ sai ở đây là chính câu SQL anti-join (NULL,
NOT EXISTS, idempotency), và fake sẽ khẳng định lại đúng cái giả định sai.
"""

from __future__ import annotations

from datetime import UTC, datetime

import duckdb
import pytest

from processing.softdelete import (
    SoftDeleteConfig,
    SoftDeleteError,
    apply_soft_delete,
)

NOW = datetime(2026, 9, 3, 12, 0, tzinfo=UTC)
WARDS = ["00004", "00008", "00010", "00013", "00016"]


def build_target(connection: duckdb.DuckDBPyConnection, codes: list[str]) -> None:
    connection.execute(
        """
        CREATE TABLE dim_ward (
            ward_code VARCHAR,
            ward_name VARCHAR,
            is_active BOOLEAN,
            _deactivated_at TIMESTAMPTZ
        )
        """
    )
    for code in codes:
        connection.execute(
            "INSERT INTO dim_ward VALUES (?, ?, TRUE, NULL)", [code, f"Phường {code}"]
        )


def config(source_codes: list[str] | None = None, **overrides: object) -> SoftDeleteConfig:
    codes = WARDS if source_codes is None else source_codes
    values = ", ".join(f"('{code}')" for code in codes) or "(NULL)"
    source_sql = (
        f"SELECT * FROM (VALUES {values}) AS t(ward_code)"
        if codes
        else "SELECT NULL AS ward_code WHERE FALSE"
    )
    # Ngưỡng mặc định 10% tính trên 126 phường thật; fixture chỉ có 5 dòng nên bỏ
    # một cái đã là 20%. Nới lên 50% cho các test ĐƯỜNG CHÍNH; guard có test riêng.
    overrides.setdefault("max_deactivation_ratio", 0.5)
    return SoftDeleteConfig(
        target="dim_ward",
        business_key=("ward_code",),
        key_source_sql=str(overrides.pop("key_source_sql", source_sql)),
        **overrides,  # type: ignore[arg-type]
    )


@pytest.fixture
def connection() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    build_target(con, WARDS)
    return con


# ── đường chính ───────────────────────────────────────────────────────────────
def test_missing_key_is_deactivated_not_deleted(connection) -> None:
    """Dòng phải CÒN LẠI, chỉ tắt cờ — nếu không fact lịch sử thành orphan."""
    result = apply_soft_delete(connection, config(WARDS[:-1]), now=NOW)

    assert result.deactivated == 1
    rows = connection.execute(
        "SELECT ward_code, is_active, _deactivated_at FROM dim_ward ORDER BY ward_code"
    ).fetchall()
    assert len(rows) == 5, "không được xoá cứng dòng nào"
    gone = [row for row in rows if not row[1]]
    assert [row[0] for row in gone] == ["00016"]
    assert gone[0][2] == NOW


def test_running_twice_changes_nothing_more(connection) -> None:
    apply_soft_delete(connection, config(WARDS[:-1]), now=NOW)

    again = apply_soft_delete(connection, config(WARDS[:-1]), now=NOW)

    assert again.deactivated == 0
    assert again.reactivated == 0


def test_key_returning_to_source_is_reactivated(connection) -> None:
    """Không có chiều này thì một lần nguồn lỗi tắt vĩnh viễn dòng đúng."""
    apply_soft_delete(connection, config(WARDS[:-1]), now=NOW)

    result = apply_soft_delete(connection, config(WARDS), now=NOW)

    assert result.reactivated == 1
    active, deactivated_at = connection.execute(
        "SELECT is_active, _deactivated_at FROM dim_ward WHERE ward_code = '00016'"
    ).fetchone()
    assert active is True
    assert deactivated_at is None


def test_nothing_missing_is_a_no_op(connection) -> None:
    result = apply_soft_delete(connection, config(WARDS), now=NOW)

    assert (result.deactivated, result.reactivated) == (0, 0)
    assert result.source_keys == 5
    assert result.active_before == 5


def test_extra_key_in_source_is_ignored(connection) -> None:
    """Nguồn có key mà đích chưa có là việc của bước MERGE, không phải của adapter."""
    result = apply_soft_delete(connection, config([*WARDS, "99999"]), now=NOW)

    assert result.deactivated == 0
    assert connection.execute("SELECT COUNT(*) FROM dim_ward").fetchone()[0] == 5


# ── guard: đây là chỗ pattern gốc hỏng nặng nhất ──────────────────────────────
def test_empty_source_refuses_to_wipe_the_table(connection) -> None:
    """Nguồn rỗng là nguồn HỎNG, không phải 'mọi thứ đã bị xoá'."""
    empty = SoftDeleteConfig(
        target="dim_ward",
        business_key=("ward_code",),
        key_source_sql="SELECT NULL AS ward_code WHERE FALSE",
    )

    with pytest.raises(SoftDeleteError, match="0 dòng"):
        apply_soft_delete(connection, empty, now=NOW)

    assert connection.execute("SELECT COUNT(*) FROM dim_ward WHERE is_active").fetchone()[0] == 5


def test_null_key_in_source_is_rejected(connection) -> None:
    """NOT IN với NULL trả rỗng — sai lệch âm thầm nếu không chặn."""
    with_null = SoftDeleteConfig(
        target="dim_ward",
        business_key=("ward_code",),
        key_source_sql="SELECT * FROM (VALUES ('00004'), (NULL)) AS t(ward_code)",
    )

    with pytest.raises(SoftDeleteError, match="NULL"):
        apply_soft_delete(connection, with_null, now=NOW)


def test_mass_deactivation_is_refused(connection) -> None:
    """126 phường mất 1–2 là nghị quyết; mất 60 là nguồn sai."""
    with pytest.raises(SoftDeleteError, match="ngưỡng"):
        apply_soft_delete(connection, config(WARDS[:1]), now=NOW)

    assert connection.execute("SELECT COUNT(*) FROM dim_ward WHERE is_active").fetchone()[0] == 5


def test_threshold_is_configurable(connection) -> None:
    result = apply_soft_delete(
        connection, config(WARDS[:1], max_deactivation_ratio=1.0), now=NOW
    )

    assert result.deactivated == 4


def test_empty_target_skips_the_ratio_guard() -> None:
    """Lần chạy đầu bảng rỗng: 0/0 không phải tỉ lệ, không được chia."""
    con = duckdb.connect()
    build_target(con, [])

    result = apply_soft_delete(con, config(WARDS), now=NOW)

    assert (result.active_before, result.deactivated) == (0, 0)


# ── composite key ─────────────────────────────────────────────────────────────
def test_composite_business_key() -> None:
    con = duckdb.connect()
    con.execute(
        """
        CREATE TABLE bridge (
            ward_code VARCHAR, weather_model VARCHAR,
            is_active BOOLEAN, _deactivated_at TIMESTAMPTZ
        )
        """
    )
    for code in ("00004", "00008"):
        for model in ("era5", "ecmwf_ifs"):
            con.execute("INSERT INTO bridge VALUES (?, ?, TRUE, NULL)", [code, model])

    result = apply_soft_delete(
        con,
        SoftDeleteConfig(
            target="bridge",
            business_key=("ward_code", "weather_model"),
            key_source_sql=(
                "SELECT * FROM (VALUES ('00004','era5'), ('00004','ecmwf_ifs'), "
                "('00008','era5')) AS t(ward_code, weather_model)"
            ),
            max_deactivation_ratio=0.5,
        ),
        now=NOW,
    )

    assert result.deactivated == 1
    gone = con.execute(
        "SELECT ward_code, weather_model FROM bridge WHERE NOT is_active"
    ).fetchall()
    assert gone == [("00008", "ecmwf_ifs")]


# ── validation config ─────────────────────────────────────────────────────────
@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"business_key": ()}, "business_key"),
        ({"key_source_sql": "  "}, "key_source_sql"),
        ({"max_deactivation_ratio": 1.5}, "max_deactivation_ratio"),
    ],
)
def test_invalid_config_is_rejected(kwargs: dict, match: str) -> None:
    base = {
        "target": "t",
        "business_key": ("k",),
        "key_source_sql": "SELECT 1 AS k",
    }
    with pytest.raises(ValueError, match=match):
        SoftDeleteConfig(**{**base, **kwargs})
