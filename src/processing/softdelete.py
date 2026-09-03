"""Soft delete bằng anti-join với nguồn — module độc lập, dùng lại được.

Vấn đề nó giải: bảng curated giữ bản ghi hiện hành, nhưng nguồn có thể XOÁ một
thực thể (phường giải thể, khách hàng bị gỡ). Xoá cứng ở đích thì mọi fact lịch
sử trỏ vào nó thành orphan; giữ nguyên thì báo cáo đếm cả thứ không còn tồn tại.
Soft delete tắt cờ và giữ dòng, nên lịch sử vẫn join được còn báo cáo hiện tại
lọc bằng ``WHERE is_active``.

Chỉ phụ thuộc một connection có ``.execute()`` — không biết gì về DuckDB, dbt hay
dự án nào. Nguồn key khai báo bằng SQL, nên nó chạy với seed, bảng khác, hay một
Postgres/MySQL đã ATTACH đều được.

    config = SoftDeleteConfig(
        target="gold.dim_ward",
        business_key=("ward_code",),
        key_source_sql="SELECT commune_code FROM seed.ward WHERE province_code='01'",
    )
    result = apply_soft_delete(connection, config, now=run_started_at)
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol


class SqlConnection(Protocol):
    """Ranh giới tối thiểu — đủ để test bằng DuckDB in-memory thật."""

    def execute(self, query: str, parameters: object = ...) -> Any: ...


class SoftDeleteError(RuntimeError):
    """Nguồn key không đáng tin, hoặc thay đổi lớn bất thường."""


@dataclass(frozen=True)
class SoftDeleteConfig:
    target: str
    business_key: tuple[str, ...]
    key_source_sql: str
    active_column: str = "is_active"
    deactivated_at_column: str = "_deactivated_at"
    # Bao nhiêu phần trăm dòng đang active được phép tắt trong MỘT lần chạy.
    # 126 phường mất 1–2 là nghị quyết; mất 60 là nguồn sai.
    max_deactivation_ratio: float = 0.1

    def __post_init__(self) -> None:
        if not self.target.strip():
            raise ValueError("target must not be empty")
        if not self.business_key:
            raise ValueError(f"{self.target}: cần ít nhất một cột business_key")
        if not self.key_source_sql.strip():
            raise ValueError(f"{self.target}: key_source_sql must not be empty")
        if not 0 <= self.max_deactivation_ratio <= 1:
            raise ValueError(f"{self.target}: max_deactivation_ratio phải trong [0, 1]")


@dataclass(frozen=True)
class SoftDeleteResult:
    target: str
    source_keys: int
    active_before: int
    deactivated: int
    reactivated: int


def _scalar(connection: SqlConnection, query: str) -> int:
    row = connection.execute(query).fetchone()
    return 0 if row is None or row[0] is None else int(row[0])


def apply_soft_delete(
    connection: SqlConnection,
    config: SoftDeleteConfig,
    *,
    now: datetime,
) -> SoftDeleteResult:
    """Đồng bộ cờ active của ``target`` với tập key hiện có ở nguồn.

    Hai chiều, cả hai idempotent — chạy lại lần hai không đổi gì thêm:

    * có ở target, KHÔNG có ở nguồn  -> ``is_active = FALSE``
    * đang tắt, nguồn CÓ trở lại      -> ``is_active = TRUE``

    Chiều thứ hai không có trong pattern gốc. Thiếu nó thì một lần nguồn lỗi sẽ
    tắt vĩnh viễn các dòng đúng, và không có đường quay lại ngoài sửa tay.
    """
    keys = ", ".join(config.business_key)
    join = " AND ".join(f"t.{key} = s.{key}" for key in config.business_key)
    null_check = " OR ".join(f"{key} IS NULL" for key in config.business_key)

    connection.execute("DROP TABLE IF EXISTS _soft_delete_keys")
    connection.execute(
        f"CREATE TEMP TABLE _soft_delete_keys AS "
        f"SELECT DISTINCT {keys} FROM ({config.key_source_sql}) AS source"
    )
    try:
        source_keys = _scalar(connection, "SELECT COUNT(*) FROM _soft_delete_keys")

        # Guard 1. Nguồn rỗng là dấu hiệu nguồn HỎNG, không phải "mọi thứ đã bị
        # xoá". Không chặn thì một lần mất kết nối sẽ tắt sạch bảng, im lặng.
        if source_keys == 0:
            raise SoftDeleteError(
                f"{config.target}: nguồn key trả 0 dòng — từ chối tắt toàn bộ bảng"
            )
        # Key NULL làm anti-join sai lệch âm thầm (NOT IN với NULL trả rỗng).
        orphan_null = _scalar(
            connection,
            f"SELECT COUNT(*) FROM _soft_delete_keys WHERE {null_check}",
        )
        if orphan_null:
            raise SoftDeleteError(
                f"{config.target}: nguồn key có {orphan_null} dòng NULL ở business key"
            )

        active_before = _scalar(
            connection,
            f"SELECT COUNT(*) FROM {config.target} WHERE {config.active_column}",
        )
        candidates = _scalar(
            connection,
            f"""
            SELECT COUNT(*) FROM {config.target} AS t
            WHERE t.{config.active_column}
              AND NOT EXISTS (
                  SELECT 1 FROM _soft_delete_keys AS s WHERE {join}
              )
            """,
        )

        # Guard 2. Thay đổi lớn bất thường thì dừng và để người xem, thay vì ghi
        # rồi mới phát hiện. Bảng rỗng thì không có tỉ lệ để so.
        if active_before and candidates / active_before > config.max_deactivation_ratio:
            raise SoftDeleteError(
                f"{config.target}: sẽ tắt {candidates}/{active_before} dòng "
                f"({candidates / active_before:.1%}) > ngưỡng "
                f"{config.max_deactivation_ratio:.0%}. Kiểm tra nguồn trước khi chạy lại."
            )

        connection.execute(
            f"""
            UPDATE {config.target} AS t
            SET {config.active_column} = FALSE,
                {config.deactivated_at_column} = ?
            WHERE t.{config.active_column}
              AND NOT EXISTS (SELECT 1 FROM _soft_delete_keys AS s WHERE {join})
            """,
            [now],
        )
        reactivated = _scalar(
            connection,
            f"""
            SELECT COUNT(*) FROM {config.target} AS t
            WHERE NOT t.{config.active_column}
              AND EXISTS (SELECT 1 FROM _soft_delete_keys AS s WHERE {join})
            """,
        )
        connection.execute(
            f"""
            UPDATE {config.target} AS t
            SET {config.active_column} = TRUE,
                {config.deactivated_at_column} = NULL
            WHERE NOT t.{config.active_column}
              AND EXISTS (SELECT 1 FROM _soft_delete_keys AS s WHERE {join})
            """
        )
    finally:
        connection.execute("DROP TABLE IF EXISTS _soft_delete_keys")

    return SoftDeleteResult(
        target=config.target,
        source_keys=source_keys,
        active_before=active_before,
        deactivated=candidates,
        reactivated=reactivated,
    )
