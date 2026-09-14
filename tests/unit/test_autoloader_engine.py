"""Test engine autoloader bằng fake — không cần Postgres/MinIO/DuckDB thật.

Trọng tâm là ĐƯỜNG LỖI, vì đó là chỗ từng có defect mất dữ liệu: một file JSON
hỏng kéo theo mọi file lành cùng lô (đo 2026-08-21: batch_size=10, 2 lành + 1
hỏng -> 0 dòng vào bảng, cả 3 kẹt FAILED).
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import duckdb
import pytest

from vn_climate_risk_monitor.ingestion.loader import AutoLoader, SourceConfig
from vn_climate_risk_monitor.ingestion.state import PostgresIngestionRepository


@dataclass
class FakeObject:
    object_name: str
    size: int = 100
    etag: str = "etag"
    last_modified: datetime | None = None


class FakeObjectClient:
    def __init__(self, keys: list[str]) -> None:
        self._keys = keys

    def list_objects(self, bucket_name: str, prefix: str, recursive: bool):
        return [FakeObject(k) for k in self._keys if k.startswith(prefix)]


@dataclass
class FakeClaim:
    file_id: UUID
    object_key: str


@dataclass
class FakeAttempt:
    attempt_id: UUID


class FakeCheckpoint:
    """Bản ghi nhớ trong RAM, đủ hình dạng cho engine."""

    def __init__(self, now: datetime | None = None) -> None:
        self.files: dict[str, dict[str, Any]] = {}
        self.committed: list[UUID] = []
        self.failed: list[UUID] = []
        self.source_attempts: list[FakeAttempt] = []
        self.completed_source_runs: list[UUID] = []
        self.failed_source_runs: list[UUID] = []
        self.now = now or datetime(2026, 9, 3, 10, 0, tzinfo=UTC)
        self.clock_calls = 0

    def control_now(self) -> datetime:
        self.clock_calls += 1
        return self.now

    def known_object_keys(self, **_: Any) -> set[str]:
        return set(self.files)

    def begin_source_run(self, **_: Any) -> FakeAttempt:
        attempt = FakeAttempt(attempt_id=uuid4())
        self.source_attempts.append(attempt)
        return attempt

    def complete_source_run(self, attempt_id: UUID, **_: Any) -> None:
        self.completed_source_runs.append(attempt_id)

    def fail_source_run(self, attempt_id: UUID, **_: Any) -> None:
        self.failed_source_runs.append(attempt_id)

    def register_file(
        self, *, object_key: str, attempt_id: UUID | None = None, **_: Any
    ) -> UUID:
        file_id = uuid4()
        self.files[object_key] = {
            "file_id": file_id,
            "attempt_id": attempt_id or self.source_attempts[-1].attempt_id,
            "status": "PENDING",
            "retry_count": 0,
        }
        return file_id

    def claim_files(self, *, limit: int, max_retries: int, **_: Any):
        claimable = [
            (key, meta)
            for key, meta in self.files.items()
            if meta["status"] == "PENDING"
            or (meta["status"] == "FAILED" and meta["retry_count"] < max_retries)
        ]
        chosen = claimable[:limit]
        for _key, meta in chosen:
            if meta["status"] == "FAILED":
                # Match PostgresIngestionRepository: retry_count records a
                # reclaimed FAILED attempt, not the failure transition.
                meta["retry_count"] += 1
            meta["status"] = "PROCESSING"
        return tuple(FakeClaim(meta["file_id"], key) for key, meta in chosen)

    def _meta(self, file_id: UUID) -> dict[str, Any]:
        return next(m for m in self.files.values() if m["file_id"] == file_id)

    def commit_file(self, file_id: UUID, **_: Any) -> None:
        self._meta(file_id)["status"] = "COMMITTED"
        self.committed.append(file_id)

    def fail_file(self, file_id: UUID, **_: Any) -> None:
        meta = self._meta(file_id)
        meta["status"] = "FAILED"
        self.failed.append(file_id)


class BrokenRegistrationCheckpoint(FakeCheckpoint):
    def register_file(self, **_: Any) -> UUID:
        raise RuntimeError("registration failed")


class FakeResult:
    def __init__(self, rows: int) -> None:
        self._rows = rows

    def fetchone(self) -> tuple[int]:
        return (self._rows,)


class FakeSql:
    """DuckDB giả, có catalog: file nào có `poison` trong tên thì ném lỗi.

    Truy vấn vào bảng chưa tồn tại ném lỗi giống Catalog Error thật, nếu không thì
    đường tạo bảng đích của engine sẽ không bao giờ được test chạy tới.
    """

    def __init__(
        self, rows_per_file: int = 100, existing_tables: set[str] | None = None
    ) -> None:
        self.rows_per_file = rows_per_file
        self.statements: list[str] = []
        self.tables: set[str] = set(existing_tables or ())

    def execute(self, query: str, parameters: object = None) -> FakeResult:
        self.statements.append(query)
        # Trước cả CREATE: DuckDB phải suy schema từ file nguồn nên file hỏng cũng
        # làm gãy câu CREATE ... AS SELECT, không riêng INSERT.
        if "poison" in query:
            raise ValueError("JSON transform error: unknown key")
        if query.startswith("SELECT 1 FROM "):
            table = query.split()[3]
            if table not in self.tables:
                raise ValueError(f"Catalog Error: Table {table} does not exist")
            return FakeResult(0)
        if query.startswith("CREATE TABLE IF NOT EXISTS "):
            self.tables.add(query.split()[5])
            return FakeResult(0)
        return FakeResult(query.count("s3://") * self.rows_per_file)


class RecordingSql:
    """Real DuckDB with a statement log for cross-system ordering assertions."""

    def __init__(self, events: list[str] | None = None) -> None:
        self.connection = duckdb.connect()
        self.events = events if events is not None else []

    def execute(self, query: str, parameters: object = None):
        self.events.append(query.strip())
        if parameters is None:
            return self.connection.execute(query)
        return self.connection.execute(query, parameters)


class CrashGapCheckpoint(FakeCheckpoint):
    """Crash once after DuckLake commit, then expose lease expiry for retry."""

    def __init__(self, events: list[str]) -> None:
        super().__init__()
        self.events = events
        self.crash_before_postgres_commit = True

    def commit_file(self, file_id: UUID, **kwargs: Any) -> None:
        if self.crash_before_postgres_commit:
            self.crash_before_postgres_commit = False
            self.events.append("POSTGRES COMMIT ATTEMPT")
            raise RuntimeError("simulated crash after DuckLake commit")
        self.events.append("POSTGRES COMMIT")
        super().commit_file(file_id, **kwargs)

    def expire_processing_lease(self) -> None:
        processing = [m for m in self.files.values() if m["status"] == "PROCESSING"]
        assert len(processing) == 1
        processing[0]["status"] = "FAILED"
        processing[0]["error_type"] = "LeaseExpired"


def build_config(tmp_path: Path, *, batch_size: int, max_retries: int = 3):
    sql_file = tmp_path / "t.sql"
    sql_file.write_text("SELECT * FROM read_json_auto({{ files }})", encoding="utf-8")
    return SourceConfig(
        name="src",
        dataset="ds",
        prefix="raw",
        pattern="**/*.json",
        sql_file="t.sql",
        target="db.schema.tbl",
        batch_size=batch_size,
        parameters={},
        scope="test",
        max_retries=max_retries,
        base_dir=tmp_path,
    )


def build_idempotency_config(tmp_path: Path):
    sql_file = tmp_path / "idempotency.sql"
    sql_file.write_text(
        "SELECT source_file AS _source_file, md5(source_file) AS row_hash "
        "FROM UNNEST({{ files }}) AS source_files(source_file)",
        encoding="utf-8",
    )
    return SourceConfig(
        name="src",
        dataset="ds",
        prefix="raw",
        pattern="**/*.json",
        sql_file=sql_file.name,
        target="main.staging",
        batch_size=10,
        parameters={},
        scope="test",
        max_retries=3,
        base_dir=tmp_path,
    )


def build_loader(
    tmp_path: Path,
    keys: list[str],
    *,
    batch_size: int,
    existing_tables: set[str] | None = None,
) -> AutoLoader:
    return AutoLoader(
        config=build_config(tmp_path, batch_size=batch_size),
        checkpoint=FakeCheckpoint(),
        object_client=FakeObjectClient(keys),
        sql=FakeSql(existing_tables=existing_tables),
        bucket="bkt",
        worker_id="w1",
    )


def test_loads_every_discovered_file(tmp_path: Path) -> None:
    loader = build_loader(tmp_path, ["raw/a.json", "raw/b.json"], batch_size=10)
    result = loader.load()

    assert result.discovered == 2
    assert result.newly_registered == 2
    assert result.committed_files == 2
    assert result.failures == ()


def test_second_run_loads_nothing_exactly_once(tmp_path: Path) -> None:
    loader = build_loader(tmp_path, ["raw/a.json"], batch_size=10)
    loader.load()

    second = loader.load()

    assert second.newly_registered == 0
    assert second.committed_files == 0
    assert second.rows_inserted == 0


def test_failed_registration_closes_the_discovery_run(tmp_path: Path) -> None:
    loader = AutoLoader(
        config=build_config(tmp_path, batch_size=1),
        checkpoint=BrokenRegistrationCheckpoint(),
        object_client=FakeObjectClient(["raw/a.json"]),
        sql=FakeSql(),
        bucket="bkt",
    )

    with pytest.raises(RuntimeError, match="registration failed"):
        loader.load()

    assert loader.checkpoint.failed_source_runs == [
        loader.checkpoint.source_attempts[0].attempt_id
    ]


def test_reloading_the_same_object_replaces_rows_without_duplicates(
    tmp_path: Path,
) -> None:
    checkpoint = FakeCheckpoint()
    sql = RecordingSql()
    loader = AutoLoader(
        config=build_idempotency_config(tmp_path),
        checkpoint=checkpoint,
        object_client=FakeObjectClient(["raw/a.json"]),
        sql=sql,
        bucket="bkt",
        worker_id="w1",
    )

    loader.load()
    first_rows = sql.connection.execute(
        "SELECT _source_file, row_hash FROM main.staging ORDER BY 1"
    ).fetchall()
    checkpoint.files["raw/a.json"]["status"] = "FAILED"
    loader.load()
    second_rows = sql.connection.execute(
        "SELECT _source_file, row_hash FROM main.staging ORDER BY 1"
    ).fetchall()

    assert first_rows == [("s3://bkt/raw/a.json", "a941395053fcf7bb5d8fd69fad1db1ab")]
    assert second_rows == first_rows


def test_commit_gap_retry_replaces_rows_after_expired_lease(tmp_path: Path) -> None:
    events: list[str] = []
    checkpoint = CrashGapCheckpoint(events)
    sql = RecordingSql(events)
    loader = AutoLoader(
        config=build_idempotency_config(tmp_path),
        checkpoint=checkpoint,
        object_client=FakeObjectClient(["raw/a.json"]),
        sql=sql,
        bucket="bkt",
        worker_id="w1",
    )

    with pytest.raises(RuntimeError, match="simulated crash"):
        loader.load()
    first_rows = sql.connection.execute(
        "SELECT _source_file, row_hash FROM main.staging ORDER BY 1"
    ).fetchall()
    assert checkpoint.files["raw/a.json"]["status"] == "PROCESSING"
    first_postgres_attempt = events.index("POSTGRES COMMIT ATTEMPT")
    assert events[first_postgres_attempt - 1] == "COMMIT"

    checkpoint.expire_processing_lease()
    loader.load()
    second_rows = sql.connection.execute(
        "SELECT _source_file, row_hash FROM main.staging ORDER BY 1"
    ).fetchall()

    assert checkpoint.files["raw/a.json"]["status"] == "COMMITTED"
    assert checkpoint.files["raw/a.json"]["retry_count"] == 1
    assert checkpoint.files["raw/a.json"]["error_type"] == "LeaseExpired"
    assert second_rows == first_rows
    postgres_commit = len(events) - 1 - events[::-1].index("POSTGRES COMMIT")
    assert events[postgres_commit - 1] == "COMMIT"
    assert sum(statement.startswith("DELETE FROM main.staging") for statement in events) == 2


def test_later_discovery_creates_a_real_discovery_run(tmp_path: Path) -> None:
    loader = build_loader(tmp_path, ["raw/a.json"], batch_size=10)
    loader.load(now=datetime(2026, 1, 1, tzinfo=UTC))
    loader.object_client._keys.append("raw/b.json")

    later = loader.load(now=datetime(2026, 6, 1, tzinfo=UTC))

    assert later.newly_registered == 1
    assert later.committed_files == 1
    attempt_ids = {meta["attempt_id"] for meta in loader.checkpoint.files.values()}
    assert len(attempt_ids) == 2
    assert len(loader.checkpoint.completed_source_runs) == 2


@pytest.mark.parametrize(("batch_size", "expected_batches"), [(1, 6), (3, 4), (10, 4)])
def test_poison_file_does_not_block_healthy_files(
    tmp_path: Path, batch_size: int, expected_batches: int
) -> None:
    """Hồi quy: file hỏng chỉ được làm hỏng chính nó.

    Trước khi sửa, với batch_size > 1 thì cả lô bị đánh FAILED và các file lành
    không bao giờ vào được bảng đích.
    """
    loader = build_loader(
        tmp_path,
        ["raw/good_1.json", "raw/poison.json", "raw/good_2.json"],
        batch_size=batch_size,
    )

    result = loader.load()

    checkpoint = loader.checkpoint
    assert result.committed_files == 2
    assert result.committed_files == len(checkpoint.committed)
    assert result.rows_inserted == 200
    assert result.batches == expected_batches
    committed_keys = {
        key for key, meta in checkpoint.files.items() if meta["status"] == "COMMITTED"
    }
    assert committed_keys == {"raw/good_1.json", "raw/good_2.json"}
    assert checkpoint.files["raw/poison.json"]["status"] == "FAILED"
    # Chỉ đúng một FILE hỏng; nó xuất hiện nhiều lần trong failures vì bị retry.
    failed_files = {msg.split(":", 1)[0] for msg in result.failures}
    assert failed_files == {"raw/poison.json"}


def test_retry_is_bounded_by_max_retries_not_max_batches(tmp_path: Path) -> None:
    """Một file hỏng không được đốt hết max_batches.

    max_batches=100 nhưng max_retries=3, nên có một claim ban đầu và ba retry.
    """
    loader = build_loader(tmp_path, ["raw/poison.json"], batch_size=1)

    result = loader.load()

    assert loader.checkpoint.files["raw/poison.json"]["retry_count"] == 3
    assert result.committed_files == 0
    assert result.committed_files == len(loader.checkpoint.committed)
    assert result.rows_inserted == 0
    assert result.batches == 4, "mỗi lần claim file hỏng vẫn là một batch đã xử lý"
    assert len(result.failures) == 4


def test_retry_stops_at_max_retries(tmp_path: Path) -> None:
    loader = build_loader(tmp_path, ["raw/poison.json"], batch_size=1)
    loader.config = replace(loader.config, max_retries=2)

    for _ in range(5):
        loader.load()

    meta = loader.checkpoint.files["raw/poison.json"]
    assert meta["retry_count"] == 2, "không được retry quá max_retries"


def test_healthy_batch_runs_one_statement_not_per_file(tmp_path: Path) -> None:
    """Đường thành công phải giữ nguyên hiệu năng: 1 câu INSERT cho cả lô."""
    loader = build_loader(
        tmp_path, ["raw/a.json", "raw/b.json", "raw/c.json"], batch_size=10
    )

    loader.load()

    inserts = [s for s in loader.sql.statements if s.startswith("INSERT")]
    assert len(inserts) == 1
    assert inserts[0].count("s3://") == 3


def test_creates_target_table_on_first_load(tmp_path: Path) -> None:
    """Nguồn mới không cần DDL tay: bảng đích sinh từ chính SQL của nguồn."""
    loader = build_loader(tmp_path, ["raw/a.json"], batch_size=10)

    result = loader.load()

    creates = [
        s for s in loader.sql.statements if s.startswith("CREATE TABLE IF NOT EXISTS")
    ]
    assert len(creates) == 1
    assert creates[0].startswith("CREATE TABLE IF NOT EXISTS db.schema.tbl AS")
    assert "WHERE false" in creates[0], "CREATE không được nạp dòng nào"
    assert "db.schema.tbl" in loader.sql.tables
    assert result.committed_files == 1


def test_create_runs_before_insert(tmp_path: Path) -> None:
    loader = build_loader(tmp_path, ["raw/a.json"], batch_size=10)

    loader.load()

    kinds = [s.split()[0] for s in loader.sql.statements]
    assert kinds.index("CREATE") < kinds.index("INSERT")


def test_existing_target_table_is_never_recreated(tmp_path: Path) -> None:
    """Bảng chỉnh tay (partition, constraint) phải được giữ nguyên."""
    loader = build_loader(
        tmp_path, ["raw/a.json"], batch_size=10, existing_tables={"db.schema.tbl"}
    )

    loader.load()

    assert not [s for s in loader.sql.statements if s.startswith("CREATE")]


def test_target_table_is_created_once_across_batches(tmp_path: Path) -> None:
    """Probe + CREATE chỉ chạy một lần, không phải mỗi micro-batch."""
    loader = build_loader(
        tmp_path, ["raw/a.json", "raw/b.json", "raw/c.json"], batch_size=1
    )

    loader.load()
    loader.load()

    assert len([s for s in loader.sql.statements if s.startswith("CREATE")]) == 1
    assert len([s for s in loader.sql.statements if s.startswith("SELECT 1 FROM")]) == 1


def test_broken_first_file_still_lets_a_healthy_file_create_the_table(
    tmp_path: Path,
) -> None:
    """CREATE gãy vì file hỏng thì lô sau vẫn tạo được bảng — không kẹt vĩnh viễn."""
    loader = build_loader(
        tmp_path, ["raw/poison.json", "raw/z_good.json"], batch_size=10
    )

    loader.load()

    assert "db.schema.tbl" in loader.sql.tables
    assert loader.checkpoint.files["raw/z_good.json"]["status"] == "COMMITTED"
    assert loader.checkpoint.files["raw/poison.json"]["status"] == "FAILED"


def test_load_reports_discovered_count_even_when_nothing_new(tmp_path: Path) -> None:
    loader = build_loader(tmp_path, ["raw/a.json"], batch_size=10)
    loader.load()

    again = loader.load()

    assert again.discovered == 1
    assert again.newly_registered == 0


def test_run_timestamp_is_timezone_aware(tmp_path: Path) -> None:
    loader = build_loader(tmp_path, ["raw/a.json"], batch_size=10)
    moment = datetime.now(UTC)

    result = loader.load(now=moment)

    assert result.source == "src"


def test_ingested_at_comes_from_control_plane_not_duckdb(tmp_path: Path) -> None:
    """`_ingested_at` phải là giờ Postgres, không phải CURRENT_TIMESTAMP của DuckDB.

    Downstream so mốc này với checkpoint lấy từ Postgres. Nếu Bronze đóng dấu
    bằng giờ máy worker thì clock skew vài giây đủ để một cửa sổ incremental bỏ
    sót row — và lỗi đó không tái hiện được.
    """
    loader = build_loader(tmp_path, ["raw/a.json"], batch_size=10)
    (tmp_path / "t.sql").write_text(
        "SELECT {{ ingested_at }} AS _ingested_at FROM read_json_auto({{ files }})",
        encoding="utf-8",
    )

    loader.load()

    inserts = [s for s in loader.sql.statements if s.startswith("INSERT")]
    assert "2026-09-03 10:00:00.000000+00" in inserts[0]
    assert "{{ ingested_at }}" not in inserts[0]
    assert "CURRENT_TIMESTAMP" not in inserts[0]
    assert loader.checkpoint.clock_calls > 0


def test_ingested_at_literal_is_timestamptz(tmp_path: Path) -> None:
    """Literal trần bị DuckDB suy thành TIMESTAMP (không tz) — mất offset."""
    loader = build_loader(tmp_path, ["raw/a.json"], batch_size=10)

    assert loader._ingested_at_literal().startswith("TIMESTAMPTZ '")


def test_sources_do_not_use_duckdb_clock_for_ingested_at() -> None:
    """Bảo vệ 3 file SQL nguồn thật khỏi việc lặng lẽ quay lại CURRENT_TIMESTAMP."""
    for path in sorted(Path("sources").glob("*.sql")):
        body = path.read_text(encoding="utf-8")
        if "_ingested_at" not in body:
            continue
        # Bỏ comment: header của chính các file này GIẢI THÍCH vì sao không dùng
        # CURRENT_TIMESTAMP, nên khớp trên nguyên văn sẽ luôn false-positive.
        code = "\n".join(
            line for line in body.splitlines() if not line.lstrip().startswith("--")
        )
        assert "{{ ingested_at }}" in code, path
        assert "CURRENT_TIMESTAMP" not in code, path


def test_repository_has_a_real_discovery_run_lifecycle() -> None:
    assert hasattr(PostgresIngestionRepository, "begin_source_run")
    assert hasattr(PostgresIngestionRepository, "complete_source_run")
    assert hasattr(PostgresIngestionRepository, "fail_source_run")


def test_code_native_parameters_reach_the_sql(tmp_path: Path) -> None:
    """Nhiều nguồn cùng schema phải dùng CHUNG một file SQL.

    era5 và ecmwf_ifs từng là hai file SQL lệch nhau đúng một dòng — dạng trùng
    lặp chắc chắn sẽ trôi khỏi nhau khi thêm cột.
    """
    config = build_config(tmp_path, batch_size=10)
    config = replace(config, parameters={"weather_model": "era5"})
    (tmp_path / "t.sql").write_text(
        "SELECT '{{ weather_model }}' AS weather_model "
        "FROM read_json_auto({{ files }})",
        encoding="utf-8",
    )
    loader = AutoLoader(
        config=config,
        checkpoint=FakeCheckpoint(),
        object_client=FakeObjectClient(["raw/a.json"]),
        sql=FakeSql(),
        bucket="bkt",
        worker_id="w1",
    )

    loader.load()

    insert = next(s for s in loader.sql.statements if s.startswith("INSERT"))
    assert "'era5' AS weather_model" in insert
    assert "{{" not in insert


def test_parameter_cannot_shadow_an_engine_placeholder(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="trùng placeholder"):
        replace(
            build_config(tmp_path, batch_size=1),
            parameters={"ingested_at": "now()"},
        )


def test_parameter_with_a_quote_is_rejected(tmp_path: Path) -> None:
    """Giá trị chèn thẳng vào SQL, nên nháy đơn làm gãy câu lệnh ở runtime."""
    with pytest.raises(ValueError, match="nháy đơn"):
        replace(
            build_config(tmp_path, batch_size=1),
            parameters={"weather_model": "era5' OR '1"},
        )


def test_shipped_sources_render_without_leftover_placeholders() -> None:
    """Mọi source definition phải cấp đủ parameter cho SQL của nó.

    Placeholder thiếu chỉ nổ lúc chạy thật trên DuckDB, sau khi đã claim file.
    """
    import re

    from vn_climate_risk_monitor.load import source_configs

    for config in source_configs():
        rendered = config.sql
        for key, value in {
            "files": "[]",
            "ingested_at": "NOW()",
            **config.parameters,
        }.items():
            rendered = rendered.replace(f"{{{{ {key} }}}}", value)
        code = "\n".join(
            line for line in rendered.splitlines() if not line.lstrip().startswith("--")
        )
        assert not re.search(r"\{\{.*?\}\}", code), (
            f"{config.name}: còn placeholder chưa thay"
        )
