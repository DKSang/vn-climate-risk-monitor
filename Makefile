.PHONY: bootstrap bootstrap-env up down logs fetch-forecast fetch-archive backfill-archive load quality seed dbt dbt-test freshness transform dbt-docs clean-lake lint

# ==== Setup ====
bootstrap-env:
	cp -n .env.example .env || true

bootstrap: bootstrap-env
	uv run python scripts/bootstrap.py

# ==== Infrastructure (Docker Compose: MinIO + Postgres + pgAdmin) ====
up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

# ==== Ingest: activities (Lookup/ForEach/Copy) -> autoloader nạp bronze (SQL) ====
# Mặc định chỉ IN KẾ HOẠCH; thêm EXEC=1 để chạy thật.
EXEC ?=
_X = $(if $(EXEC),--execute,)

fetch-forecast:
	uv run fetch-open-meteo forecast $(_X)

START ?= 2000-01-01
END   ?= $(shell date +%Y-%m-01)
fetch-archive:
	uv run fetch-open-meteo archive --start $(START) --end $(END) $(_X)

# Backfill archive 2001→nay theo từng năm, resumable (fetch bỏ qua file đã có).
FROM ?= 2001
TO   ?= $(shell date +%Y)
backfill-archive:
	scripts/backfill_archive.sh $(FROM) $(TO)

# Phát hiện file mới trên MinIO và nạp vào bronze. Idempotent, exactly-once.
# Không tham số = chạy mọi nguồn trong ingest/load/*.yml
load:
	uv run load-sources $(SOURCE)

# ==== Data quality: Provero quét bronze NGAY SAU load ====
# dbt không với tới bronze vì autoloader ghi ngoài đồ thị dbt.
# Đọc QUA catalog DuckLake (không glob Parquet — glob thấy cả dòng đã xoá).
#
# --no-store: BẮT BUỘC. Provero v0.2.1 crash khi check `range` FAIL trên bảng có
#   cột timestamp: store/sqlite.py json.dumps(failing_rows_sample) không xử lý
#   được datetime. Tắt store thì né được; đánh đổi là mất `provero history`.
# --no-optimize: chạy từng check riêng thay vì gộp một query.
#
# Trả exit code 1 khi có check fail -> dùng làm cổng chặn trong CI được.
quality:
	DUCKLAKE_ALIAS=bronze_store \
	DUCKLAKE_DATA_PATH=s3://$(or $(MINIO_BUCKET),vn-climate)/bronze \
	DUCKLAKE_METADATA_SCHEMA=ducklake_bronze \
	uv run provero run -c quality/provero.yaml --no-optimize --no-store

# ==== Transform (dbt + DuckDB + DuckLake) ====
# dbt project ở transform/, không phải transform/dbt/
seed:
	cd transform && uv run dbt seed --profiles-dir .

dbt:
	cd transform && uv run dbt run --profiles-dir .

dbt-test:
	cd transform && uv run dbt test --profiles-dir .

# LƯU Ý: `dbt build` KHÔNG chạy source freshness — phải gọi riêng.
freshness:
	cd transform && uv run dbt source freshness --profiles-dir .

# build = run + test, và tự dọn file cũ qua on-run-end
transform:
	cd transform && uv run dbt build --profiles-dir .

dbt-docs:
	cd transform && uv run dbt docs generate --profiles-dir . && uv run dbt docs serve --profiles-dir .

# ==== Serve (CHƯA CÀI ĐẶT — Bước 8) ====
# serving/api và serving/dashboard hiện là thư mục rỗng.

# ==== Bảo trì lakehouse ====
# on-run-end trong dbt_project.yml đã tự dọn sau mỗi lần build, NHƯNG giữ lại
# 7 ngày snapshot để còn time-travel -> file của các build trong 7 ngày vẫn nằm đó.
# Target này squash sạch: bỏ toàn bộ lịch sử snapshot, chỉ giữ phiên bản hiện tại.
clean-lake:
	uv run python scripts/clean_lake.py

# ==== Quality ====
lint:
	uv run ruff check .
