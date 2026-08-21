.PHONY: bootstrap bootstrap-env up down logs ingest-provinces ingest-weather-plan ingest-weather-canary ingest-weather load-weather-canary load-weather run-weather-plan run-weather-canary run-weather plan-historical run-historical-canary run-historical-year run-historical-backfill run-historical-tail historical-status historical-tail-status weather-status weather-healthcheck migrate-legacy-dry-run migrate-legacy migrate-bronze-layout-dry-run migrate-bronze-layout migrate-ingestion-control-dry-run migrate-ingestion-control migrate-general-control-dry-run migrate-general-control seed dbt dbt-test transform dbt-docs clean-lake lint

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

# ==== Ingest (PostgreSQL control plane + immutable Bronze response files) ====
# Collector reference hành chính tĩnh, độc lập với Open-Meteo forecast.
ingest-provinces:
	uv run python -m vn_climate_risk_monitor.ingestion.collectors.administrative_reference

# Forecast collector: plan là read-only; canary/full ghi PostgreSQL + MinIO.
ingest-weather-plan:
	uv run collect-open-meteo-forecast

ingest-weather-canary:
	uv run collect-open-meteo-forecast --execute --limit 1

ingest-weather:
	uv run collect-open-meteo-forecast --execute

# Available-now Bronze loader. Canary và production dùng checkpoint scope riêng.
load-weather-canary:
	uv run load-open-meteo-forecast --scope canary_1

load-weather:
	uv run load-open-meteo-forecast --scope production

# Phase 5 operational entrypoint: deterministic hourly slot, collect then drain.
run-weather-plan:
	uv run run-open-meteo-pipeline

run-weather-canary:
	uv run run-open-meteo-pipeline --execute --limit 1

run-weather:
	uv run run-open-meteo-pipeline --execute

# Historical plan grouped by year; runtime checkpoints by month, read-only here.
plan-historical:
	uv run plan-open-meteo-archive

# Archive source JSON + year-partitioned Bronze table. Backfill admits only one
# new month per command by default so the configured free-tier guardrail wins.
HISTORICAL_YEAR ?= 2000

run-historical-canary:
	uv run run-open-meteo-archive --year $(HISTORICAL_YEAR) --month 1 --limit 1 --execute

run-historical-year:
	uv run run-open-meteo-archive --year $(HISTORICAL_YEAR) --execute --max-periods 12

run-historical-backfill:
	uv run run-open-meteo-archive --execute --max-periods 1

run-historical-tail:
	uv run run-open-meteo-archive --tail --execute

historical-status:
	uv run observe-ingestion --pipeline-name open_meteo_archive --dataset historical_weather_hourly --scope backfill --stale-after-minutes 2160

historical-tail-status:
	uv run observe-ingestion --pipeline-name open_meteo_archive --dataset historical_weather_hourly --scope tail --stale-after-minutes 2160

weather-status:
	uv run observe-open-meteo-ingestion --scope production

weather-healthcheck:
	uv run observe-open-meteo-ingestion --scope production --check

# Migration v2: exact allowlist; DuckLake cleanup managed files, MinIO chỉ xóa
# prefix unmanaged raw/geography/... đã khai báo trong migration.
migrate-legacy-dry-run:
	uv run python scripts/migrations/001_remove_legacy_relations.py

migrate-legacy:
	uv run python scripts/migrations/001_remove_legacy_relations.py --execute

# Migration storage layout: bronze/source -> bronze/files và catalog1.bronze ->
# bronze_store.tables. Mặc định chỉ in plan; target execute không chạy dbt build.
migrate-bronze-layout-dry-run:
	uv run python scripts/migrations/002_split_bronze_files_tables.py

migrate-bronze-layout:
	uv run python scripts/migrations/002_split_bronze_files_tables.py --execute

# Migration control plane: chỉ drop đúng hai DuckLake relation ops legacy khi rỗng.
migrate-ingestion-control-dry-run:
	uv run python scripts/migrations/003_move_ingestion_control_to_postgres.py

migrate-ingestion-control:
	uv run python scripts/migrations/003_move_ingestion_control_to_postgres.py --execute

# Migration v4: generalize run/file control metadata; parser/Bronze giữ theo source.
migrate-general-control-dry-run:
	uv run python scripts/migrations/004_generalize_ingestion_control.py

migrate-general-control:
	uv run python scripts/migrations/004_generalize_ingestion_control.py --execute

# ==== Transform (dbt + DuckDB + DuckLake) ====
# dbt project ở transform/, không phải transform/dbt/
seed:
	cd transform && uv run dbt seed --profiles-dir .

dbt:
	cd transform && uv run dbt run --profiles-dir .

dbt-test:
	cd transform && uv run dbt test --profiles-dir .

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
