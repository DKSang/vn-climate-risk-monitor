.PHONY: bootstrap bootstrap-env up down logs ingest-provinces migrate-legacy-dry-run migrate-legacy migrate-bronze-layout-dry-run migrate-bronze-layout seed dbt dbt-test transform dbt-docs clean-lake lint

# ==== Setup ====
bootstrap-env:
	cp -n .env.example .env || true

bootstrap: bootstrap-env
	uv run python scripts/bootstrap.py

# ==== Infrastructure (Docker Compose: MinIO + Postgres + Airflow + API + Dashboard) ====
up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

# ==== Ingest (collect immutable Bronze source files) ====
# Open-Meteo chưa được triển khai. Target này chỉ tải reference hành chính tĩnh.
ingest-provinces:
	uv run python -m vn_climate_risk_monitor.ingestion.collectors.administrative_reference

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
