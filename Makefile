.PHONY: bootstrap bootstrap-env bootstrap-geography up down logs backup-metadata restore-metadata map-grid fetch-forecast fetch-archive backfill-archive load forecast-pipeline archive-pipeline quality quality-forecast quality-archive health health-alert seed dbt dbt-test freshness transform transform-forecast processing-full-refresh dbt-docs serve-api maintain-lake clean-lake lint airflow-init airflow-logs airflow-shell

# ==== Setup ====
bootstrap-env:
	cp -n .env.example .env || true

bootstrap: bootstrap-env
	uv run python scripts/bootstrap.py

# Chỉ seed và build graph địa lý cần để tạo gold.dim_ward.
# Tách khỏi bootstrap hạ tầng để không tạo vòng phụ thuộc bootstrap <-> dbt.
bootstrap-geography:
	cd transform && uv run dbt seed --profiles-dir . --select +dim_ward
	cd transform && uv run dbt build --profiles-dir . --select +dim_ward --exclude resource_type:seed

# ==== Infrastructure (Docker Compose: MinIO + Postgres + pgAdmin) ====
up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

# PostgreSQL chứa metadata DuckLake, ingestion checkpoint và dữ liệu tham chiếu.
# Mọi tham số kết nối/đường dẫn được scripts đọc từ biến môi trường.
backup-metadata:
	scripts/backup_metadata.sh

restore-metadata:
	scripts/restore_metadata.sh

# ==== Fetch (HTTP→MinIO) rồi autoloader nạp bronze (SQL) ====
# Mặc định chỉ IN KẾ HOẠCH; thêm EXEC=1 để chạy thật.
EXEC ?=
_X = $(if $(EXEC),--execute,)

fetch-forecast:
	uv run fetch-open-meteo forecast $(_X)

# Chốt ô lưới của từng model archive rồi ghi transform/seeds/ward_grid_map_seed.csv.
# Chạy MỘT LẦN trước backfill (và lại khi danh sách phường đổi). ~252 đơn vị quota.
MODEL ?=
map-grid:
	uv run fetch-open-meteo map-grid $(if $(MODEL),--models $(MODEL),) $(_X)

# Archive tự chọn model theo thời kỳ: era5 trước 2017, ecmwf_ifs từ 2017.
# Fetch theo Ô LƯỚI (12 ô era5 / 48 ô ifs), không theo 126 phường.
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
# Không tham số = chạy mọi nguồn trong sources/*.yml
load:
	uv run load-sources $(SOURCE)

# Pipeline production tuần tự. Make dừng ngay nếu một bước lỗi, nên quality fail
# sẽ chặn dbt build. Silver/Gold giữ forecast vintage; current serving view chỉ
# chiếu logical run mới nhất.
forecast-pipeline:
	$(MAKE) fetch-forecast EXEC=1
	$(MAKE) load SOURCE=open_meteo_forecast
	$(MAKE) quality-forecast
	$(MAKE) transform-forecast
	$(MAKE) health SCOPE=forecast REQUIRE_GOLD=1

# Bồi đuôi cả ERA5/IFS, chặn transform khi Bronze/control plane không đạt chuẩn.
archive-pipeline:
	$(MAKE) fetch-archive EXEC=1 START=$(START) END=$(END)
	$(MAKE) load SOURCE="open_meteo_archive open_meteo_ifs"
	$(MAKE) quality-archive
	$(MAKE) transform
	$(MAKE) health SCOPE=archive REQUIRE_GOLD=1

# ==== Data quality: Provero quét staging NGAY SAU load ====
# dbt không với tới staging vì autoloader ghi ngoài đồ thị dbt.
# Đọc QUA catalog DuckLake (không glob Parquet — glob thấy cả dòng đã xoá).
#
# --no-store: BẮT BUỘC. Provero v0.2.1 crash khi check `range` FAIL trên bảng có
#   cột timestamp: store/sqlite.py json.dumps(failing_rows_sample) không xử lý
#   được datetime. Tắt store thì né được; đánh đổi là mất `provero history`.
# --no-optimize: chạy từng check riêng thay vì gộp một query.
#
# Trả exit code 1 khi có check fail -> dùng làm cổng chặn trong CI được.
_PROVERO_FORECAST = DUCKLAKE_ALIAS=catalog1 DUCKLAKE_DATA_PATH=s3://$(or $(MINIO_BUCKET),vn-climate) DUCKLAKE_METADATA_SCHEMA=ducklake uv run provero run -c quality/provero.yaml --no-optimize --no-store
_PROVERO_ARCHIVE  = DUCKLAKE_ALIAS=catalog1 DUCKLAKE_DATA_PATH=s3://$(or $(MINIO_BUCKET),vn-climate) DUCKLAKE_METADATA_SCHEMA=ducklake uv run provero run -c quality/provero_archive.yaml --no-optimize --no-store

# Gate đầy đủ cho vận hành tay: Provero forecast + mọi nguồn + Gold/control/disk.
quality:
	$(_PROVERO_FORECAST)
	$(_PROVERO_ARCHIVE)
	uv run python scripts/healthcheck.py --scope all --require-gold

# Gate trước transform trong pipeline forecast: không phụ thuộc backfill archive.
quality-forecast:
	$(_PROVERO_FORECAST)
	uv run python scripts/healthcheck.py --scope forecast

# Gate Bronze archive/IFS và checkpoint.
quality-archive:
	$(_PROVERO_ARCHIVE)
	uv run python scripts/healthcheck.py --scope archive

SCOPE ?= all
REQUIRE_GOLD ?=
_GOLD = $(if $(REQUIRE_GOLD),--require-gold,)
health:
	uv run python scripts/healthcheck.py --scope $(SCOPE) $(_GOLD) --output logs/health.json

health-alert:
	uv run python scripts/alert_health.py --scope $(SCOPE) $(_GOLD)

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

# Chuỗi HAI process, mỗi process một checkpoint riêng:
#   staging --(_ingested_at)--> silver curated --(_updated_at)--> gold
# Tách ra để lỗi ở Gold không buộc Silver dedup lại 20M dòng từ đầu.
# Mỗi process chốt run_started_at TRƯỚC khi dbt đọc gì, và chỉ ghi mốc đó vào
# checkpoint khi dbt exit 0. Make dừng ngay nếu process đầu lỗi.
transform: seed
	uv run python scripts/run_processing.py run silver_weather
	uv run python scripts/run_processing.py run rain_gold

transform-forecast:
	uv run python scripts/run_processing.py run forecast_silver
	uv run python scripts/run_processing.py run forecast_gold

# Tiến độ + lịch sử run của một process.
PROCESS ?= rain_gold
processing-status:
	uv run python scripts/run_processing.py status $(PROCESS)

# Tính lại từ một mốc: make processing-rewind FROM=2026-08-25 REASON="bug X"
processing-rewind:
	uv run python scripts/run_processing.py reprocess-from $(PROCESS) \
		--from "$(FROM)" --reason "$(REASON)"

# Migration có audit cho thay đổi business rule/schema trên model incremental.
# Ví dụ: make processing-full-refresh PROCESS=forecast_gold \
#   SELECT='fct_rain_forecast_hourly+' REASON='rain band v2'
SELECT ?=
processing-full-refresh:
	@test -n "$(strip $(REASON))" || \
		(echo "REASON là bắt buộc cho full refresh" >&2; exit 2)
	uv run python scripts/run_processing.py run $(PROCESS) --full-refresh \
		--reason "$(REASON)" $(if $(strip $(SELECT)),--select "$(SELECT)",)

dbt-docs:
	cd transform && uv run dbt docs generate --profiles-dir . && uv run dbt docs serve --profiles-dir .

# ==== Operational serving (read-only health API + /ops dashboard) ====
serve-api:
	uv run uvicorn serving.api.app.main:app --host 0.0.0.0 --port $${PORT:-8000}

# Chạy giao diện Streamlit dashboard
serve-dashboard:
	uv run streamlit run serving/dashboard/app.py --server.port $${DASHBOARD_PORT:-8501} --server.address 0.0.0.0

fetch-geojson:
	uv run python scripts/fetch_hanoi_geojson.py

# Nhãn ngập từ bảng Flourish nhúng trong bài tường thuật VnExpress 07/10/2025.
# Script DỪNG nếu version Flourish đổi hoặc invariant 123 dòng/122 ngập không
# khớp — nguồn báo chí có thể được sửa sau khi đăng, và một seed nhãn tự đổi
# dưới chân mô hình còn tệ hơn không có nhãn.
fetch-flood-observations:
	uv run python -m vn_climate_risk_monitor.flood_observations

# Geocode NHÁP cho quan sát ngập: Nominatim + point-in-polygon trên 126 ranh
# giới phường. Mọi dòng ghi ra đều `geocode_verified=false` — phải soát tay rồi
# đổi thành true, vì chỉ dòng true mới vào tập huấn luyện. Chạy lại mặc định
# giữ nguyên mọi review đang có; truyền
# GEOCODE_ARGS=--refresh-unverified khi thực sự muốn thay đề xuất chưa duyệt.
GEOCODE_ARGS ?=
geocode-flood-observations:
	uv run python -m vn_climate_risk_monitor.flood_geocode $(GEOCODE_ARGS)

# ==== Airflow (orchestration) ====
airflow-init:
	docker compose exec postgres psql -U $${POSTGRES_USER:-vnclimate} -d $${POSTGRES_DB:-vnclimate} -c "CREATE SCHEMA IF NOT EXISTS airflow;"
	docker compose exec airflow airflow db migrate
	docker compose exec airflow airflow pools set \
	  lakehouse_single_writer_pool 1 \
	  "Single-writer lock: tranh xung dot DuckLake va quota Open-Meteo"

airflow-logs:
	docker compose logs -f airflow

airflow-shell:
	docker compose exec airflow bash

# ==== Bảo trì lakehouse ====
# Retention thường kỳ: giữ snapshot 7 ngày; file chỉ xóa sau 2 ngày
# nằm trong deletion queue. DAG daily gọi cùng lệnh này.
maintain-lake:
	uv run python scripts/maintain_lake.py

# Emergency squash: xóa toàn bộ time-travel, chỉ giữ current version.
clean-lake:
	uv run python scripts/clean_lake.py

# ==== Quality ====
lint:
	uv run ruff check .
