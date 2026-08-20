.PHONY: bootstrap bootstrap-env up down ingest ingest-forecast ingest-archive dbt dbt-test transform api dashboard backfill test lint

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

# ==== Ingest (dlt) ====
ingest:
	uv run python -m dlt_pipelines.pipelines.forecast_pipeline
	uv run python -m dlt_pipelines.pipelines.archive_pipeline

ingest-forecast:
	uv run python -m dlt_pipelines.pipelines.forecast_pipeline

ingest-archive:
	uv run python -m dlt_pipelines.pipelines.archive_pipeline

backfill:
	uv run python scripts/backfill_archive.py

# ==== Transform (dbt + DuckDB) ====
dbt:
	cd transform/dbt && uv run dbt run

dbt-test:
	cd transform/dbt && uv run dbt test

transform: dbt dbt-test

# ==== Serve ====
api:
	uv run uvicorn serving.api.app.main:app --host 0.0.0.0 --port 8000

dashboard:
	uv run streamlit run serving/dashboard/app.py

# ==== Quality ====
test:
	uv run pytest tests

lint:
	uv run ruff check .