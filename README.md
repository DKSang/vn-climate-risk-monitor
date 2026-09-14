# VN Climate Risk Monitor

VN Climate Risk Monitor is a small, reproducible data platform for Hanoi rainfall
pressure and historical flood replay. It turns hourly Open-Meteo data into an
explainable signal for 126 wards/communes while preserving raw source bytes,
lineage, checkpoints, and publication audit history.

This is a portfolio project for a local single-node deployment. It is not an
official weather warning, a calibrated flood-probability model, or an HA service.

## Architecture

```text
Open-Meteo → MinIO Bronze → autoloader → DuckLake Silver staging
           → dbt/DuckDB intermediate → marts/Gold → Streamlit
```

PostgreSQL stores DuckLake catalog metadata and detailed ingestion/processing
state. DuckDB is the compute engine. Airflow owns schedules and task retries;
PostgreSQL owns data checkpoints and audit state. See
[Architecture](docs/03-architecture.md) for layer ownership, trade-offs, and the
six architecture undercurrents.

## Quick start

```powershell
Copy-Item .env.example .env
# Replace every <...> value in .env with local credentials.
docker compose up -d --build
```

The default Compose profile starts PostgreSQL, MinIO, one-shot bootstrap, Airflow,
and Streamlit. pgAdmin is opt-in:

```powershell
docker compose --profile tools up -d pgadmin
```

Public commands:

```text
fetch-open-meteo forecast --execute
fetch-open-meteo archive --start 2025-01-01 --end 2025-02-01 --execute
auto-loader forecast
auto-process run forecast
auto-process status forecast
auto-process reprocess-from forecast --from 2025-01-01T00:00:00Z --reason "parser fix"
pipeline-health --scope all --require-gold
maintain-lakehouse
```

See [Data contracts](docs/04-data-contracts.md) for source grains, quality
gates, and Gold models. See [Operations](docs/05-operations.md) for bootstrap,
retry, rewind, maintenance, and recovery.

## Dashboard

Streamlit keeps three pages:

- Forecast map: the latest complete 72-hour forecast across the wards.
- Ward drilldown: rainfall windows, pressure signal, persistence, and flood-point context.
- Archive replay: historical rainfall by local date/hour alongside verified observations.

Dashboard queries pin a validated published snapshot. The serving query layer is
split into `serving/dashboard/common.py`, `forecast.py`, and `archive.py`; Gold
table names and UI semantics remain stable.

## Repository map

```text
src/vn_climate_risk_monitor/  sources, ingestion, processing, storage, lakehouse
sources/                       two parser SQL files; source definitions live in Python
transform/                     dbt models, macros, tests, and reference seeds
orchestration/                 two explicit data DAGs and one maintenance DAG
serving/                       Streamlit application and query modules
tools/                         one-time scraping, geocoding, and GeoJSON generation
tests/                         unit and contract tests
docs/                          architecture, contracts, and operations
```

## Development gates

Run `uv run pytest -q`, `uv run ruff check .`, `uv run dbt parse --project-dir
transform --profiles-dir transform`, `docker compose config`, and
`uv run python scripts/check_docs.py` before sharing a change.

On Windows PowerShell, set `$env:PYTHONUTF8 = "1"` before dbt commands so
Vietnamese model and seed text is decoded as UTF-8.
