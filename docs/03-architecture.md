# Architecture

## Scope and ownership

The platform is intentionally single-node and single-writer. Each layer owns one
kind of state:

| Layer | Owner | Responsibility |
|---|---|---|
| Source planning | `sources.open_meteo` | Forecast/archive request windows and parameters |
| Bronze | `ingestion.fetch` + MinIO | Immutable HTTP response bytes at existing object paths |
| Ingestion control | PostgreSQL `ingestion` | Discovery runs, file status, leases, retries, parser metadata, errors |
| Silver staging | `ingestion.loader` + DuckLake | File-to-row parsing with `_source_file` lineage |
| Processing control | PostgreSQL `processing` | Checkpoints, run audit, rewind/abandon, published snapshot |
| Intermediate and marts | dbt + DuckDB/DuckLake | Types, deduplication, business logic, Gold contract |
| Orchestration | Airflow | Schedule, task dependency, task retry, single-writer pool |
| Serving | Streamlit | Read-only snapshot-pinned dashboard queries |

The runtime path is:

```text
Open-Meteo → MinIO Bronze → autoloader → DuckLake Silver staging
           → dbt/DuckDB intermediate → dbt/DuckDB marts → Streamlit
```

PostgreSQL is the durable catalog and control plane, not the analytical warehouse.
Bronze is not represented by a dbt model: raw bytes are the source of truth and
physical Silver staging is written by the autoloader.

## Storage and publication

Bronze objects retain the existing `bronze/files/...` paths and immutable-key rule.
A response is parsed in memory only to detect an Open-Meteo error; the exact
received bytes are written to MinIO. Silver staging can retain multiple versions
of a source grain, and every loaded row carries `_source_file`.

Each staging retry runs as one DuckLake transaction: delete rows whose
`_source_file` belongs to the batch, insert the parsed batch, and commit. PostgreSQL
marks the file `COMMITTED` only after that commit. If the process crashes in the
gap, the lease expires and a retry replaces that file's rows rather than appending
duplicates.

Processing has two active flow keys, `forecast` and `archive`. Each executes one
dbt build from intermediate through marts using a selector. A 15-minute overlap
protects late-arriving rows. The checkpoint advances only after the complete build
and tests succeed, and `published_snapshot_id` is written with the successful run.

## Trade-offs

- Single-node/single-writer keeps recovery and local development understandable;
  it does not provide HA, horizontal scale, or concurrent DuckLake writers.
- MinIO Bronze plus detailed PostgreSQL state costs more bookkeeping than a direct
  API-to-table load, but makes replay, parser upgrades, and recovery demonstrable.
- dbt-native tests replace a second quality framework. Quality rules live beside
  the model/source contract, while health checks stay operational and cheap.
- DuckDB/DuckLake is an economical local analytical store, not a multi-node
  warehouse under high concurrency.
- Static geography and flood references are versioned seeds. Scraping, geocoding,
  and GeoJSON generation are one-time tools, not runtime pipeline dependencies.

## Six undercurrents

### Security

Local `.env` holds credentials and is gitignored. Compose requires explicit
credential values, exposes only the local services needed by the portfolio, and
uses non-root application images. This is local configuration, not production
secret management.

### Data management

Bronze is immutable. Layer contracts, `_source_file` lineage, stable grains, and
dbt generic/singular tests make downstream data traceable and replayable.

### DataOps

Airflow retries tasks; PostgreSQL tracks data leases, checkpoints, errors, and
publication. `pipeline-health` checks freshness, incomplete/failed files, Gold
readiness, and validated snapshots. See [Operations](05-operations.md).

### Architecture

The design chooses one catalog, one writer, and one local Compose deployment. It
does not add Kubernetes, HA, a service API, or Polars because those abstractions
do not improve this portfolio workload.

### Orchestration

There are exactly two explicit data DAGs:

```text
copy_raw → autoload_staging → process_dbt → health
```

Forecast runs hourly, archive runs monthly, and the maintenance DAG has one task
for retention/cleanup. Archive does not rerun static `dbt seed`.

### Software engineering

Runtime code is one `vn_climate_risk_monitor` package with cohesive modules.
Unit/contract tests, Ruff, dbt parse/build/test, pinned lockfile dependencies, and
documentation link checks form the CI surface.
