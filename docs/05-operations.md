# Operations

> The pipeline is being rebuilt (see `docs/adr/`). The first two sections below
> describe the new pipeline; later sections still describe the old one and are
> rewritten in the final phase.

## Start

Create local credentials first; do not commit `.env`:

```powershell
Copy-Item .env.example .env
docker compose up -d --build
docker compose ps
```

Every forecast DAG run starts with `init`, which creates the MinIO bucket, the
DuckLake schemas and the `meta.*` tables; it is safe to rerun.

## Forecast loader is blocked by a bad Bronze file

The loader (`load` task) checks every new Bronze file with Great Expectations and
loads nothing if one fails. Bronze is immutable, so a genuinely bad Open-Meteo
response blocks every later load until you skip it. The failed run's reason is in
Postgres:

```sql
SELECT started_at, error FROM meta.job_runs
WHERE asset = 'silver.stg_open_meteo_forecast' ORDER BY id DESC LIMIT 5;
```

To skip the bad file, move the watermark past its landing time (`LastModified`
in MinIO); the next run loads only files that landed later:

```sql
UPDATE meta.watermarks SET watermark = '<landing time + 1 second>'
WHERE asset = 'silver.stg_open_meteo_forecast';
```

To reprocess a range instead, move the watermark back; re-read rows are
deduplicated downstream.

When running dbt directly from Windows PowerShell, force Python UTF-8 mode so
Vietnamese place names in models and seeds are decoded consistently:

```powershell
$env:PYTHONUTF8 = "1"
uv run dbt parse --project-dir transform --profiles-dir transform
```

Linux containers and CI already use UTF-8 locales.

The first environment initialization must create both physical Silver weather
tables before a tagged Gold build. Fetch at least one forecast run and the intended
archive window, then trigger the archive DAG followed by the forecast DAG. The DAG
order is archive then forecast because the shared grid dimension reads the archive
intermediate model. Missing cross-domain relations fail fast; dbt no longer hides
them behind relation-existence branches or empty placeholder CTEs.

## Normal runs

Airflow owns schedule and task retries. The two data DAGs are explicit:

```text
copy_raw → autoload_staging → process_dbt → health
```

Airflow owns one complete run, including dbt publication and checkpoint advance.
For an ad-hoc end-to-end run, trigger `open_meteo_forecast_hourly`; `auto-process`
intentionally exposes the phases used by that DAG rather than a `run` subcommand.
These public commands are useful for manual ingestion and diagnosis:

```text
fetch-open-meteo forecast --execute
auto-loader forecast
auto-process status forecast
pipeline-health --scope forecast --require-gold
```

Archive receives a monthly window and loads both code-native archive sources. It does
not run `dbt seed` on every monthly execution; static seed changes are deliberate
bootstrap/maintenance operations.

## State, retry, and leases

`ingestion.ingestion_runs` and `ingestion.ingestion_files` record source run,
status, retry count, worker, lease, parser version, and error metadata. A file is
not marked `COMMITTED` until its DuckLake staging transaction commits. For a
failed file, inspect the error metadata and retry through Airflow or
`auto-loader <group>`.

An expired lease is reclaimable. Retrying a committed object is idempotent because
the loader replaces rows for that `_source_file`. Bronze is never overwritten or
deleted by retry.

`processing.processing_state` and `processing.processing_runs` record flow status,
source checkpoints, row counts, rewind/abandon reasons, and
`published_snapshot_id`. A failed dbt build leaves the checkpoint unchanged.

## Status, rewind, and abandon

```text
auto-process status forecast
auto-process reprocess-from forecast --from 2025-01-01T00:00:00Z --reason "late source correction"
auto-process abandon forecast --reason "replace an abandoned worker run"
pipeline-health --scope all --require-gold
```

The first run of active `forecast` or `archive` performs a controlled full refresh.
Rewind creates an audit record and causes the next run to reprocess from the
requested point. Abandon is an explicit terminal
audit action; it does not delete Bronze or old run history.

## Maintenance and reset

Maintenance is separate from data DAGs and runs one retention task:

```text
maintain-lakehouse --snapshot-retention-days 7 --file-grace-days 2
```

The reset command is confirmation-protected and scopes dropped objects to DuckLake
Silver/Gold. It never deletes MinIO Bronze:

```text
reset-lakehouse --list
reset-lakehouse --yes
```

Use `--keep-staging` when iterating only on Gold models. Do not combine it with
`--reset-ingestion`; removing the file ledger while retaining staging would append
the same Bronze files again.

## Recovery checklist

1. Run `pipeline-health --scope all --require-gold` and capture the JSON report.
2. Check Airflow task logs and the matching PostgreSQL ingestion/processing run.
3. If a lease is stale, rerun the source load; the loader replaces that file's staging rows.
4. If dbt failed, fix the model/test and rerun the same flow. Confirm the checkpoint did not advance.
5. For late data, use `reprocess-from` with an explicit reason; verify the new snapshot and Gold readiness.
6. If a worker is irrecoverable, `abandon` it, then start a new run. Preserve the audit records.
7. Treat Bronze as the recovery boundary. Never repair staging by deleting Bronze; reload from immutable object paths.

Backup and restore scripts under `scripts/` are operational utilities for the
local portfolio. Verify restored catalog metadata, MinIO object checksums, and a
fresh health report before re-enabling scheduled DAGs.
