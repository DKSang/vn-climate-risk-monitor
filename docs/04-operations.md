# 4. Operations

Runbook for the local stack. Every command runs from the repository root.

## Start and stop

```powershell
Copy-Item .env.example .env        # then change the passwords
docker compose up -d --build
docker compose ps                  # all four services should be healthy
docker compose down                # stop; add -v to also delete all data
```

| Service | URL | Login |
|---|---|---|
| Report (Streamlit) | http://localhost:8501 | none |
| Airflow | http://localhost:8080 | `AIRFLOW_USERNAME` / `AIRFLOW_PASSWORD` |
| MinIO console | http://localhost:9001 | `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` |

DAGs start paused. Unpause `open_meteo_forecast_hourly` for the live forecast and
`lakehouse_maintenance_daily` for housekeeping.

## Load history (archive backfill)

The archive DAG processes the month of its data interval, on day 6 of the next
month (Open-Meteo's archive trails real time by about five days). Load past
months with a backfill:

```powershell
docker compose exec airflow airflow dags backfill open_meteo_archive_monthly -s 2024-01-01 -e 2024-12-31
```

Re-running a month is safe: fetch skips batches already in Bronze, and the
merge replaces rows by key.

## See what ran

```sql
-- Latest attempts and their errors (Postgres, database vnclimate)
SELECT asset, status, started_at, finished_at, rows_in, rows_out, error
FROM meta.job_runs ORDER BY id DESC LIMIT 20;

-- Where each asset will resume from
SELECT * FROM meta.watermarks ORDER BY asset;
```

In DuckDB, `FROM catalog1.snapshots()` lists lake snapshots; the report reads the
newest one whose `commit_message` is `publish`.

## A load is blocked by a bad Bronze file

The `load` task validates new Bronze files with Great Expectations and loads
nothing if one fails. `meta.job_runs.error` names the failed expectation and the
run or month. Bronze is immutable, so a genuinely bad response keeps failing
until you act:

- **Skip it:** move the watermark past its landing time (MinIO `Last Modified`);
  the next run reads only files that landed later.

  ```sql
  UPDATE meta.watermarks SET watermark = '<landing time + 1 second>'
  WHERE asset = 'silver.stg_open_meteo_forecast';  -- or silver.stg_open_meteo_archive
  ```

- **Fetch it again:** delete that batch object in MinIO, then clear the DAG
  run's `fetch` task in Airflow.

## Reprocess a range

Move an asset's watermark back; its next run re-reads everything newer.
Downstream dedup-then-merge makes the re-read harmless.

```sql
UPDATE meta.watermarks SET watermark = '2026-09-01 00:00+00'
WHERE asset = 'silver.clean_weather_forecast_hourly';
```

To rebuild an asset from nothing, delete its watermark row; the next run
processes all of its source.

## Maintenance

`lakehouse_maintenance_daily` runs `maintain-lakehouse`. It expires snapshots
older than 7 days, then deletes data files unreferenced for 2 days. The report
pins the latest publication, so only snapshots older than a week become
unreadable.

## Run a step by hand

Every task is a console script (see `pyproject.toml`), runnable in the Airflow
container or locally with `uv run` against the published ports:

```text
init-lakehouse
fetch-forecast --slot 2026-09-25T05:15:00+00:00    fetch-archive --month 2026-08-01
load-forecast                                      load-archive
build-clean-forecast                               build-clean-archive
build-gold-forecast                                build-gold-archive
maintain-lakehouse
```

Writers must not overlap ([ADR 0001](adr/0001-watermark-incremental-pattern.md)).
Pause the DAGs before running write steps by hand.

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| Report shows "Chưa có bản publish nào" | No Gold build has succeeded yet: run the forecast DAG once |
| Report shows "…không còn giờ dự báo nào…" | The published forecast is older than its 72 hours: check why the forecast DAG stopped |
| "Lịch sử mưa" page shows "Chưa có dữ liệu lịch sử" | No archive month published: run a backfill |
| `fetch` fails with a connection error | Open-Meteo is unreachable or rate-limited; Airflow retries twice, then clear the task |
| `airflow` stays unhealthy | `docker compose logs airflow`; the metadata schema lives in Postgres schema `airflow` |
| dbt on Windows garbles Vietnamese names | Set `$env:PYTHONUTF8 = "1"` before running dbt directly |
