# Design: Open-Meteo fetch Python → Bento

**Date:** 2026-08-27
**Status:** accepted, then amended the same day — keep Bento Copy Data YAML;
drop Lookup CSV, `foreach.py`, and unused Open-Meteo timeout/attempt/minute
settings. Current shape: `missing_rows()` → `for` + pause → `activities.copy`
(Bento). See [04b-ingestion-runbook.md](../../04b-ingestion-runbook.md).
**Scope:** replace HTTP GET + MinIO PUT in `fetch.py` only. Autoloader, dbt, DuckLake stay.

## Problem

`src/vn_climate_risk_monitor/ingestion/fetch.py` is a ~330-line Python collector: batch 126 wards, skip existing `response_NNN.json`, GET Open-Meteo, PUT JSON to MinIO. Adding more REST sources would mean another long Python file each. Bento YAML is the ingest path going forward.

## Non-goals

- Do not replace autoloader (`load-sources`) or dbt.
- Do not reimplement skip-if-exists, DuckLake location lookup, or dry-run in Bloblang.
- Do not use Bento `http_client` **input** (one URL loop). Work items drive `http` **processor** then `aws_s3`.
- Do not keep month-parallel Python threads. Bento runs `max_in_flight: 1` so Open-Meteo sees one in-flight request.

## Architecture

Same shape as a Fabric pipeline: **Lookup → ForEach → Copy Data**.

```
make fetch-* [--execute]
        │
        ▼
  Lookup   activities.lookup.run(missing_rows)  generic CSV + nguồn Open-Meteo
        │  reads gold.dim_hanoi_ward, lists MinIO, drops files already there
        ▼
  rows [{url, key}]  →  .cache/open_meteo_lookup.csv
        │
        ▼
  ForEach  activities.foreach.run()        generic
        │  one row at a time, paced by effective-call budget
        ▼
  Copy Data  activities.copy.run() → bento -c ingest/copy/open_meteo.yaml
        │  row columns exported as ROW_URL / ROW_KEY
        │  GET → wrap singleton JSON as array → PUT key on MinIO
        ▼
  autoloader (load, unchanged)
```

Generic (`src/activities`): Lookup CSV, ForEach, Copy Data (Bento).

Source-specific (`open_meteo.py` + `ingest/copy/open_meteo.yaml`): `gold.dim_hanoi_ward`, month/hour prefixes, resume by basename `response_NNN.json`, effective-call unit estimate, JSON array normalisation.

A new REST source needs one lookup function returning rows plus one YAML. No engine changes.

## Contract (must not change)

| Item | Value |
|---|---|
| Forecast prefix | `bronze/files/open_meteo/forecast/incremental/{YYYY}/{MM}/{DD}/{HH}/run_{ts}/response_{NNN}.json` |
| Archive prefix | `bronze/files/open_meteo/historical_weather_hourly/backfill/year=YYYY/month=MM/run_{ts}/response_{NNN}.json` |
| Resume | skip if `response_{index:03}.json` exists under prefix (any run dir) |
| Batch | 25 locations (`OPEN_METEO_LOCATION_BATCH_SIZE`) |
| Params | `timeformat=unixtime`, `timezone=UTC`, existing hourly fields + models |
| JSON shape | array of location objects (wrap a single object so the last batch of 1 ward matches DuckDB `read_json_auto` of existing files) |
| CLI | `uv run fetch-open-meteo forecast\|archive [--start --end] [--execute] [--limit]` |
| Make | `make fetch-forecast EXEC=1`, `make fetch-archive EXEC=1 START=… END=…` |

## Rate limit

Pacing lives in ForEach, not in the YAML: one Bento process per row means a per-process `rate_limit` resource would never see a second message. `pause_s = 3600 × units_per_request ÷ OPEN_METEO_MAX_EFFECTIVE_CALLS_PER_HOUR` (~43s archive, ~20s forecast). Bento still honors 429 with 5 retries and 60s backoff.

## Failure handling

A row whose request keeps failing is **dropped** by the `catch` branch, so an error body (e.g. the 429 JSON) can never overwrite a bronze object. `generate` sets `auto_replay_nacks: false` so a bad row cannot loop forever and burn quota. Bento then exits 0, so Python treats a missing object (`stat_object`) as the failure signal and stops the whole batch with exit code 2.

## Runtime

Prefer `bento` on PATH. If missing, `docker run --rm --network host` with `ghcr.io/warpstreamlabs/bento:1.20.0` (Linux/WSL; MinIO is localhost). Timeouts 60s (HTTP and S3). `max_in_flight: 1`.

`--parallel` is gone: ForEach is sequential by construction.

## Tests

Lookup unit tests replace the old `_land` HTTP fakes: resume skip, complete month = no rows, forecast prefix/URL, plus generic checks on `row_env`, CSV roundtrip and `for_each`. No Python 429 test (Bento owns retries). `bento lint` on the YAML, and a canary run proving an errored row uploads nothing.
