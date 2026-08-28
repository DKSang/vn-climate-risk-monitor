# Design: Open-Meteo fetch Bento → Python

**Date:** 2026-08-28
**Status:** accepted — copier is stdlib `urllib` + existing MinIO SDK.
**Supersedes:** [2026-08-27-bento-fetch-design.md](2026-08-27-bento-fetch-design.md) for the copy runtime only.
**Scope:** replace Bento GET→PUT with Python in `open_meteo.py`. Autoloader, dbt, DuckLake, MinIO prefixes, CLI stay.

## Problem

Ingest’s only job is land source JSON 1:1 onto MinIO. The current copy path spawns one Bento (or Docker) process per row, sleeps in Python because YAML `rate_limit` never sees a second message, then `stat_object` because Bento’s `catch` exits 0. That kit is heavier than GET+PUT. dlt was evaluated and rejected: it cannot skip normalize or keep HTTP bytes.

## Non-goals

- Do not change autoloader (`load-sources`), Bronze SQL, dbt, or DuckLake.
- Do not add `requests`, `httpx`, rclone, or a generic multi-source copy package.
- Do not extract a new REST source framework. One source exists (Open-Meteo); a second source can copy `land()` later.
- Do not write a jobs.jsonl / one-shot Bento pipeline.
- Do not change resume-by-basename, batch size 25, or object key prefixes.

## Architecture

```
make fetch-* [--execute]
        │
        ▼
  missing_rows()     gold.dim_hanoi_ward, list MinIO, skip response_NNN.json
        │            → [{url, key}]
        ▼
  for row, pause_s   OPEN_METEO_MAX_EFFECTIVE_CALLS_PER_HOUR
        │
        ▼
  land()             urllib GET → reject error JSON → wrap object as array
                     → minio.put_object(key)
        ▼
  autoloader (unchanged)
```

Planner and pacing stay in `open_meteo.py`. Copy is three functions in the same file. Package `ingest` is autoloader only (file → table).

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
| Pacing | `pause_s = 3600 × units_per_request ÷ OPEN_METEO_MAX_EFFECTIVE_CALLS_PER_HOUR` before each GET after the first |

## Copier

All in `src/vn_climate_risk_monitor/open_meteo.py`. No new dependencies.

```python
RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})
RETRIES = 5
RETRY_PAUSE_S = 60
HTTP_TIMEOUT_S = 60
USER_AGENT = "vn-climate-risk-monitor"

def get_body(url: str) -> bytes: ...
def bronze_bytes(body: bytes) -> bytes: ...
def land(client: Minio, bucket: str, url: str, key: str) -> None: ...
```

`get_body`: `urllib.request.Request` with `Accept: application/json`, `Accept-Encoding: identity`, `User-Agent` as above. Timeout 60s. On `HTTPError` whose code is in `RETRY_STATUSES`, sleep 60s and retry. **Five attempts total** (the first GET plus four retries). Other HTTP codes and `URLError` raise immediately. Successful body is returned as received (no re-serialize yet).

`bronze_bytes`: `json.loads`. If the root is a dict, wrap as `[root]`. If any element has `error is True`, raise `RuntimeError` with `reason` if present. `json.dumps` the array with default separators (compact). This is the one allowed transform: singleton → array for DuckDB. Wire whitespace of multi-location arrays may change on re-dump; existing production files are already arrays from Bento’s Bloblang, which also re-serialized.

`land`: `body = bronze_bytes(get_body(url))` then `client.put_object(bucket, key, BytesIO(body), len(body), content_type="application/json")`. No `stat_object` after PUT. `put_object` success is the commit signal.

`main()` loop: same pause as today; call `land(...)` instead of Bento; on failure print the existing recovery sentence and `SystemExit(2)`. Catch `OSError` (covers `URLError`/`HTTPError`) and MinIO `S3Error`.

Delete `minio_env()`, `BENTO_CONFIG`, `ingest.copy` imports, `subprocess` for Bento.

## Files to remove

- `src/ingest/copy.py`
- `ingest/copy/open_meteo.yaml` (and the empty `ingest/copy/` dir if nothing remains)
- `tests/unit/test_copy.py`

Update `src/ingest/__init__.py` so ingest is autoloader only. Drop Bento/binary/Docker copy mentions from README, `docs/03-architecture.md`, `docs/03a-repo-structure.md`, `docs/04b-ingestion-runbook.md`. Leave `docs/04-ingestion.md` as historical (already marked obsolete).

## Failure handling

| Event | MinIO | Next run |
|---|---|---|
| HTTP 429/5xx then success | one object, array JSON | skip basename |
| HTTP 429/5xx exhausted | no object for that key | GET again |
| JSON `{error: true}` | no object | GET again |
| Crash during PUT | object may or may not exist | skip if basename present (at-least-once) |
| Crash after PUT before next row | object exists | skip; continue remaining rows |

A failed row never writes an error payload. Partial batch is safe because resume is per file, not per month.

## Tests

`tests/unit/test_open_meteo_land.py` using `unittest.mock` on `urllib.request.urlopen` and a fake MinIO client (dict of key→bytes). Do not use the `responses` package (`requests` is not a dependency).

- dict body → PUT bytes parse as a one-element array
- array body → PUT parses as the same array (keys preserved)
- `{"error": true, "reason": "Rate limit"}` → no `put_object`
- 429 then 200 → one PUT
- five 429s → raise, no PUT
- 400 → raise immediately, no retry sleep, no PUT

Keep `tests/unit/test_open_meteo_lookup.py` except `test_row_env_exposes_every_column`. Delete `test_copy.py`.

## Runtime

Python 3.13 already required. `minio` already in `pyproject.toml`. No `bento` binary, no `ghcr.io/warpstreamlabs/bento` image. Fetch works if MinIO is up; Docker is only for MinIO/Postgres as today.
