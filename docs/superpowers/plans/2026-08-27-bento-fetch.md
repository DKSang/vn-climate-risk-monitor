# Bento fetch Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land Open-Meteo JSON on MinIO via Bento; Python only plans work items (locations, skip-existing, dry-run).

> Amended 2026-08-27: no NDJSON/CSV job file and no ForEach package — `missing_rows()` plus `for` + Bento Copy Data. YAML path is `ingest/copy/open_meteo.yaml`.

**Architecture:** Planner writes NDJSON `{url, key}` jobs. One Bento pipeline does GET → wrap JSON array → `aws_s3` to MinIO. Autoloader is untouched.

**Tech Stack:** Bento YAML, existing MinIO/DuckDB planner helpers, Makefile CLI unchanged.

## Global Constraints

- Do not change bronze key prefixes, resume-by-basename, or `timeformat=unixtime`.
- Do not replace autoloader or dbt.
- Bento `max_in_flight: 1`; HTTP timeout 60s; 429 backoff.
- Keep `fetch-open-meteo` entry point and `make fetch-*` / `EXEC=1`.
- Work on branch `refactor/bento-fetch`, not master.

## Files

| File | Role |
|---|---|
| `src/vn_climate_risk_monitor/ingestion/plan.py` | Locations, batches, units, skip, job URLs |
| `src/vn_climate_risk_monitor/ingestion/fetch.py` | CLI + write NDJSON + run Bento |
| `ingest/open_meteo.yaml` | Bento pipeline |
| `tests/unit/test_ingestion_fetch.py` | Planner resume/URL tests |
| `Makefile`, `.gitignore`, runbook, README, repo-structure | Wire-up + docs |

---

### Task 1: Planner (TDD)

- [x] Rewrite `tests/unit/test_ingestion_fetch.py` against `jobs_for_prefix` / `existing_basenames` (no HTTP).
- [x] Extract `plan.py` from `fetch.py` (Location, prefixes, fields, `load_locations`, `effective_call_units`, `months_between`, URL builder, skip, jobs).
- [x] Run `uv run pytest tests/unit/test_ingestion_fetch.py` — pass.

### Task 2: Bento pipeline + CLI

- [x] Add `ingest/open_meteo.yaml` (file input, http GET, wrap array, aws_s3, rate 1/45s, max_in_flight 1).
- [x] Slim `fetch.py`: dry-run prints plan; `--execute` writes `.cache/open_meteo_jobs.ndjson` and runs `bento` or Docker image.
- [x] Ignore `--parallel` with one line of output.
- [x] `bento lint ingest/open_meteo.yaml` if binary/image available.

### Task 3: Docs + Makefile

- [x] `.gitignore` `.cache/`
- [x] Makefile comment: fetch is planner + Bento
- [x] Update `docs/04b-ingestion-runbook.md`, `docs/03a-repo-structure.md`, `README.md` ingest sentences
- [x] Run full unit tests

**Verify:** `uv run pytest tests/unit/test_ingestion_fetch.py tests/unit/test_ingestion_http.py tests/unit/test_config.py` and `bento lint` (or docker equivalent).
