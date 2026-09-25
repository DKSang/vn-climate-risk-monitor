# 2. Data contracts

What each layer promises to the next: the key, the grain and the columns that
downstream code relies on. Terms follow the glossary in
[`CONTEXT.md`](../CONTEXT.md).

## Source

[Open-Meteo](https://open-meteo.com/) (free, non-commercial, CC BY 4.0), always
with the ECMWF IFS model at hourly resolution and timestamps in UTC.

| Dataset | Endpoint | Fields | One request covers |
|---|---|---|---|
| Forecast | `/v1/forecast`, `forecast_hours=72` | precipitation, rain, showers, precipitation_probability, weather_code | 25 grid cells × the next 72 hours |
| Archive | `/v1/archive`, `start_date`…`end_date` of one month | precipitation, rain, weather_code | 25 grid cells × every hour of the month |

The 126 wards fall in **48 distinct ECMWF IFS grid cells**
(`transform/seeds/ward_grid_map_seed.csv`), so each run or month is 2 requests.
Only grid cells are requested; wards are joined back in Gold.

## Bronze

```text
bronze/open_meteo/forecast/year=YYYY/month=MM/day=DD/forecast_run=YYYYMMDDTHH/batch_NNN.json
bronze/open_meteo/archive/year=YYYY/month=MM/batch_NNN.json
```

- The object is the exact response body. Open-Meteo errors
  (`{"error": true}`) are rejected before anything is written.
- Keys are deterministic, so a retried fetch skips batches that already landed.
- Objects are never overwritten or deleted by the pipeline.

## Silver staging (`silver.stg_*`)

Append-only copies of Bronze rows, loaded by `pipeline/open_meteo/load.py`.
Duplicates are kept, as the loader may re-read a file.

| Table | Grain | Columns |
|---|---|---|
| `stg_open_meteo_forecast` | forecast_run × grid cell × hour, per source file | `forecast_run`, `grid_latitude`, `grid_longitude`, `valid_at`, the five fields, `_source_file`, `_inserted_at` |
| `stg_open_meteo_archive` | grid cell × hour, per source file | `grid_latitude`, `grid_longitude`, `valid_at`, the three fields, `_source_file`, `_inserted_at` |

`forecast_run` is parsed from the Hive partition of the object path.

## Silver clean (`silver.clean_*`)

dbt incremental models with the `merge` strategy. New staging rows (after the
watermark) are deduplicated with `QUALIFY ROW_NUMBER()` on the key, keeping the
newest `_inserted_at`, then merged.

| Table | Unique key | Notable columns |
|---|---|---|
| `clean_weather_forecast_hourly` | `forecast_run, grid_cell_id, valid_at` | `precipitation_mm`, `rain_mm`, `showers_mm`, `precipitation_probability_pct`, `weather_code` |
| `clean_weather_archive_hourly` | `grid_cell_id, valid_at` | `precipitation_mm`, `rain_mm`, `weather_code` |

`grid_cell_id` is a stable text id built from model and coordinates
(`macros/grid_cell_id.sql`). Every row carries `_source_file`, `_inserted_at`
and `_updated_at` (the time of the merge that last wrote it).

## Gold (`gold.*`)

| Model | Grain | Build | Purpose |
|---|---|---|---|
| `dim_ward` | ward | table, from seed | 126 Hanoi wards and communes |
| `dim_grid` | grid cell | table, from seed | Weather-model grid cells |
| `bridge_ward_grid` | ward × weather model | table, from seed | Which grid cell a ward reads |
| `fct_rain_forecast_hourly` | forecast run × grid cell × hour | incremental merge of touched runs | Forecast history with rolling (past) and forward rain windows |
| `fct_rain_forecast_current_hourly` | grid cell × hour of the latest run | view | The current forecast |
| `fct_rain_pressure_alert` | forecast run × ward × hour | table, current run only | Rain pressure level, score and reasons |
| `fct_rain_archive_hourly` | grid cell × hour | table, full rebuild | Past rainfall with rolling windows across month boundaries |

Rain windows are 1, 3, 6, 12 and 24 hours. A window is NULL when any of its
hours is missing, never a partial sum.

**Rain pressure** is a prioritisation signal, not a flood probability. Its levels
are `HIGH`, `ELEVATED`, `WATCH`, `NORMAL` and `UNKNOWN`. They come from the
forward windows, persistence over the last three forecast runs, and an upward
revision of the next-24h forecast. The thresholds are dbt vars
(`rain_pressure_thresholds` in `transform/dbt_project.yml`). The 0–100 score is
the highest forward window relative to its threshold.

## Publication

The report reads only the newest DuckLake snapshot whose commit message is
`publish` ([ADR 0002](adr/0002-publish-a-ducklake-snapshot.md)). Such a snapshot
is created only after a Gold build and all of its tests succeed. Every
publication also adds a row to `gold._publications`
(`published_at`, `asset`, `job_started_at`).

## Control tables (Postgres `meta`)

| Table | Row | Written by |
|---|---|---|
| `meta.watermarks` | one per asset: start time of its last successful run | `job_run()`, on success only |
| `meta.job_runs` | one per attempt: status, times, watermark used, rows in/out, error | `job_run()`, always |
