# Data contracts

## Sources

Source groups are immutable `SourceConfig` values in
`vn_climate_risk_monitor.auto_loader.config`; no runtime YAML parser is involved. Forecast and
archive use separate parser SQL files because their API payloads and grains differ.

Open-Meteo forecast covers the current 72-hour horizon. Archive uses ERA5 before
2017 and ECMWF IFS from 2017 onward; the models remain distinct. Requests are
planned on weather-grid cells and joined back to wards through the versioned
mapping seed.

## Bronze and Silver

Bronze object keys are immutable and retain the existing `bronze/files/...`
layout. The raw object is the exact HTTP response body, including an API error
payload if one was received. The response is inspected in memory for error
metadata before it is stored; no parse/re-serialize cycle changes the bytes.

Physical staging tables are stable public interfaces:

- `silver.stg_weather_forecast`
- `silver.stg_weather_archive_hourly`

Staging is append-only by source file and preserves source history. Parsed rows
carry `_source_file`, `_ingested_at`, and ingestion metadata needed for audit. An
idempotent retry deletes the batch's `_source_file` rows and reinserts them in one
DuckLake transaction.

## Grain and quality

The forecast physical grain is one weather grid cell and valid UTC hour per
forecast run. A publishable run contains exactly 126 locations and 72 distinct
valid hours. Duplicate records cannot mask a missing location.

The archive physical grain is one weather model, weather grid cell, and valid UTC
hour per source vintage. `era5` and `ecmwf_ifs` are accepted model values and are
never silently blended into one grid.

dbt generic tests cover non-empty sources, required columns, accepted model
values, precipitation ranges, and source freshness. dbt singular tests cover
Gold readability, grain, pressure semantics, forward coverage, rolling-window
monotonicity, and ward/grid coverage. `process_dbt` uses `dbt build`, so a failed
test prevents checkpoint advancement and publication.

## Gold contract

Public dimensions and bridges:

- `gold.dim_ward`: active Hanoi ward/commune identity and coordinates.
- `gold.dim_grid`: weather-model grid-cell identity.
- `gold.bridge_ward_grid`: deterministic ward-to-grid mapping by model.

Public facts:

- `gold.fct_rain_forecast_hourly`: forecast history by run, grid cell, and hour.
- `gold.fct_rain_forecast_current_hourly`: latest validated forecast horizon.
- `gold.fct_rain_pressure_alert`: current rainfall-pressure signal by ward/hour.
- `gold.fct_rain_archive_hourly`: historical archive rainfall by model/grid/hour.
- `gold.fct_flood_event_observation`: sourced and normalized flood observations.

Reference dimensions and bridges are rebuilt as tables from versioned seeds;
runtime soft-delete is not used for static references. Forecast and archive use
native dbt model tags, so no handwritten selector registry can drift from the
graph. Dashboard semantics and these names are stable.

## Snapshot semantics

Processing records `published_snapshot_id` only for a successful, tested dbt build.
Serving resolves that validated snapshot from PostgreSQL and pins DuckDB reads to
it, so one dashboard page cannot mix two publication states. A snapshot is not
usable merely because a table exists: it must be tied to a successful flow run and
remain readable.
