# Incremental processing uses start-time watermarks kept in Postgres

Every incremental asset (Silver staging, Silver clean, the Gold history facts) reads only source rows whose `_inserted_at` or `_updated_at` is newer than its watermark. The watermark is the start time of the asset's last successful job run, stored in Postgres `meta.watermarks`. Every attempt is logged in `meta.job_runs`. A single `job_run(asset)` module in Python owns the pattern: it records the start, hands the watermark to the loader or to dbt through `--vars`, and advances the watermark to the recorded start time only after the build and its Great Expectations check both succeed. A failed run leaves the watermark untouched, so the next run picks up the same rows. The downstream dedup-then-merge makes that reprocessing harmless. To reprocess a range, update the watermark row by hand.

We chose this technology-agnostic pattern over dbt's native `is_incremental()` / `max(...) from {{ this }}` for three reasons. First, the watermark must not advance until the post-build data-quality gate passes. Second, one mechanism should cover the Python loader and the dbt models alike. Third, the start time, not the newest row, is what keeps rows that land mid-run from being skipped.

## Consequences

- The pattern is safe only because Airflow serialises writers (single-writer pool). A concurrent writer could commit rows stamped before a run's start time after that run has read its source.
- Object stores keep `LastModified` to the second, so the loader compares against the watermark rounded down to the second. It may re-read a file landed in that second, which is harmless, but it can never skip one.
- Tables that aggregate over their whole history (`fct_rain_pressure_alert`, `fct_rain_forecast_current_hourly`, dimensions) are rebuilt in full, not processed incrementally.
