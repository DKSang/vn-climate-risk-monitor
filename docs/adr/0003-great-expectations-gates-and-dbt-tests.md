# Great Expectations gates the data; dbt tests the transformations

Quality checks answer two different questions. Did the source send what we
expect (a complete forecast run, plausible values, on time)? And did our SQL
do what we meant (unique keys, valid relationships, correct rain windows)? We
use a tool for each. Great Expectations suites in `pipeline/quality.py` run
after Bronze (in the loader) and after Silver (after the clean merge). They
cover volume, completeness, uniqueness, validity and freshness, per forecast
run or per archive month. dbt generic, singular and unit tests run inside every
`dbt build` and cover keys, relationships and business rules.

We considered dbt tests alone: they cannot see Bronze, which is not a dbt model.
We also considered source freshness checks: they would run after the loader has
already appended bad rows. A single Python gate library would duplicate the key
checks dbt declares next to each model. With the split, each check lives next
to the step it protects, and the Silver gate can re-check whole runs, not only
new rows.

## Consequences

- A GX failure raises inside `job_run()`, so the watermark does not move and the
  same rows are retried; a dbt failure stops publication
  ([ADR 0001](0001-watermark-incremental-pattern.md),
  [ADR 0002](0002-publish-a-ducklake-snapshot.md)).
- GX runs in-process with an ephemeral context on pandas batches: no Data Docs
  site or checkpoint store to operate. Failures are reported in
  `meta.job_runs.error`.
- Key uniqueness in Silver is a dbt test, not a GX expectation, so it is not
  declared twice.
