# The report reads a DuckLake snapshot tagged `publish`

A Gold build writes several tables over several commits. A report that reads
Gold while a build runs could mix new facts with old dimensions, or show a
build whose tests then fail. So each successful Gold build marks one snapshot
as published. `publish()` inserts a row into `gold._publications` and calls
`set_commit_message('airflow', 'publish')` in the same transaction. The report
reads `max(snapshot_id)` from `catalog1.snapshots()` where the commit message is
`publish`. It then attaches the lake with `SNAPSHOT_VERSION` and `READ_ONLY`,
so every query of a page sees that one state.

We considered a pointer table in Postgres holding the published snapshot id, and
copying Gold into separate "serving" tables. Both add state that can drift from
the lake. The DuckLake catalog already records every snapshot atomically with
its commit message. Using that record means publication cannot disagree with
the data, and costs one `CALL`.

## Consequences

- Publication happens only after `dbt build` succeeds, so a failed model or test
  leaves the report on the previous publication.
- Forecast and archive publish independently. The newest publication reflects
  the latest state of both, because a snapshot includes every table.
- Snapshot expiry (`lakehouse_maintenance_daily`, 7 days) must stay longer than
  the gap between publications, or the report could lose its snapshot.
