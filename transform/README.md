# dbt project

This directory contains the dbt/DuckDB transformation graph. The autoloader
writes the two physical Silver staging tables; dbt reads them directly and builds
intermediate and Gold models.

## Parse and build

```powershell
uv run dbt parse --project-dir transform --profiles-dir transform
uv run dbt build --project-dir transform --profiles-dir transform --select tag:forecast
uv run dbt build --project-dir transform --profiles-dir transform --select tag:archive
```

The `forecast` and `archive` model tags select their shared reference models and
the corresponding intermediate and mart models. `dbt build` is the quality
gate: a failed generic or singular test prevents processing publication.

## Seeds

Reference seeds are static data products. Bootstrap loads them once; the monthly
archive DAG does not rerun `dbt seed`. After an intentional seed change, run:

```powershell
uv run dbt seed --project-dir transform --profiles-dir transform
uv run dbt build --project-dir transform --profiles-dir transform --select dim_ward dim_grid bridge_ward_grid
```

One-time source scraping, geocoding, and GeoJSON generation live under
`tools/`; their outputs are reviewed and committed as seeds/reference data.
