# dbt project

The transformation graph from Silver staging to Gold, run on DuckDB against the
DuckLake catalog `catalog1`. Python loads the `silver.stg_*` tables (declared in
`models/sources.yml`); dbt builds everything after them.

| Folder | Builds |
|---|---|
| `models/silver` | `silver.clean_*`: deduplicated, merged incrementally from the `watermark` var |
| `models/staging` | views over the versioned seeds |
| `models/marts` | Gold dimensions, bridge and facts |
| `tests`, `macros` | singular tests, generic tests and shared SQL |

The pipeline runs dbt in-process (`pipeline/dbt.py`) with one selector per Gold
asset from `selectors.yml`, so a failed model or test stops publication. By hand:

```powershell
uv run dbt parse --project-dir transform --profiles-dir transform
uv run dbt build --project-dir transform --profiles-dir transform --selector gold_forecast
uv run dbt build --project-dir transform --profiles-dir transform --selector gold_archive
```

Seeds are reviewed reference data: `ward_coordinates_seed` lists the 126 wards
and `ward_grid_map_seed` is produced once by `scripts/map_ward_grid.py`. Both
Gold selectors include them.
