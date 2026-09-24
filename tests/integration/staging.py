"""Write rows to silver.stg_open_meteo_forecast the way the auto loader would."""

from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd

from pipeline import lake
from pipeline.open_meteo.forecast import grid_cells


def stage(
    run: datetime,
    *,
    rain: float,
    inserted_at: datetime,
    file: str,
    weather_code: float = 61.0,
) -> None:
    """Append one full forecast run to staging, as the loader would."""
    rows = pd.DataFrame(
        [
            {
                "forecast_run": run,
                "grid_latitude": float(lat),
                "grid_longitude": float(lon),
                "valid_at": run + timedelta(hours=h),
                "precipitation": rain,
                "rain": rain,
                "showers": 0.0,
                "precipitation_probability": 40.0,
                "weather_code": weather_code,
                "_source_file": file,
                "_inserted_at": inserted_at,
            }
            for lat, lon in grid_cells()
            for h in range(72)
        ]
    )
    with lake.connect() as con:
        con.register("new_rows", rows)
        con.execute("INSERT INTO silver.stg_open_meteo_forecast SELECT * FROM new_rows")
