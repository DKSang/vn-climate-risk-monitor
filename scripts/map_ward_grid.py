"""One-time tool: ask Open-Meteo which grid cell each ward falls in, per weather model.

Writes transform/seeds/ward_grid_map_seed.csv, which the forecast fetch and dbt read.
Re-run only when the ward list or the models change:

    uv run python scripts/map_ward_grid.py
"""

from __future__ import annotations

import csv
from itertools import batched
from pathlib import Path

import requests

WARDS = Path("transform/seeds/ward_coordinates_seed.csv")
OUTPUT = Path("transform/seeds/ward_grid_map_seed.csv")
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
MODELS = ("ecmwf_ifs", "era5")
PROBE_DAY = "2024-06-01"
FIELDS = (
    "model",
    "ward_code",
    "ward_latitude",
    "ward_longitude",
    "grid_latitude",
    "grid_longitude",
    "elevation_m",
)


def probe(model: str, wards: list[tuple[str, float, float]]) -> list[dict[str, str]]:
    rows = []
    for chunk in batched(wards, 40):
        response = requests.get(
            ARCHIVE_URL,
            params={
                "latitude": ",".join(f"{lat:.6f}" for _, lat, _ in chunk),
                "longitude": ",".join(f"{lon:.6f}" for _, _, lon in chunk),
                "start_date": PROBE_DAY,
                "end_date": PROBE_DAY,
                "models": model,
                "hourly": "precipitation",
            },
            timeout=60,
        )
        response.raise_for_status()
        items = response.json()
        items = items if isinstance(items, list) else [items]
        for item, (code, lat, lon) in zip(items, chunk, strict=True):
            elevation = item.get("elevation")
            rows.append(
                {
                    "model": model,
                    "ward_code": code,
                    "ward_latitude": f"{lat:.6f}",
                    "ward_longitude": f"{lon:.6f}",
                    "grid_latitude": f"{item['latitude']:.6f}",
                    "grid_longitude": f"{item['longitude']:.6f}",
                    "elevation_m": "" if elevation is None else f"{elevation:g}",
                }
            )
    return rows


def main() -> None:
    with WARDS.open(encoding="utf-8") as stream:
        wards = [
            (row["commune_code"], float(row["latitude"]), float(row["longitude"]))
            for row in csv.DictReader(stream)
        ]
    rows = [row for model in MODELS for row in probe(model, wards)]
    with OUTPUT.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(sorted(rows, key=lambda r: (r["model"], r["ward_code"])))
    for model in MODELS:
        cells = {
            (r["grid_latitude"], r["grid_longitude"])
            for r in rows
            if r["model"] == model
        }
        print(f"{model}: {len(wards)} wards -> {len(cells)} grid cells")


if __name__ == "__main__":
    main()
