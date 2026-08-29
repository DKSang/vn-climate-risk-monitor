"""Ánh xạ phường → ô lưới model, lấy từ chính phép snap của Open-Meteo.

── VÌ SAO CẦN ──────────────────────────────────────────────────────────────────
Fetch theo phường là lãng phí: đo 2026-08-28 trên bronze của dự án, 126 phường
Hà Nội chỉ rơi vào **12 ô ERA5** (0,25°; seed ``ward_grid_map_seed``), và mọi
bản sao trong cùng một ô có giá trị GIỐNG HỆT nhau — 0 cặp (ô, giờ) nào lệch.
Tức là trả quota gấp ~10 lần để nhận về cùng một dữ liệu.

Fetch theo ô rồi chiếu ngược về phường ở Silver cho kết quả y hệt với chi phí
~1/10. Nhưng phép chiếu phải dùng ĐÚNG ô mà API đã gán, không phải ô gần nhất
do ta tự tính: bin 0,25° và snap API từng lệch nhau.

Nên module này hỏi thẳng API: gửi toạ độ 126 phường, ghi lại ô nó trả về. Một
lần cho mỗi model, kết quả version-control trong ``transform/seeds/`` để dbt seed
vào warehouse cho Silver join, và để planner đọc khi lập kế hoạch fetch.
"""

from __future__ import annotations

import csv
import json
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from itertools import batched
from pathlib import Path
from urllib.parse import urlencode, urlparse, urlunparse
from urllib.request import Request, urlopen

SEED_PATH = Path("transform/seeds/ward_grid_map_seed.csv")
FIELDNAMES = (
    "model",
    "ward_code",
    "ward_latitude",
    "ward_longitude",
    "grid_latitude",
    "grid_longitude",
    "elevation_m",
)
# Nhiều toạ độ trong một URL; 40 giữ query string ở mức lành mạnh.
PROBE_CHUNK = 40
PROBE_DAY = "2024-06-01"
HTTP_TIMEOUT_S = 60


@dataclass(frozen=True)
class GridCell:
    """Một ô lưới model — đơn vị fetch thật sự."""

    latitude: float
    longitude: float


@dataclass(frozen=True)
class WardGrid:
    """Một phường và ô mà model gán cho nó."""

    model: str
    ward_code: str
    ward_latitude: float
    ward_longitude: float
    grid_latitude: float
    grid_longitude: float
    elevation_m: float | None


def probe_ward_grid(
    *,
    model: str,
    wards: Sequence[tuple[str, float, float]],
    archive_url: str,
    opener=urlopen,
) -> tuple[WardGrid, ...]:
    """Hỏi API xem mỗi phường rơi vào ô nào của ``model``."""
    mapped: list[WardGrid] = []
    for chunk in batched(wards, PROBE_CHUNK):
        params = {
            "latitude": ",".join(f"{lat:.6f}" for _, lat, _ in chunk),
            "longitude": ",".join(f"{lon:.6f}" for _, _, lon in chunk),
            "timezone": "UTC",
            "timeformat": "unixtime",
            "start_date": PROBE_DAY,
            "end_date": PROBE_DAY,
            "models": model,
            "hourly": "precipitation",
        }
        url = urlunparse(
            urlparse(archive_url)._replace(query=urlencode(params, safe=","))
        )
        request = Request(
            url,
            headers={"Accept": "application/json", "User-Agent": "vn-climate-risk-monitor"},
        )
        with opener(request, timeout=HTTP_TIMEOUT_S) as response:
            payload = json.loads(response.read())
        items = payload if isinstance(payload, list) else [payload]
        if len(items) != len(chunk):
            raise RuntimeError(
                f"{model}: gửi {len(chunk)} toạ độ nhưng nhận {len(items)} kết quả"
            )
        for item, (code, lat, lon) in zip(items, chunk, strict=True):
            mapped.append(
                WardGrid(
                    model=model,
                    ward_code=code,
                    ward_latitude=lat,
                    ward_longitude=lon,
                    grid_latitude=float(item["latitude"]),
                    grid_longitude=float(item["longitude"]),
                    elevation_m=(
                        float(item["elevation"]) if item.get("elevation") is not None
                        else None
                    ),
                )
            )
    return tuple(mapped)


# ── seed CSV ──────────────────────────────────────────────────────────────────
def read_seed(path: str | Path = SEED_PATH) -> tuple[WardGrid, ...]:
    csv_path = Path(path)
    if not csv_path.is_file():
        raise FileNotFoundError(
            f"Chưa có bản đồ ô lưới: {csv_path}. Chạy `make map-grid` trước."
        )
    with csv_path.open(encoding="utf-8", newline="") as stream:
        return tuple(
            WardGrid(
                model=row["model"],
                ward_code=row["ward_code"],
                ward_latitude=float(row["ward_latitude"]),
                ward_longitude=float(row["ward_longitude"]),
                grid_latitude=float(row["grid_latitude"]),
                grid_longitude=float(row["grid_longitude"]),
                elevation_m=float(row["elevation_m"]) if row["elevation_m"] else None,
            )
            for row in csv.DictReader(stream)
        )


def write_seed(rows: Sequence[WardGrid], path: str | Path = SEED_PATH) -> None:
    """Ghi đè bản đồ của các model có trong ``rows``, giữ nguyên model khác."""
    csv_path = Path(path)
    replaced = {row.model for row in rows}
    kept = [r for r in _read_or_empty(csv_path) if r.model not in replaced]
    merged = sorted(
        [*kept, *rows], key=lambda r: (r.model, r.ward_code)
    )
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=FIELDNAMES)
        writer.writeheader()
        for row in merged:
            writer.writerow(
                {
                    "model": row.model,
                    "ward_code": row.ward_code,
                    "ward_latitude": f"{row.ward_latitude:.6f}",
                    "ward_longitude": f"{row.ward_longitude:.6f}",
                    "grid_latitude": f"{row.grid_latitude:.6f}",
                    "grid_longitude": f"{row.grid_longitude:.6f}",
                    "elevation_m": "" if row.elevation_m is None else f"{row.elevation_m:g}",
                }
            )


def _read_or_empty(path: Path) -> Iterator[WardGrid]:
    if path.is_file():
        yield from read_seed(path)


def cells_for(model: str, path: str | Path = SEED_PATH) -> tuple[GridCell, ...]:
    """Các ô riêng biệt của ``model`` — đây mới là thứ cần fetch."""
    seen = {
        GridCell(row.grid_latitude, row.grid_longitude)
        for row in read_seed(path)
        if row.model == model
    }
    if not seen:
        raise RuntimeError(
            f"Bản đồ ô lưới chưa có model {model!r}. Chạy `make map-grid MODEL={model}`."
        )
    return tuple(sorted(seen, key=lambda c: (c.latitude, c.longitude)))
