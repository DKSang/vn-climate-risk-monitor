"""Nguồn Open-Meteo: việc còn thiếu → ``fetch.land`` lên MinIO, chạy song song.

CLI: ``uv run fetch-open-meteo forecast|archive|map-grid``. Load: ``uv run load-sources``.

── ARCHIVE FETCH THEO Ô LƯỚI, KHÔNG THEO PHƯỜNG ────────────────────────────────
Đo 2026-08-28: 126 phường Hà Nội chỉ rơi vào 12 ô ERA5, và mọi bản sao trong cùng
ô có giá trị GIỐNG HỆT (0 cặp (ô, giờ) nào lệch). Fetch theo phường tiêu quota
gấp ~9,7 lần mà không thêm một bit thông tin nào. Ánh xạ phường→ô nằm ở
:mod:`vn_climate_risk_monitor.grid`, phép chiếu ngược làm ở Silver.

── HAI MODEL THEO THỜI KỲ ──────────────────────────────────────────────────────
``era5``     0,25° (12 ô), 1940→nay. Thô, nhưng là chuỗi lịch sử sâu duy nhất
             (IFS không có dữ liệu trước 2017).
``ecmwf_ifs`` ~9km (48 ô), **2017→nay**. Cho tín hiệu khác nhau THẬT giữa các
             phường: cùng ngày mưa, ba phường mà ERA5 gộp thành một chuỗi 9,3mm
             thì IFS trả 105,0 / 137,9 / 116,2 mm.

Không dùng ``era5_land`` (không có biến mưa nào) và ``era5_seamless`` (toạ độ mịn
0,1° nhưng giá trị mưa vẫn là ERA5 0,25° dán lại — trùng lặp bị GIẤU đi thay vì
lộ ra, làm hỏng dedup theo ô ở Silver).
"""

from __future__ import annotations

import argparse
import calendar
import math
import time
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from itertools import batched
from pathlib import Path
from urllib.parse import urlencode, urlparse, urlunparse

import duckdb
from minio import Minio
from minio.error import S3Error

from fetch import FetchTask, land, run_fetch_pool
from vn_climate_risk_monitor import grid
from vn_climate_risk_monitor.config import OpenMeteoSettings, load_settings
from vn_climate_risk_monitor.lakehouse import get_connection
from vn_climate_risk_monitor.storage import ensure_bucket, get_minio_client

FORECAST_PREFIX = "bronze/files/open_meteo/forecast/incremental"
FORECAST_FIELDS = "precipitation,rain,showers,precipitation_probability,weather_code"
ARCHIVE_FIELDS = (
    "precipitation,rain,weather_code,soil_moisture_0_to_7cm,soil_moisture_7_to_28cm"
)

#: Mốc đầu tiên ``ecmwf_ifs`` có dữ liệu — dò 2026-08-28: 2016 mọi quý đều NULL,
#: 2017 mọi quý đều có.
IFS_START = date(2017, 1, 1)


@dataclass(frozen=True)
class ArchiveModel:
    """Một model archive: tên, nơi land, bảng bronze để biết đã có tháng nào."""

    name: str
    prefix: str


# Hai model chia CHUNG một bảng staging, phân biệt bằng cột `weather_model`.
# Trước 2026-09-03 mỗi model một bảng, dù schema y hệt nhau.
STAGING_HOURLY = "catalog1.silver.stg_weather_hourly"

ERA5 = ArchiveModel(
    name="era5",
    # Giữ nguyên prefix cũ: dữ liệu ward-based 2000–2013 đã nằm đây và cùng schema.
    prefix="bronze/files/open_meteo/historical_weather_hourly/backfill",
)
IFS = ArchiveModel(
    name="ecmwf_ifs",
    prefix="bronze/files/open_meteo/historical_weather_hourly/ifs",
)
ARCHIVE_MODELS = (ERA5, IFS)


def model_for_month(month: date) -> ArchiveModel:
    """era5 trước 2017, ecmwf_ifs từ 2017. IFS không có dữ liệu trước 2017."""
    return ERA5 if month < IFS_START else IFS


Row = dict[str, str]


@dataclass(frozen=True)
class Location:
    ward_code: str
    latitude: float
    longitude: float


def load_locations(connection: duckdb.DuckDBPyConnection) -> tuple[Location, ...]:
    rows = connection.execute(
        "SELECT ward_code, latitude, longitude FROM gold.dim_hanoi_ward ORDER BY ward_key"
    ).fetchall()
    if not rows:
        raise RuntimeError("gold.dim_hanoi_ward rỗng — chạy `make transform` trước")
    return tuple(Location(str(c), float(lat), float(lon)) for c, lat, lon in rows)


def effective_call_units(*, locations: int, days: int, variables: int) -> int:
    return math.ceil(locations * max(1.0, days / 14) * max(1.0, variables / 10))


def months_between(start: date, end: date) -> Iterator[date]:
    current = start.replace(day=1)
    while current <= end:
        yield current
        current = (current.replace(day=28) + timedelta(days=4)).replace(day=1)


def days_in_month(month: date) -> int:
    return calendar.monthrange(month.year, month.month)[1]


def covered_months(
    connection: duckdb.DuckDBPyConnection,
    table: str = STAGING_HOURLY,
    *,
    weather_model: str | None = None,
) -> frozenset[date]:
    """Tháng đã ĐỦ giờ trong staging — bỏ qua khi lập kế hoạch.

    Cố ý hỏi bảng staging chứ không đếm object trên MinIO. Cách fetch đã đổi từ
    theo-phường (6 file/tháng) sang theo-ô (1–2 file/tháng), nên so khớp
    ``response_NNN.json`` sẽ hiểu sai: một tháng ward-based đủ 6 file trông giống
    một tháng grid-based đã xong, và ngược lại một tháng grid-based đủ dữ liệu
    lại trông như thiếu file. Số giờ có thật trong bảng thì không mơ hồ.
    """
    try:
        # Lọc theo model là BẮT BUỘC từ khi hai model dùng chung một bảng:
        # không lọc thì tháng của era5 làm ecmwf_ifs trông như đã phủ đủ.
        predicate = "" if weather_model is None else "WHERE weather_model = ?"
        parameters = [] if weather_model is None else [weather_model]
        rows = connection.execute(
            f"""
            SELECT date_trunc('month', valid_time_utc) AS month_start,
                   count(DISTINCT valid_time_utc)      AS hours
            FROM {table}
            {predicate}
            GROUP BY 1
            """,
            parameters,
        ).fetchall()
    except duckdb.Error:
        return frozenset()  # bảng chưa tồn tại: chưa có gì được phủ
    return frozenset(
        month_start.date().replace(day=1)
        for month_start, hours in rows
        if hours >= days_in_month(month_start.date()) * 24
    )


def existing_basenames(client: Minio, bucket: str, prefix: str) -> set[str]:
    return {
        obj.object_name.rsplit("/", 1)[-1]
        for obj in client.list_objects(bucket, prefix=f"{prefix}/", recursive=True)
    }


def request_url(base: str, params: dict[str, str]) -> str:
    return urlunparse(urlparse(base)._replace(query=urlencode(params, safe=",")))


def base_params(points: Sequence[object]) -> dict[str, str]:
    """Toạ độ cho một request. Nhận Location hoặc GridCell — chỉ cần lat/lon."""
    return {
        "latitude": ",".join(f"{p.latitude:.6f}" for p in points),  # type: ignore[attr-defined]
        "longitude": ",".join(f"{p.longitude:.6f}" for p in points),  # type: ignore[attr-defined]
        "timezone": "UTC",
        "timeformat": "unixtime",
    }


def tasks_for_prefix(
    *,
    points: Sequence[object],
    batch_size: int,
    prefix: str,
    url: str,
    extra_params: dict[str, str],
    existing: set[str],
    run: str,
    days: int,
    variables: int,
) -> list[FetchTask]:
    """Một FetchTask cho mỗi lô toạ độ còn thiếu file."""
    tasks: list[FetchTask] = []
    for index, batch in enumerate(batched(points, batch_size)):
        name = f"response_{index:03}.json"
        if name in existing:
            continue
        tasks.append(
            FetchTask(
                url=request_url(url, base_params(batch) | extra_params),
                key=f"{prefix}/{run}/{name}",
                # Tính theo ĐỘ DÀI LÔ THẬT, không theo batch_size danh nghĩa: lô
                # cuối thường ngắn hơn và trước đây bị tính (và bị nghỉ) như lô đầy.
                units=effective_call_units(
                    locations=len(batch), days=days, variables=variables
                ),
            )
        )
    return tasks


def month_params(month: date, model: ArchiveModel) -> tuple[str, dict[str, str]]:
    last = month.replace(day=days_in_month(month))
    prefix = f"{model.prefix}/year={month.year:04}/month={month.month:02}"
    return prefix, {
        "hourly": ARCHIVE_FIELDS,
        "models": model.name,
        "start_date": month.isoformat(),
        "end_date": last.isoformat(),
    }


def slot_params(slot: datetime, settings: OpenMeteoSettings) -> tuple[str, dict[str, str]]:
    prefix = f"{FORECAST_PREFIX}/{slot:%Y/%m/%d/%H}"
    return prefix, {
        "hourly": FORECAST_FIELDS,
        "models": settings.forecast_model,
        "forecast_hours": str(settings.forecast_hours),
    }


def archive_tasks(
    *,
    months: Sequence[date],
    settings: OpenMeteoSettings,
    client: Minio,
    bucket: str,
    run: str,
    covered: dict[str, frozenset[date]],
    seed_path: str | Path = grid.SEED_PATH,
) -> list[FetchTask]:
    """Kế hoạch archive: mỗi tháng đi theo model của thời kỳ đó, fetch theo ô."""
    cells = {m.name: grid.cells_for(m.name, seed_path) for m in ARCHIVE_MODELS}
    variables = len(ARCHIVE_FIELDS.split(","))
    tasks: list[FetchTask] = []
    for month in months:
        model = model_for_month(month)
        if month in covered.get(model.name, frozenset()):
            continue
        prefix, extra = month_params(month, model)
        tasks.extend(
            tasks_for_prefix(
                points=cells[model.name],
                batch_size=settings.location_batch_size,
                prefix=prefix,
                url=settings.archive_url,
                extra_params=extra,
                existing=existing_basenames(client, bucket, prefix),
                run=run,
                days=days_in_month(month),
                variables=variables,
            )
        )
    return tasks


def forecast_tasks(
    *,
    slots: Sequence[datetime],
    locations: Sequence[Location],
    settings: OpenMeteoSettings,
    client: Minio,
    bucket: str,
    run: str,
) -> list[FetchTask]:
    """Forecast vẫn fetch THEO PHƯỜNG — cố ý.

    Lưới ``best_match`` mịn hơn (48 ô cho 126 phường, chỉ trùng 2,6×) và mesh của
    nó ĐỔI theo thời gian khi Open-Meteo chuyển model nền, nên danh sách ô cache
    cứng sẽ mục. Forecast cũng chỉ ~6 request/giờ nên không phải nút thắt.
    """
    variables = len(FORECAST_FIELDS.split(","))
    days = max(1, settings.forecast_hours // 24)
    tasks: list[FetchTask] = []
    for slot in slots:
        prefix, extra = slot_params(slot, settings)
        tasks.extend(
            tasks_for_prefix(
                points=locations,
                batch_size=settings.location_batch_size,
                prefix=prefix,
                url=settings.forecast_url,
                extra_params=extra,
                existing=existing_basenames(client, bucket, prefix),
                run=run,
                days=days,
                variables=variables,
            )
        )
    return tasks


# ── CLI ───────────────────────────────────────────────────────────────────────
def _run_pool(tasks: Sequence[FetchTask], settings: OpenMeteoSettings, minio_cfg) -> int:
    client = get_minio_client(minio_cfg)
    ensure_bucket(client, minio_cfg.bucket)
    print(
        f"Land: {len(tasks)} request, {settings.fetch_workers} luồng",
        flush=True,
    )
    started = time.monotonic()
    result = run_fetch_pool(
        tasks,
        land_one=lambda task: land(client, minio_cfg.bucket, task.url, task.key),
        workers=settings.fetch_workers,
    )
    elapsed = time.monotonic() - started
    print(f"Xong: {result.landed} file đã land trong {elapsed / 60:.1f} phút.")
    if result.cancelled:
        print(f"   {result.cancelled} request bị bỏ sau khi lô dừng.")
    for failure in result.failures:
        print(f"⛔ {failure}")
    if not result.ok:
        print("   Không mất dữ liệu: chạy lại, các file đã land sẽ tự bị bỏ qua.")
        return 2
    print("Bước tiếp theo: make load")
    return 0


def _cmd_map_grid(args, settings) -> int:
    """Probe API để chốt ô lưới của từng model rồi ghi seed CSV."""
    connection = get_connection(attach_bronze=False, read_only=True)
    try:
        wards = load_locations(connection)
    finally:
        connection.close()
    models = args.models or [m.name for m in ARCHIVE_MODELS]
    print(f"Probe {len(wards)} phường × {len(models)} model")
    if not args.execute:
        print("DRY RUN — thêm --execute để chạy thật")
        return 0
    mapped: list[grid.WardGrid] = []
    for name in models:
        rows = grid.probe_ward_grid(
            model=name,
            wards=[(w.ward_code, w.latitude, w.longitude) for w in wards],
            archive_url=settings.open_meteo.archive_url,
        )
        cells = {(r.grid_latitude, r.grid_longitude) for r in rows}
        print(f"   {name}: {len(rows)} phường → {len(cells)} ô lưới")
        mapped.extend(rows)
    grid.write_seed(mapped)
    print(f"Đã ghi {grid.SEED_PATH}. Bước tiếp theo: make seed && make fetch-archive")
    return 0


def _cmd_archive(args, settings) -> int:
    open_meteo = settings.open_meteo
    months = list(months_between(args.start, args.end))
    connection = get_connection()
    try:
        covered = {
            m.name: covered_months(connection, weather_model=m.name)
            for m in ARCHIVE_MODELS
        }
    finally:
        connection.close()
    client = get_minio_client(settings.minio)
    tasks = archive_tasks(
        months=months,
        settings=open_meteo,
        client=client,
        bucket=settings.minio.bucket,
        run=f"run_{datetime.now(UTC):%Y%m%dT%H%M%S}",
        covered=covered,
    )
    by_model: dict[str, int] = {}
    for month in months:
        name = model_for_month(month).name
        by_model[name] = by_model.get(name, 0) + (0 if month in covered.get(name, ()) else 1)
    print(f"Kế hoạch archive {args.start}→{args.end}: {len(months)} tháng")
    for model in ARCHIVE_MODELS:
        done = len([m for m in months if model_for_month(m) is model]) - by_model.get(model.name, 0)
        print(f"   {model.name:<10} còn {by_model.get(model.name, 0)} tháng (đã đủ {done})")
    print(f"   {len(tasks)} request / {sum(t.units for t in tasks):,} đơn vị")
    if not tasks:
        print("Không còn gì để land. Bước tiếp theo: make load")
        return 0
    if not args.execute:
        print("DRY RUN — thêm --execute để chạy thật")
        return 0
    return _run_pool(tasks, open_meteo, settings.minio)


def _cmd_forecast(args, settings) -> int:
    open_meteo = settings.open_meteo
    connection = get_connection(attach_bronze=False, read_only=True)
    try:
        locations = load_locations(connection)
    finally:
        connection.close()
    if args.limit:
        locations = locations[: args.limit]
    slot = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    client = get_minio_client(settings.minio)
    tasks = forecast_tasks(
        slots=[slot],
        locations=locations,
        settings=open_meteo,
        client=client,
        bucket=settings.minio.bucket,
        run=f"run_{datetime.now(UTC):%Y%m%dT%H%M%S}",
    )
    print(
        f"Kế hoạch forecast {slot:%Y-%m-%d %H}h: {len(locations)} phường, "
        f"{len(tasks)} request / {sum(t.units for t in tasks):,} đơn vị"
    )
    if not tasks:
        print("Không còn gì để land. Bước tiếp theo: make load")
        return 0
    if not args.execute:
        print("DRY RUN — thêm --execute để chạy thật")
        return 0
    return _run_pool(tasks, open_meteo, settings.minio)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    p_fc = sub.add_parser("forecast", help="Land dự báo cho slot giờ hiện tại")
    p_fc.add_argument("--limit", type=int, help="Chỉ N phường đầu (canary)")

    p_ar = sub.add_parser("archive", help="Land lịch sử theo khoảng tháng")
    p_ar.add_argument("--start", type=date.fromisoformat, default=date(2000, 1, 1))
    p_ar.add_argument("--end", type=date.fromisoformat, default=datetime.now(UTC).date())

    p_mg = sub.add_parser("map-grid", help="Probe ô lưới của model rồi ghi seed CSV")
    p_mg.add_argument("--models", nargs="*", help="Mặc định: mọi model archive")

    for sub_parser in (p_fc, p_ar, p_mg):
        sub_parser.add_argument(
            "--execute", action="store_true", help="Mặc định chỉ in kế hoạch"
        )
    args = parser.parse_args()
    settings = load_settings()

    handler = {
        "forecast": _cmd_forecast,
        "archive": _cmd_archive,
        "map-grid": _cmd_map_grid,
    }[args.command]
    try:
        exit_code = handler(args, settings)
    except (S3Error, OSError, RuntimeError) as error:
        print(f"⛔ Dừng hẳn ({type(error).__name__}: {error}).")
        raise SystemExit(2) from error
    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
