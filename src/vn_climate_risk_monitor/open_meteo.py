"""Nguồn Open-Meteo: row còn thiếu → ``fetch.land`` lên MinIO.

CLI: ``uv run fetch-open-meteo forecast|archive``. Load: ``uv run load-sources``.
"""

from __future__ import annotations

import argparse
import math
import time
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from itertools import batched
from urllib.parse import urlencode, urlparse, urlunparse

import duckdb
from minio import Minio
from minio.error import S3Error

from fetch import land
from vn_climate_risk_monitor.config import OpenMeteoSettings, load_settings
from vn_climate_risk_monitor.lakehouse import get_connection
from vn_climate_risk_monitor.storage import ensure_bucket, get_minio_client

FORECAST_PREFIX = "bronze/files/open_meteo/forecast/incremental"
ARCHIVE_PREFIX = "bronze/files/open_meteo/historical_weather_hourly/backfill"
FORECAST_FIELDS = "precipitation,rain,showers,precipitation_probability,weather_code"
ARCHIVE_FIELDS = (
    "precipitation,rain,weather_code,soil_moisture_0_to_7cm,soil_moisture_7_to_28cm"
)

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


def existing_basenames(client: Minio, bucket: str, prefix: str) -> set[str]:
    return {
        obj.object_name.rsplit("/", 1)[-1]
        for obj in client.list_objects(bucket, prefix=f"{prefix}/", recursive=True)
    }


def request_url(base: str, params: dict[str, str]) -> str:
    return urlunparse(urlparse(base)._replace(query=urlencode(params, safe=",")))


def base_params(locations: Sequence[Location]) -> dict[str, str]:
    return {
        "latitude": ",".join(f"{loc.latitude:.6f}" for loc in locations),
        "longitude": ",".join(f"{loc.longitude:.6f}" for loc in locations),
        "timezone": "UTC",
        "timeformat": "unixtime",
    }


def rows_for_prefix(
    *,
    locations: Sequence[Location],
    batch_size: int,
    prefix: str,
    url: str,
    extra_params: dict[str, str],
    existing: set[str],
    run: str,
) -> list[Row]:
    rows: list[Row] = []
    for index, batch in enumerate(batched(locations, batch_size)):
        name = f"response_{index:03}.json"
        if name in existing:
            continue
        rows.append(
            {
                "url": request_url(url, base_params(batch) | extra_params),
                "key": f"{prefix}/{run}/{name}",
            }
        )
    return rows


def month_params(
    month: date, settings: OpenMeteoSettings
) -> tuple[str, dict[str, str]]:
    last = (month.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(
        days=1
    )
    prefix = f"{ARCHIVE_PREFIX}/year={month.year:04}/month={month.month:02}"
    return prefix, {
        "hourly": ARCHIVE_FIELDS,
        "models": settings.archive_model,
        "start_date": month.isoformat(),
        "end_date": last.isoformat(),
    }


def slot_params(
    slot: datetime, settings: OpenMeteoSettings
) -> tuple[str, dict[str, str]]:
    prefix = f"{FORECAST_PREFIX}/{slot:%Y/%m/%d/%H}"
    return prefix, {
        "hourly": FORECAST_FIELDS,
        "models": settings.forecast_model,
        "forecast_hours": str(settings.forecast_hours),
    }


def missing_rows(
    *,
    targets: Sequence[date | datetime],
    locations: Sequence[Location],
    settings: OpenMeteoSettings,
    client: Minio,
    bucket: str,
    run: str,
    dataset: str,
) -> list[Row]:
    url = settings.forecast_url if dataset == "forecast" else settings.archive_url
    rows: list[Row] = []
    for target in targets:
        if dataset == "forecast":
            prefix, extra = slot_params(target, settings)  # type: ignore[arg-type]
        else:
            prefix, extra = month_params(target, settings)  # type: ignore[arg-type]
        rows.extend(
            rows_for_prefix(
                locations=locations,
                batch_size=settings.location_batch_size,
                prefix=prefix,
                url=url,
                extra_params=extra,
                existing=existing_basenames(client, bucket, prefix),
                run=run,
            )
        )
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    p_fc = sub.add_parser("forecast", help="Land dự báo cho slot giờ hiện tại")
    p_ar = sub.add_parser("archive", help="Land lịch sử theo khoảng tháng")
    p_ar.add_argument("--start", type=date.fromisoformat, default=date(2000, 1, 1))
    p_ar.add_argument(
        "--end", type=date.fromisoformat, default=datetime.now(UTC).date()
    )
    for sub_parser in (p_fc, p_ar):
        sub_parser.add_argument("--limit", type=int, help="Chỉ N phường đầu (canary)")
        sub_parser.add_argument(
            "--execute", action="store_true", help="Mặc định chỉ in kế hoạch"
        )
    args = parser.parse_args()
    settings = load_settings()
    open_meteo = settings.open_meteo

    connection = get_connection(attach_bronze=False, read_only=True)
    try:
        locations = load_locations(connection)
    finally:
        connection.close()
    if args.limit:
        locations = locations[: args.limit]

    if args.command == "forecast":
        targets: list = [datetime.now(UTC).replace(minute=0, second=0, microsecond=0)]
        days_each = max(1, open_meteo.forecast_hours // 24)
        variables = len(FORECAST_FIELDS.split(","))
    else:
        targets = list(months_between(args.start, args.end))
        days_each = 31
        variables = len(ARCHIVE_FIELDS.split(","))
    per_run = -(-len(locations) // open_meteo.location_batch_size)
    units_total = sum(
        effective_call_units(locations=len(batch), days=days_each, variables=variables)
        for _ in targets
        for batch in batched(locations, open_meteo.location_batch_size)
    )
    print(
        f"Kế hoạch {args.command}: {len(locations)} phường, {per_run} request/lần, "
        f"{len(targets)} lần → {per_run * len(targets)} request"
    )
    print(
        f"  Chi phí hạn mức: ~{units_total:,} đơn vị "
        f"(Open-Meteo tính theo toạ độ × thời gian × số biến, KHÔNG phải 1/request)"
    )
    if units_total > 10000:
        print(
            f"  ⚠️  VƯỢT hạn mức free tier ~10.000 đơn vị/ngày. "
            f"Cần ~{-(-units_total // 10000)} ngày, hoặc chia nhỏ khoảng --start/--end."
        )
    if not args.execute:
        print("DRY RUN — thêm --execute để chạy thật")
        return

    client = get_minio_client(settings.minio)
    ensure_bucket(client, settings.minio.bucket)
    rows = missing_rows(
        targets=targets,
        locations=locations,
        settings=open_meteo,
        client=client,
        bucket=settings.minio.bucket,
        run=f"run_{datetime.now(UTC):%Y%m%dT%H%M%S}",
        dataset=args.command,
    )
    print(f"Lookup: {len(rows)} row còn thiếu")
    if not rows:
        print("Xong: 0 file đã land. Bước tiếp theo: make load")
        return

    units_each = effective_call_units(
        locations=open_meteo.location_batch_size, days=days_each, variables=variables
    )
    pause_s = 3600 * units_each / open_meteo.max_effective_calls_per_hour
    print(f"Land: {len(rows)} row, cách nhau {pause_s:.0f}s", flush=True)

    landed = 0
    try:
        for row in rows:
            if landed and pause_s:
                time.sleep(pause_s)
            print(f"  [{landed + 1}/{len(rows)}] GET → {row['key']}", flush=True)
            land(client, settings.minio.bucket, row["url"], row["key"])
            landed += 1
    except (S3Error, OSError, RuntimeError) as error:
        print(f"⛔ Land thất bại — dừng hẳn ({error}).")
        print("   Không mất dữ liệu: chạy lại sau khi quota reset,")
        print("   các file đã land sẽ tự bị bỏ qua.")
        raise SystemExit(2) from error
    print(f"Xong: {landed} file đã land. Bước tiếp theo: make load")


if __name__ == "__main__":
    main()
