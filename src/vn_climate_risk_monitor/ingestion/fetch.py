"""Lấy dữ liệu Open-Meteo và land JSON nguyên vẹn lên MinIO.

CHỈ fetch và ghi file. Không parse, không đăng ký checkpoint, không ghi bảng.
Việc phát hiện file mới và nạp vào bronze do :mod:`autoloader` đảm nhiệm —
nó liệt kê storage nên không cần biết ai đã ghi ra file.

Tách bạch như vậy sửa được một điểm yếu của thiết kế cũ: trước đây collector ghi
file rồi tự đăng ký vào control plane trong cùng luồng, nên nếu chết giữa hai
bước thì file thành mồ côi vĩnh viễn. Nay discovery sẽ nhặt được ở lần chạy sau.
"""

from __future__ import annotations

import argparse
import io
import json
import math
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

import duckdb
from minio import Minio
from requests import Session

from autoloader.http import EffectiveCallPacer, build_http_session
from vn_climate_risk_monitor.config import OpenMeteoSettings, load_settings
from vn_climate_risk_monitor.lakehouse import get_connection
from vn_climate_risk_monitor.storage import ensure_bucket, get_minio_client

FORECAST_PREFIX = "bronze/files/open_meteo/forecast/incremental"
ARCHIVE_PREFIX = "bronze/files/open_meteo/historical_weather_hourly/backfill"

FORECAST_FIELDS = "precipitation,rain,showers,precipitation_probability,weather_code"
ARCHIVE_FIELDS = (
    "precipitation,rain,weather_code,soil_moisture_0_to_7cm,soil_moisture_7_to_28cm"
)


@dataclass(frozen=True)
class Location:
    ward_code: str
    latitude: float
    longitude: float


def load_locations(connection: duckdb.DuckDBPyConnection) -> tuple[Location, ...]:
    """126 phường/xã Hà Nội từ gold.dim_hanoi_ward, thứ tự ổn định."""
    rows = connection.execute(
        "SELECT ward_code, latitude, longitude FROM gold.dim_hanoi_ward ORDER BY ward_key"
    ).fetchall()
    if not rows:
        raise RuntimeError("gold.dim_hanoi_ward rỗng — chạy `make transform` trước")
    return tuple(Location(str(c), float(lat), float(lon)) for c, lat, lon in rows)


def effective_call_units(*, locations: int, days: int, variables: int) -> int:
    """Trọng số một request theo cách Open-Meteo tính hạn mức.

    Open-Meteo KHÔNG đếm mỗi HTTP request là 1: chi phí tỉ lệ với số toạ độ, độ
    dài khoảng thời gian và số biến. Một request archive 25 phường × 1 tháng
    nặng khoảng 54 đơn vị, không phải 1.

    Bỏ qua trọng số này là lý do backfill dễ bị 429 rồi khoá: 1.770 request tưởng
    nhẹ nhưng thực chất ~95.000 đơn vị, gấp 9 lần hạn mức 10.000/ngày của free tier.

    Công thức giữ nguyên từ collector cũ (đã dùng để chạy 25 tháng đầu):
        units = locations × max(1, days/14) × max(1, variables/10)
    """
    time_factor = max(1.0, days / 14)
    variable_factor = max(1.0, variables / 10)
    return math.ceil(locations * time_factor * variable_factor)


def _batches(items: Sequence[Location], size: int) -> Iterator[Sequence[Location]]:
    for start in range(0, len(items), size):
        yield items[start : start + size]


def _base_params(locations: Sequence[Location]) -> dict[str, str]:
    return {
        "latitude": ",".join(f"{loc.latitude:.6f}" for loc in locations),
        "longitude": ",".join(f"{loc.longitude:.6f}" for loc in locations),
        "timezone": "UTC",
        # Thiếu tham số này Open-Meteo trả chuỗi ISO thay vì epoch số, trộn hai
        # định dạng trong cùng một cột JSON sẽ làm SQL transform lỗi ép kiểu.
        "timeformat": "unixtime",
    }


def _fetch(
    session: Session,
    pacer: EffectiveCallPacer,
    url: str,
    params: dict[str, str],
    timeout: int,
    call_units: int = 1,
) -> list[dict]:
    pacer.wait(call_units)
    response = session.get(url, params=params, timeout=timeout)
    response.raise_for_status()
    payload = response.json()
    body = payload if isinstance(payload, list) else [payload]
    for entry in body:
        if isinstance(entry, dict) and entry.get("error"):
            raise RuntimeError(f"Open-Meteo trả lỗi: {entry.get('reason', 'unknown')}")
    return body


def _write(client: Minio, bucket: str, key: str, payload: object) -> None:
    body = json.dumps(payload, ensure_ascii=False).encode()
    client.put_object(
        bucket, key, io.BytesIO(body), len(body), content_type="application/json"
    )


def _existing_keys(client: Minio, bucket: str, prefix: str) -> set[str]:
    return {
        obj.object_name
        for obj in client.list_objects(bucket, prefix=f"{prefix}/", recursive=True)
    }


def _land(
    *,
    locations: Sequence[Location],
    settings: OpenMeteoSettings,
    session: Session,
    pacer: EffectiveCallPacer,
    client: Minio,
    bucket: str,
    prefix: str,
    extra_params: dict[str, str],
    url: str,
    overwrite: bool,
    days: int,
) -> int:
    # Bỏ qua theo TỪNG FILE, không theo cả prefix: crash giữa chừng tháng/giờ để
    # lại vài file rồi chạy lại phải đi tiếp phần thiếu, không skip cả tháng.
    existing: set[str] = set()
    if not overwrite:
        existing = _existing_keys(client, bucket, prefix)
    written = 0
    variables = len(extra_params["hourly"].split(","))
    for index, batch in enumerate(_batches(locations, settings.location_batch_size)):
        key = f"{prefix}/response_{index:03}.json"
        if key in existing:
            continue
        units = effective_call_units(
            locations=len(batch), days=days, variables=variables
        )
        payload = _fetch(
            session,
            pacer,
            url,
            _base_params(batch) | extra_params,
            settings.request_timeout_seconds,
            call_units=units,
        )
        _write(client, bucket, key, payload)
        written += 1
    print(
        f"  {prefix} -> {written} file mới"
        + (f", bỏ qua {len(existing)} file cũ" if existing else "")
    )
    return written


def months_between(start: date, end: date) -> Iterator[date]:
    current = start.replace(day=1)
    while current <= end:
        yield current
        current = (current.replace(day=28) + timedelta(days=4)).replace(day=1)


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
        sub_parser.add_argument("--overwrite", action="store_true")
        sub_parser.add_argument(
            "--execute", action="store_true", help="Mặc định chỉ in kế hoạch"
        )
    args = parser.parse_args()
    settings = load_settings()

    connection = get_connection(attach_bronze=False, read_only=True)
    try:
        locations = load_locations(connection)
    finally:
        connection.close()
    if args.limit:
        locations = locations[: args.limit]

    if args.command == "forecast":
        targets: list = [datetime.now(UTC).replace(minute=0, second=0, microsecond=0)]
    else:
        targets = list(months_between(args.start, args.end))
    batch_size = settings.open_meteo.location_batch_size
    per_run = -(-len(locations) // batch_size)
    if args.command == "forecast":
        days_each = max(1, settings.open_meteo.forecast_hours // 24)
        variables = len(FORECAST_FIELDS.split(","))
    else:
        days_each = 31
        variables = len(ARCHIVE_FIELDS.split(","))
    units_total = sum(
        effective_call_units(
            locations=min(batch_size, len(locations) - i * batch_size),
            days=days_each,
            variables=variables,
        )
        for i in range(per_run)
    ) * len(targets)
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
    pacer = EffectiveCallPacer(
        calls_per_minute=settings.open_meteo.max_effective_calls_per_minute,
        calls_per_hour=settings.open_meteo.max_effective_calls_per_hour,
    )
    total = 0
    with build_http_session(max_attempts=settings.open_meteo.max_attempts) as session:
        shared = {
            "locations": locations,
            "settings": settings.open_meteo,
            "session": session,
            "pacer": pacer,
            "client": client,
            "bucket": settings.minio.bucket,
            "overwrite": args.overwrite,
        }
        for target in targets:
            if args.command == "forecast":
                total += _land(
                    prefix=f"{FORECAST_PREFIX}/{target:%Y/%m/%d/%H}",
                    url=settings.open_meteo.forecast_url,
                    extra_params={
                        "hourly": FORECAST_FIELDS,
                        "models": settings.open_meteo.forecast_model,
                        "forecast_hours": str(settings.open_meteo.forecast_hours),
                    },
                    days=max(1, settings.open_meteo.forecast_hours // 24),
                    **shared,
                )
            else:
                last = (target.replace(day=28) + timedelta(days=4)).replace(
                    day=1
                ) - timedelta(days=1)
                total += _land(
                    prefix=f"{ARCHIVE_PREFIX}/year={target.year:04}/month={target.month:02}",
                    url=settings.open_meteo.archive_url,
                    extra_params={
                        "hourly": ARCHIVE_FIELDS,
                        "models": settings.open_meteo.archive_model,
                        "start_date": target.isoformat(),
                        "end_date": last.isoformat(),
                    },
                    days=(last - target).days + 1,
                    **shared,
                )
    print(f"Xong: {total} file đã land. Bước tiếp theo: make load")


if __name__ == "__main__":
    main()
