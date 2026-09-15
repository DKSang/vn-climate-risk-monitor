"""Plan and land Open-Meteo forecast/archive data into Bronze."""

from __future__ import annotations

import argparse
import time
from collections.abc import Iterable, Sequence
from datetime import UTC, date, datetime

import duckdb
from minio import Minio
from minio.error import S3Error

from vn_climate_risk_monitor.platform.lakehouse import get_connection
from vn_climate_risk_monitor.platform.minio import get_minio_client
from vn_climate_risk_monitor.platform.settings import OpenMeteoSettings, load_settings
from vn_climate_risk_monitor.sources.open_meteo import grid
from vn_climate_risk_monitor.sources.open_meteo.fetch import (
    FetchTask,
    ensure_bucket,
    land,
    run_fetch_pool,
)
from vn_climate_risk_monitor.sources.open_meteo.planner import (
    ARCHIVE_MODELS,
    STAGING_HOURLY,
    Location,
    archive_tasks,
    days_in_month,
    forecast_run_id,
    forecast_tasks,
    model_for_month,
    month_params,
    months_between,
    slot_params,
)


def load_locations(connection: duckdb.DuckDBPyConnection) -> tuple[Location, ...]:
    rows = connection.execute(
        "SELECT ward_code, ward_latitude, ward_longitude FROM gold.dim_ward ORDER BY ward_code"
    ).fetchall()
    if not rows:
        raise RuntimeError(
            "gold.dim_ward rỗng — chạy dbt seed và build geography trước"
        )
    return tuple(Location(str(c), float(lat), float(lon)) for c, lat, lon in rows)


def covered_months(
    connection: duckdb.DuckDBPyConnection,
    table: str = STAGING_HOURLY,
    *,
    weather_model: str | None = None,
) -> frozenset[date]:
    """Return months with complete hourly staging coverage."""
    try:
        # Coverage is model-specific in the shared staging table.
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


def existing_for_prefixes(
    client: Minio, bucket: str, prefixes: Iterable[str]
) -> dict[str, set[str]]:
    return {
        prefix: existing_basenames(client, bucket, prefix)
        for prefix in dict.fromkeys(prefixes)
    }


def _run_pool(
    tasks: Sequence[FetchTask], settings: OpenMeteoSettings, minio_cfg
) -> int:
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
    print("Bước tiếp theo: uv run auto-loader")
    return 0


def _cmd_map_grid(args, settings) -> int:
    """Probe API để chốt ô lưới của từng model rồi ghi seed CSV."""
    connection = get_connection(read_only=True)
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
    print(
        f"Đã ghi {grid.SEED_PATH}. Bước tiếp theo: dbt seed rồi fetch archive"
    )
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
    cells_by_model = {
        model.name: grid.cells_for(model.name, grid.SEED_PATH)
        for model in ARCHIVE_MODELS
    }
    prefixes = [
        month_params(month, model_for_month(month))[0]
        for month in months
    ]
    tasks = archive_tasks(
        months=months,
        settings=open_meteo,
        run=f"run_{datetime.now(UTC):%Y%m%dT%H%M%S}",
        covered=covered,
        cells_by_model=cells_by_model,
        existing=existing_for_prefixes(client, settings.minio.bucket, prefixes),
    )
    by_model: dict[str, int] = {}
    for month in months:
        name = model_for_month(month).name
        by_model[name] = by_model.get(name, 0) + (
            0 if month in covered.get(name, ()) else 1
        )
    print(f"Kế hoạch archive {args.start}→{args.end}: {len(months)} tháng")
    for model in ARCHIVE_MODELS:
        done = len([m for m in months if model_for_month(m) is model]) - by_model.get(
            model.name, 0
        )
        print(
            f"   {model.name:<10} còn {by_model.get(model.name, 0)} tháng (đã đủ {done})"
        )
    print(f"   {len(tasks)} request / {sum(t.units for t in tasks):,} đơn vị")
    if not tasks:
        print("Không còn gì để land. Bước tiếp theo: uv run auto-loader")
        return 0
    if not args.execute:
        print("DRY RUN — thêm --execute để chạy thật")
        return 0
    return _run_pool(tasks, open_meteo, settings.minio)


def _cmd_forecast(args, settings) -> int:
    open_meteo = settings.open_meteo
    connection = get_connection(read_only=True)
    try:
        locations = load_locations(connection)
    finally:
        connection.close()
    if args.limit:
        locations = locations[: args.limit]
    slot = (args.slot or datetime.now(UTC)).astimezone(UTC).replace(
        minute=0, second=0, microsecond=0
    )
    client = get_minio_client(settings.minio)
    prefixes = [slot_params(slot, open_meteo)[0]]
    tasks = forecast_tasks(
        slots=[slot],
        locations=locations,
        settings=open_meteo,
        run=forecast_run_id(slot),
        existing=existing_for_prefixes(
            client, settings.minio.bucket, prefixes
        ),
    )
    print(
        f"Kế hoạch forecast {slot:%Y-%m-%d %H}h: {len(locations)} phường, "
        f"{len(tasks)} request / {sum(t.units for t in tasks):,} đơn vị"
    )
    if not tasks:
        print("Không còn gì để land. Bước tiếp theo: uv run auto-loader")
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
    p_fc.add_argument("--slot", type=datetime.fromisoformat, help="Slot UTC do scheduler truyền")

    p_ar = sub.add_parser("archive", help="Land lịch sử theo khoảng tháng")
    p_ar.add_argument("--start", type=date.fromisoformat, default=date(2000, 1, 1))
    p_ar.add_argument(
        "--end", type=date.fromisoformat, default=datetime.now(UTC).date()
    )

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
