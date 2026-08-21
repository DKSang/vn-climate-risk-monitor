"""Report pipeline health from the generic PostgreSQL ingestion control plane."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from datetime import timedelta
from enum import Enum
from uuid import UUID

from vn_climate_risk_monitor.config import load_settings
from vn_climate_risk_monitor.ingestion.collectors.open_meteo_forecast import (
    DATASET,
    PIPELINE_NAME,
)
from vn_climate_risk_monitor.ingestion.state import (
    PipelineHealth,
    PostgresIngestionRepository,
    connect_control_plane,
    ensure_ingestion_state,
)


def _json_default(value: object) -> str:
    if isinstance(value, Enum):
        return str(value.value)
    if isinstance(value, UUID):
        return str(value)
    isoformat = getattr(value, "isoformat", None)
    if callable(isoformat):
        return str(isoformat())
    raise TypeError(f"Cannot serialize {type(value).__name__}")


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pipeline-name", default=PIPELINE_NAME)
    parser.add_argument("--dataset", default=DATASET)
    parser.add_argument("--scope", default="production")
    parser.add_argument(
        "--stale-after-minutes",
        type=_positive_int,
        help="Override the forecast-oriented default for slower pipelines.",
    )
    parser.add_argument("--json", action="store_true")
    parser.add_argument(
        "--check",
        action="store_true",
        help="Exit non-zero unless pipeline health is HEALTHY.",
    )
    args = parser.parse_args()
    settings = load_settings()
    connection = connect_control_plane(settings.postgres)
    try:
        ensure_ingestion_state(connection)
        metrics = PostgresIngestionRepository(connection).pipeline_metrics(
            pipeline_name=args.pipeline_name,
            dataset=args.dataset,
            scope=args.scope,
            max_retries=settings.open_meteo.loader_max_retries,
        )
    finally:
        connection.close()
    stale_after_minutes = (
        args.stale_after_minutes or settings.open_meteo.stale_after_minutes
    )
    health = metrics.health(stale_after=timedelta(minutes=stale_after_minutes))

    if args.json:
        print(
            json.dumps(
                {"health": health, **asdict(metrics)},
                default=_json_default,
                sort_keys=True,
            )
        )
    else:
        print(
            f"Ingestion: pipeline={metrics.pipeline_name}, "
            f"dataset={metrics.dataset}, health={health}, scope={metrics.scope}, "
            f"latest_run={metrics.latest_run_status}, "
            f"latest_schedule={metrics.latest_scheduled_at_utc}, "
            f"files[pending={metrics.pending_files}, "
            f"processing={metrics.processing_files}, failed={metrics.failed_files}, "
            f"exhausted={metrics.retry_exhausted_files}], "
            f"24h[runs_ok={metrics.succeeded_runs_24h}, "
            f"runs_failed={metrics.failed_runs_24h}, "
            f"rows={metrics.rows_parsed_24h}, rescued={metrics.rescued_rows_24h}]"
        )
    if args.check and health != PipelineHealth.HEALTHY:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
