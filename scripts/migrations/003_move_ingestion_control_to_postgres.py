#!/usr/bin/env python3
"""Move empty legacy DuckLake ops relations to native PostgreSQL control tables."""

from __future__ import annotations

import argparse

from vn_climate_risk_monitor.ingestion.state import (
    connect_control_plane,
    ensure_ingestion_state,
)
from vn_climate_risk_monitor.lakehouse import get_connection

LEGACY_RELATIONS = (
    "ops.ingestion_files",
    "ops.pipeline_runs",
)


def _existing_relation_counts(*, read_only: bool) -> dict[str, int]:
    connection = get_connection(attach_bronze=False, read_only=read_only)
    try:
        existing = {
            f"{schema}.{table}"
            for schema, table in connection.execute(
                """
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_schema = 'ops'
                """
            ).fetchall()
        }
        return {
            relation: connection.execute(f"SELECT count(*) FROM {relation}").fetchone()[
                0
            ]
            for relation in LEGACY_RELATIONS
            if relation in existing
        }
    finally:
        connection.close()


def _execute() -> None:
    control_connection = connect_control_plane()
    try:
        ensure_ingestion_state(control_connection)
    finally:
        control_connection.close()

    counts = _existing_relation_counts(read_only=False)
    nonempty = {relation: count for relation, count in counts.items() if count > 0}
    if nonempty:
        raise RuntimeError(
            "Refusing to drop non-empty legacy ingestion relations: "
            f"{nonempty}. Migrate their state explicitly first."
        )

    connection = get_connection(attach_bronze=False)
    try:
        for relation in LEGACY_RELATIONS:
            if relation in counts:
                connection.execute(f"DROP TABLE {relation}")
        connection.execute("DROP SCHEMA IF EXISTS ops")
    finally:
        connection.close()
    print(f"Removed {len(counts)} empty legacy DuckLake ingestion relations")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    counts = _existing_relation_counts(read_only=True)
    print("Legacy DuckLake ingestion relations:")
    if not counts:
        print("  none")
    for relation, count in counts.items():
        print(f"  {relation}: {count} rows")
    if args.execute:
        _execute()
    else:
        print("DRY RUN: pass --execute after confirming every relation is empty")


if __name__ == "__main__":
    main()
