#!/usr/bin/env python3
"""Generalize ingestion control metadata while preserving every run/file row."""

from __future__ import annotations

import argparse

from vn_climate_risk_monitor.ingestion.state import connect_control_plane

RUNS = "ingestion.ingestion_runs"
FILES = "ingestion.ingestion_files"
LEGACY_RUN_COLUMNS = {
    "batch_count",
    "location_count",
    "source_endpoint",
    "model_requested",
    "forecast_hours",
    "hourly_variables",
    "request_contract_version",
}
LEGACY_FILE_COLUMNS = {
    "ward_keys",
    "expected_location_count",
    "received_location_count",
}
GENERIC_RUN_COLUMNS = {
    "source_name",
    "expected_file_count",
    "source_uri",
    "contract_version",
    "run_parameters",
}
GENERIC_FILE_COLUMNS = {
    "file_parameters",
    "expected_item_count",
    "received_item_count",
}


def _columns(connection: object, table_name: str) -> set[str]:
    rows = connection.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'ingestion' AND table_name = %s
        """,
        (table_name,),
    ).fetchall()
    return {row[0] for row in rows}


def _snapshot(connection: object) -> dict[str, object]:
    run_columns = _columns(connection, "ingestion_runs")
    file_columns = _columns(connection, "ingestion_files")
    if not run_columns or not file_columns:
        raise RuntimeError(
            "Ingestion control tables do not exist; run bootstrap instead"
        )
    active = connection.execute(
        """
        SELECT
            (SELECT count(*) FROM ingestion.ingestion_runs WHERE status = 'RUNNING'),
            (SELECT count(*) FROM ingestion.ingestion_files WHERE status = 'PROCESSING')
        """
    ).fetchone()
    counts = connection.execute(
        """
        SELECT
            (SELECT count(*) FROM ingestion.ingestion_runs),
            (SELECT count(*) FROM ingestion.ingestion_files)
        """
    ).fetchone()
    return {
        "run_columns": run_columns,
        "file_columns": file_columns,
        "running_runs": int(active[0]),
        "processing_files": int(active[1]),
        "run_rows": int(counts[0]),
        "file_rows": int(counts[1]),
    }


def _shape(snapshot: dict[str, object]) -> str:
    run_columns = snapshot["run_columns"]
    file_columns = snapshot["file_columns"]
    if LEGACY_RUN_COLUMNS <= run_columns and LEGACY_FILE_COLUMNS <= file_columns:
        return "legacy"
    if (
        GENERIC_RUN_COLUMNS <= run_columns
        and GENERIC_FILE_COLUMNS <= file_columns
        and not (LEGACY_RUN_COLUMNS & run_columns or LEGACY_FILE_COLUMNS & file_columns)
    ):
        return "generic"
    return "unsupported"


def _execute() -> None:
    connection = connect_control_plane()
    connection.autocommit = True
    try:
        before = _snapshot(connection)
        shape = _shape(before)
        if shape == "generic":
            print("Generic ingestion control schema already installed")
            return
        if shape != "legacy":
            raise RuntimeError(
                "Refusing unsupported or partially migrated schema shape"
            )
        if before["running_runs"] or before["processing_files"]:
            raise RuntimeError(
                "Refusing migration while ingestion is active: "
                f"running_runs={before['running_runs']}, "
                f"processing_files={before['processing_files']}"
            )

        with connection.transaction():
            connection.execute(f"LOCK TABLE {RUNS}, {FILES} IN ACCESS EXCLUSIVE MODE")
            connection.execute(
                f"ALTER TABLE {RUNS} RENAME COLUMN batch_count TO expected_file_count"
            )
            connection.execute(
                f"ALTER TABLE {RUNS} RENAME COLUMN source_endpoint TO source_uri"
            )
            connection.execute(
                f"ALTER TABLE {RUNS} "
                "RENAME COLUMN request_contract_version TO contract_version"
            )
            connection.execute(
                f"ALTER TABLE {RUNS} ALTER COLUMN contract_version TYPE TEXT "
                "USING contract_version::text"
            )
            connection.execute(
                f"ALTER TABLE {FILES} "
                "RENAME COLUMN expected_location_count TO expected_item_count"
            )
            connection.execute(
                f"ALTER TABLE {FILES} "
                "RENAME COLUMN received_location_count TO received_item_count"
            )
            connection.execute(
                f"ALTER TABLE {RUNS} ADD COLUMN source_name TEXT, "
                "ADD COLUMN run_parameters JSONB"
            )
            connection.execute(f"ALTER TABLE {FILES} ADD COLUMN file_parameters JSONB")
            connection.execute(
                f"""
                UPDATE {RUNS}
                SET source_name = CASE
                        WHEN pipeline_name = 'open_meteo_forecast' THEN 'open_meteo'
                        ELSE pipeline_name
                    END,
                    run_parameters = jsonb_build_object(
                        'model', model_requested,
                        'forecast_hours', forecast_hours,
                        'hourly_variables', to_jsonb(hourly_variables),
                        'location_count', location_count
                    )
                """
            )
            connection.execute(
                f"""
                UPDATE {FILES}
                SET file_parameters = jsonb_build_object(
                    'ward_keys', to_jsonb(ward_keys)
                )
                """
            )
            invalid = connection.execute(
                f"""
                SELECT
                    count(*) FILTER (
                        WHERE jsonb_typeof(run_parameters) <> 'object'
                           OR source_name IS NULL
                    ),
                    (SELECT count(*) FROM {FILES}
                     WHERE jsonb_typeof(file_parameters) <> 'object'
                        OR jsonb_array_length(file_parameters->'ward_keys')
                           <> expected_item_count)
                FROM {RUNS}
                """
            ).fetchone()
            if invalid != (0, 0):
                raise RuntimeError(f"Backfill validation failed: {invalid}")

            connection.execute(
                f"ALTER TABLE {RUNS} ALTER COLUMN source_name SET NOT NULL, "
                "ALTER COLUMN run_parameters SET NOT NULL"
            )
            connection.execute(
                f"ALTER TABLE {FILES} ALTER COLUMN file_parameters SET NOT NULL"
            )
            connection.execute(
                f"ALTER TABLE {RUNS} ADD CONSTRAINT ingestion_runs_parameters_object "
                "CHECK (jsonb_typeof(run_parameters) = 'object')"
            )
            connection.execute(
                f"ALTER TABLE {FILES} ADD CONSTRAINT ingestion_files_parameters_object "
                "CHECK (jsonb_typeof(file_parameters) = 'object')"
            )
            connection.execute(
                f"ALTER TABLE {FILES} DROP CONSTRAINT ingestion_files_check"
            )
            connection.execute(
                f"ALTER TABLE {FILES} ADD CONSTRAINT ingestion_files_item_count_match "
                "CHECK (received_item_count IS NULL OR expected_item_count IS NULL "
                "OR received_item_count = expected_item_count)"
            )
            connection.execute(
                f"ALTER TABLE {RUNS} "
                "DROP COLUMN location_count, DROP COLUMN model_requested, "
                "DROP COLUMN forecast_hours, DROP COLUMN hourly_variables"
            )
            connection.execute(f"ALTER TABLE {FILES} DROP COLUMN ward_keys")
            connection.execute(
                f"ALTER TABLE {RUNS} RENAME CONSTRAINT "
                "ingestion_runs_batch_count_check TO "
                "ingestion_runs_expected_file_count_check"
            )
            connection.execute(
                f"ALTER TABLE {FILES} RENAME CONSTRAINT "
                "ingestion_files_expected_location_count_check TO "
                "ingestion_files_expected_item_count_check"
            )
            connection.execute(
                f"ALTER TABLE {FILES} RENAME CONSTRAINT "
                "ingestion_files_received_location_count_check TO "
                "ingestion_files_received_item_count_check"
            )

        after = _snapshot(connection)
        if _shape(after) != "generic":
            raise RuntimeError("Post-migration schema validation failed")
        if (after["run_rows"], after["file_rows"]) != (
            before["run_rows"],
            before["file_rows"],
        ):
            raise RuntimeError("Row counts changed during migration")
        print(
            "Generalized ingestion control schema: "
            f"runs={after['run_rows']}, files={after['file_rows']}"
        )
    finally:
        connection.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    connection = connect_control_plane()
    try:
        snapshot = _snapshot(connection)
    finally:
        connection.close()
    shape = _shape(snapshot)
    print(
        f"Control schema={shape}, runs={snapshot['run_rows']}, "
        f"files={snapshot['file_rows']}, running={snapshot['running_runs']}, "
        f"processing={snapshot['processing_files']}"
    )
    if args.execute:
        _execute()
    else:
        print("DRY RUN: pass --execute after stopping collector and loader processes")


if __name__ == "__main__":
    main()
