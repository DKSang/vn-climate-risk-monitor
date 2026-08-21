#!/usr/bin/env python3
"""Remove pre-v2 relations and unmanaged Raw objects using an exact allowlist.

The command is a dry-run unless ``--execute`` is provided. DuckLake-managed
Parquet files are never removed directly; dropping relations followed by
snapshot expiry lets DuckLake clean them safely.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass

from vn_climate_risk_monitor.config import load_settings
from vn_climate_risk_monitor.lakehouse import get_connection
from vn_climate_risk_monitor.storage import get_minio_client


@dataclass(frozen=True)
class LegacyRelation:
    schema: str
    name: str
    table_type: str

    @property
    def qualified_name(self) -> str:
        return f'"{self.schema}"."{self.name}"'


# Views must be removed before the Bronze tables they reference.
LEGACY_RELATIONS = (
    LegacyRelation("silver", "stg_locations_coordinates", "VIEW"),
    LegacyRelation("silver", "stg_wards", "VIEW"),
    LegacyRelation("silver", "ward_coordinates_cleaned", "VIEW"),
    LegacyRelation("silver", "wards_cleaned", "VIEW"),
    LegacyRelation("bronze", "administrative_regions_raw", "BASE TABLE"),
    LegacyRelation("bronze", "administrative_units_raw", "BASE TABLE"),
    LegacyRelation("bronze", "provinces_raw", "BASE TABLE"),
    LegacyRelation("bronze", "ward_coordinates_raw", "BASE TABLE"),
    LegacyRelation("bronze", "wards_raw", "BASE TABLE"),
)

# This is an unmanaged object prefix from the architecture before Bronze became
# the source-of-truth layer. Never broaden this prefix to ``raw/`` or ``bronze/``.
LEGACY_OBJECT_PREFIXES = (
    "raw/geography/vietnamese_provinces_db/administrative_units/",
)


def _existing_relations(connection) -> list[LegacyRelation]:
    actual = {
        (schema, name): table_type
        for schema, name, table_type in connection.execute(
            """
            SELECT table_schema, table_name, table_type
            FROM information_schema.tables
            WHERE table_schema IN ('bronze', 'silver')
            """
        ).fetchall()
    }
    existing: list[LegacyRelation] = []
    for relation in LEGACY_RELATIONS:
        actual_type = actual.get((relation.schema, relation.name))
        if actual_type is None:
            continue
        if actual_type != relation.table_type:
            raise RuntimeError(
                f"Refusing to drop {relation.schema}.{relation.name}: "
                f"expected {relation.table_type}, found {actual_type}"
            )
        existing.append(relation)
    return existing


def _legacy_objects(client, bucket: str) -> list[str]:
    objects: list[str] = []
    for prefix in LEGACY_OBJECT_PREFIXES:
        if prefix.startswith("bronze/files/"):
            raise RuntimeError("Migration guard rejected a Bronze files prefix")
        objects.extend(
            item.object_name
            for item in client.list_objects(bucket, prefix=prefix, recursive=True)
        )
    return sorted(objects)


def _print_plan(relations: list[LegacyRelation], objects: list[str]) -> None:
    print("Legacy catalog relations:")
    if not relations:
        print("  (none)")
    for relation in relations:
        print(f"  DROP {relation.table_type}: {relation.schema}.{relation.name}")

    print("Legacy unmanaged MinIO objects:")
    if not objects:
        print("  (none)")
    for object_name in objects:
        print(f"  DELETE: {object_name}")

    print("Protected prefix: bronze/files/")


def migrate(*, execute: bool) -> None:
    settings = load_settings()
    connection = get_connection(attach_bronze=False)
    minio = get_minio_client(settings.minio)
    relations = _existing_relations(connection)
    objects = _legacy_objects(minio, settings.minio.bucket)
    _print_plan(relations, objects)

    if not execute:
        print("Dry-run only. Re-run with --execute to apply this exact plan.")
        connection.close()
        return

    connection.execute("BEGIN TRANSACTION")
    try:
        for relation in relations:
            statement = "DROP VIEW" if relation.table_type == "VIEW" else "DROP TABLE"
            connection.execute(f"{statement} {relation.qualified_name}")
        connection.execute("COMMIT")
    except BaseException:
        connection.execute("ROLLBACK")
        connection.close()
        raise

    # Expire obsolete snapshots before asking DuckLake to remove unreferenced
    # files. Source objects are not registered in DuckLake and are unaffected.
    connection.execute(
        "CALL ducklake_expire_snapshots('catalog1', older_than => now())"
    )
    connection.execute(
        "CALL ducklake_cleanup_old_files('catalog1', cleanup_all => true)"
    )
    connection.close()

    for object_name in objects:
        minio.remove_object(settings.minio.bucket, object_name)

    print(
        f"Migration complete: dropped {len(relations)} relations and "
        f"deleted {len(objects)} unmanaged objects."
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Apply the allowlisted migration; default behavior is dry-run.",
    )
    args = parser.parse_args()
    migrate(execute=args.execute)


if __name__ == "__main__":
    main()
