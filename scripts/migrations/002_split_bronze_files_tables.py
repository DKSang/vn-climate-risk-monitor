#!/usr/bin/env python3
"""Migrate Bronze to the explicit ``files`` and ``tables`` subdirectories.

The command is a dry-run unless ``--execute`` is provided. Immutable objects
are copied and verified before their old keys are removed. DuckLake-managed
tables are dropped through the catalog and their files are cleaned by DuckLake;
the script never directly deletes managed Parquet objects.
"""

from __future__ import annotations

import argparse
import io
import json
from dataclasses import dataclass

from minio.commonconfig import CopySource
from minio.error import S3Error

from vn_climate_risk_monitor.config import load_settings
from vn_climate_risk_monitor.lakehouse import get_connection
from vn_climate_risk_monitor.storage import get_minio_client

OLD_FILES_PREFIX = "bronze/source/"
NEW_FILES_PREFIX = "bronze/files/"


@dataclass(frozen=True)
class Relation:
    schema: str
    name: str
    table_type: str

    @property
    def qualified_name(self) -> str:
        return f'"catalog1"."{self.schema}"."{self.name}"'


# Dependency order matters: the join view is removed before its input views.
SILVER_VIEWS = (
    Relation("silver", "ward_locations", "VIEW"),
    Relation("silver", "ward_centroids", "VIEW"),
    Relation("silver", "wards", "VIEW"),
)
BRONZE_TABLES = (
    Relation("bronze", "gso_administrative_regions", "BASE TABLE"),
    Relation("bronze", "gso_administrative_units", "BASE TABLE"),
    Relation("bronze", "gso_provinces", "BASE TABLE"),
    Relation("bronze", "gso_wards", "BASE TABLE"),
    Relation("bronze", "ward_coordinates", "BASE TABLE"),
)


def _actual_relations(connection, schema: str) -> dict[str, str]:
    return {
        name: table_type
        for name, table_type in connection.execute(
            """
            SELECT table_name, table_type
            FROM information_schema.tables
            WHERE table_catalog = 'catalog1' AND table_schema = ?
            """,
            [schema],
        ).fetchall()
    }


def _relations_to_drop(connection) -> list[Relation]:
    expected_by_schema = {
        "bronze": {relation.name: relation for relation in BRONZE_TABLES},
        "silver": {relation.name: relation for relation in SILVER_VIEWS},
    }
    selected: list[Relation] = []

    actual_bronze = _actual_relations(connection, "bronze")
    unexpected = sorted(set(actual_bronze) - set(expected_by_schema["bronze"]))
    if unexpected:
        raise RuntimeError(
            "Refusing migration: unexpected catalog1.bronze relations: "
            + ", ".join(unexpected)
        )

    # Silver views only need removal while legacy Bronze tables still exist.
    # This makes a completed migration safe to re-run for manifest recovery.
    candidates = (*SILVER_VIEWS, *BRONZE_TABLES) if actual_bronze else ()
    for relation in candidates:
        actual_type = _actual_relations(connection, relation.schema).get(relation.name)
        if actual_type is None:
            continue
        if actual_type != relation.table_type:
            raise RuntimeError(
                f"Refusing to drop {relation.qualified_name}: expected "
                f"{relation.table_type}, found {actual_type}"
            )
        selected.append(relation)
    return selected


def _file_moves(client, bucket: str) -> list[tuple[str, str]]:
    return [
        (item.object_name, NEW_FILES_PREFIX + item.object_name.removeprefix(OLD_FILES_PREFIX))
        for item in client.list_objects(
            bucket, prefix=OLD_FILES_PREFIX, recursive=True
        )
    ]


def _manifests_to_rewrite(client, bucket: str) -> list[str]:
    manifests: list[str] = []
    for item in client.list_objects(bucket, prefix=NEW_FILES_PREFIX, recursive=True):
        if not item.object_name.endswith("/_manifest.json"):
            continue
        response = client.get_object(bucket, item.object_name)
        try:
            manifest = json.loads(response.read())
        finally:
            response.close()
            response.release_conn()
        if any(
            isinstance(file_metadata.get("object_key"), str)
            and file_metadata["object_key"].startswith(OLD_FILES_PREFIX)
            for file_metadata in manifest.get("files", [])
        ):
            manifests.append(item.object_name)
    return sorted(manifests)


def _print_plan(
    relations: list[Relation],
    moves: list[tuple[str, str]],
    manifests: list[str],
) -> None:
    print("Immutable Bronze file moves:")
    if not moves:
        print("  (none)")
    for source, destination in moves:
        print(f"  COPY+VERIFY+DELETE: {source} -> {destination}")
    for manifest in manifests:
        print(f"  REWRITE OBJECT KEYS: {manifest}")

    print("Catalog relations to rebuild at bronze_store.tables:")
    if not relations:
        print("  (none)")
    for relation in relations:
        print(f"  DROP {relation.table_type}: {relation.qualified_name}")
    if any(relation.schema == "bronze" for relation in relations):
        print("  DROP SCHEMA IF EMPTY: catalog1.bronze")


def _copy_then_remove(client, bucket: str, source: str, destination: str) -> None:
    source_stat = client.stat_object(bucket, source)
    try:
        destination_stat = client.stat_object(bucket, destination)
    except S3Error as error:
        if error.code not in {"NoSuchKey", "NoSuchObject"}:
            raise
        destination_stat = None

    if destination_stat is None:
        client.copy_object(bucket, destination, CopySource(bucket, source))
        destination_stat = client.stat_object(bucket, destination)

    if destination_stat.size != source_stat.size:
        raise RuntimeError(
            f"Refusing to remove {source}: destination size "
            f"{destination_stat.size} != source size {source_stat.size}"
        )
    client.remove_object(bucket, source)


def _rewrite_manifest(client, bucket: str, object_name: str) -> None:
    response = client.get_object(bucket, object_name)
    try:
        manifest = json.loads(response.read())
    finally:
        response.close()
        response.release_conn()

    changed = False
    for file_metadata in manifest.get("files", []):
        object_key = file_metadata.get("object_key")
        if isinstance(object_key, str) and object_key.startswith(OLD_FILES_PREFIX):
            file_metadata["object_key"] = NEW_FILES_PREFIX + object_key.removeprefix(
                OLD_FILES_PREFIX
            )
            changed = True
    if not changed:
        return

    manifest["storage_layout_version"] = 2
    manifest["migrated_from_prefix"] = OLD_FILES_PREFIX
    content = json.dumps(manifest, indent=2, ensure_ascii=False).encode("utf-8")
    client.put_object(
        bucket,
        object_name,
        io.BytesIO(content),
        len(content),
        content_type="application/json",
    )


def migrate(*, execute: bool) -> None:
    settings = load_settings()
    # Only attach the old catalog during planning. A dry-run must not initialize
    # ducklake_bronze metadata as a side effect.
    connection = get_connection(attach_bronze=False)
    client = get_minio_client(settings.minio)
    relations = _relations_to_drop(connection)
    moves = _file_moves(client, settings.minio.bucket)
    manifests = _manifests_to_rewrite(client, settings.minio.bucket)
    _print_plan(relations, moves, manifests)

    if not execute:
        connection.close()
        print("Dry-run only. Re-run with --execute to apply this exact plan.")
        return

    # Immutable source data is made safe at its new key before catalog changes.
    for source, destination in moves:
        _copy_then_remove(client, settings.minio.bucket, source, destination)
    # Include manifests copied in this execution as well as manifests left by a
    # previously interrupted/older migration execution.
    manifests = _manifests_to_rewrite(client, settings.minio.bucket)
    for manifest in manifests:
        _rewrite_manifest(client, settings.minio.bucket, manifest)

    connection.execute("BEGIN TRANSACTION")
    try:
        for relation in relations:
            statement = "DROP VIEW" if relation.table_type == "VIEW" else "DROP TABLE"
            connection.execute(f"{statement} {relation.qualified_name}")
        if any(relation.schema == "bronze" for relation in relations):
            remaining = _actual_relations(connection, "bronze")
            if remaining:
                raise RuntimeError(
                    "Refusing to drop catalog1.bronze because relations remain: "
                    + ", ".join(sorted(remaining))
                )
            connection.execute('DROP SCHEMA IF EXISTS "catalog1"."bronze"')
        connection.execute("COMMIT")
    except BaseException:
        connection.execute("ROLLBACK")
        connection.close()
        raise

    connection.execute("CALL ducklake_expire_snapshots('catalog1', older_than => now())")
    connection.execute("CALL ducklake_cleanup_old_files('catalog1', cleanup_all => true)")
    connection.close()

    print(
        f"Migration complete: moved {len(moves)} immutable objects, normalized "
        f"{len(manifests)} manifests, and dropped {len(relations)} relations. "
        "Run `make bootstrap && make transform`."
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Apply the verified migration; default behavior is dry-run.",
    )
    args = parser.parse_args()
    migrate(execute=args.execute)


if __name__ == "__main__":
    main()
