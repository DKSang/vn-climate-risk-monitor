#!/usr/bin/env python3
"""Expire all historical snapshots and clean unreferenced lakehouse files."""

from vn_climate_risk_monitor.lakehouse import PRIMARY_CATALOG, get_connection


def main() -> None:
    connection = get_connection()
    for catalog in (PRIMARY_CATALOG,):
        connection.execute(
            f"CALL ducklake_expire_snapshots('{catalog}', older_than => now())"
        )
        connection.execute(
            f"CALL ducklake_cleanup_old_files('{catalog}', cleanup_all => true)"
        )
        print(f"Squashed DuckLake catalog: {catalog}")
    connection.close()
    print("Current versions retained; historical time travel was removed.")


if __name__ == "__main__":
    main()
