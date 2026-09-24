"""Daily lake housekeeping with DuckLake's own functions.

Snapshots older than 7 days are expired (the dashboard only pins recent ones),
then data files unreferenced for 2 days are deleted.
"""

from __future__ import annotations

from pipeline import lake


def main() -> None:
    with lake.connect() as con:
        expired = con.execute(
            f"CALL ducklake_expire_snapshots('{lake.CATALOG}', "
            "older_than => now() - INTERVAL 7 DAY)"
        ).fetchall()
        deleted = con.execute(
            f"CALL ducklake_cleanup_old_files('{lake.CATALOG}', "
            "older_than => now() - INTERVAL 2 DAY)"
        ).fetchall()
    print(f"Expired {len(expired)} snapshot(s), deleted {len(deleted)} file(s)")
