from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).parents[2]


def test_airflow_metadata_uri_uses_and_encodes_direct_postgres_password() -> None:
    env = os.environ.copy()
    env.pop("POSTGRES_PASSWORD_FILE", None)
    env.update(
        {
            "POSTGRES_HOST": "postgres",
            "POSTGRES_PORT": "5432",
            "POSTGRES_DB": "climate/db",
            "POSTGRES_USER": "airflow user",
            "POSTGRES_PASSWORD": "p@ss:/?",
        }
    )

    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts/airflow_sql_alchemy_conn.py")],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == (
        "postgresql+psycopg2://airflow%20user:p%40ss%3A%2F%3F@postgres:5432/"
        "climate%2Fdb?options=-csearch_path%3Dairflow"
    )
