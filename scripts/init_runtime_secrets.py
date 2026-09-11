#!/usr/bin/env python3
"""Create persistent local Docker secrets on first startup."""

from __future__ import annotations

import os
import secrets
import sys
from base64 import urlsafe_b64encode
from pathlib import Path

SECRET_FILES = {
    "postgres_password": "POSTGRES_PASSWORD",
    "minio_secret_key": "MINIO_SECRET_KEY",
    "pgadmin_password": "PGADMIN_DEFAULT_PASSWORD",
    "airflow_secret_key": "AIRFLOW_SECRET_KEY",
    "airflow_fernet_key": "AIRFLOW_FERNET_KEY",
}


def main() -> None:
    target = Path(sys.argv[1] if len(sys.argv) > 1 else "/run/secrets")
    target.mkdir(parents=True, exist_ok=True)

    for filename, env_name in SECRET_FILES.items():
        path = target / filename
        if path.is_file() and path.stat().st_size:
            print(f"secret ready: {filename}")
            continue

        value = os.getenv(env_name, "").strip()
        if not value:
            value = (
                urlsafe_b64encode(secrets.token_bytes(32)).decode()
                if filename == "airflow_fernet_key"
                else secrets.token_urlsafe(32)
            )
        path.write_text(value, encoding="utf-8")
        path.chmod(0o444)
        print(f"secret created: {filename}")


if __name__ == "__main__":
    main()
