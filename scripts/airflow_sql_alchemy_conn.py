#!/usr/bin/env python3
"""Print Airflow's metadata URI using the file-mounted PostgreSQL secret."""

from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import quote


def required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise SystemExit(f"Missing {name}")
    return value


password_path = Path(required("POSTGRES_PASSWORD_FILE"))
password = password_path.read_text(encoding="utf-8").rstrip("\r\n")
if not password:
    raise SystemExit(f"Empty PostgreSQL secret: {password_path}")

user = quote(required("POSTGRES_USER"), safe="")
password_encoded = quote(password, safe="")
host = required("POSTGRES_HOST")
port = required("POSTGRES_PORT")
database = quote(required("POSTGRES_DB"), safe="")
print(
    f"postgresql+psycopg2://{user}:{password_encoded}@{host}:{port}/{database}"
    "?options=-csearch_path%3Dairflow"
)
