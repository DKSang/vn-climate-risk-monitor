"""Print Airflow's metadata URI using the required local runtime settings."""

from __future__ import annotations

import os
from urllib.parse import quote


def required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise SystemExit(f"Missing {name}")
    return value


user = quote(required("POSTGRES_USER"), safe="")
password_encoded = quote(required("POSTGRES_PASSWORD"), safe="")
host = required("POSTGRES_HOST")
port = required("POSTGRES_PORT")
database = quote(required("POSTGRES_DB"), safe="")
print(
    f"postgresql+psycopg2://{user}:{password_encoded}@{host}:{port}/{database}"
    "?options=-csearch_path%3Dairflow"
)
