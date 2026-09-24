"""Settings shared by every DAG."""

from __future__ import annotations

import os
from datetime import timedelta

PROJECT_DIR = os.getenv("PROJECT_DIR", "/project")

# Writers are serialised: the watermark pattern (docs/adr/0001) relies on it.
POOL = "lakehouse_single_writer_pool"

DEFAULT_ARGS = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}
