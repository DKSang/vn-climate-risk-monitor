#!/usr/bin/env bash
# Start Airflow standalone (scheduler + webserver) for the local stack.
set -euo pipefail

# A stable Fernet key derived from the local admin password, so restarts can
# still decrypt what earlier runs stored.
AIRFLOW__CORE__FERNET_KEY="$(python -c 'import base64, hashlib, os; print(base64.urlsafe_b64encode(hashlib.sha256(os.environ["AIRFLOW_PASSWORD"].encode()).digest()).decode())')"
export AIRFLOW__CORE__FERNET_KEY

airflow db migrate
# Create the admin once; later starts only sync its password with .env.
airflow users create --username "$AIRFLOW_USERNAME" --password "$AIRFLOW_PASSWORD" \
  --firstname VN --lastname Climate --role Admin --email "$AIRFLOW_USERNAME@localhost" || true
airflow users reset-password --username "$AIRFLOW_USERNAME" --password "$AIRFLOW_PASSWORD"

# DuckLake has one writer at a time (docs/adr/0001-watermark-incremental-pattern.md).
airflow pools set lakehouse_single_writer_pool 1 "Single DuckLake writer"

# A stale PID file from an unclean shutdown stops the webserver from starting.
rm -f /opt/airflow/airflow-webserver.pid
exec airflow standalone
