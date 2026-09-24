-- Runs once, when the Postgres volume is first created.
-- Airflow keeps its metadata in its own schema (see scripts/airflow_sql_alchemy_conn.py)
-- and needs it before `airflow db migrate`, so it cannot wait for the pipeline's init task.
CREATE SCHEMA IF NOT EXISTS airflow;
