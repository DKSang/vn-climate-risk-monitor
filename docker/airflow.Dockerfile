FROM ghcr.io/astral-sh/uv:0.12.6@sha256:88bc6eb1ccd4b82efd0e1b530caffabddf50dc2bf612e66c14ea25b8ee8a4d3d AS uv
FROM apache/airflow:2.10.5-python3.12@sha256:6499a680a93463846d3a6be980e85d601dc97b0d81e82eed9ef5e5cb9da31b79

COPY --from=uv /uv /uvx /bin/

USER root
RUN install -d -o airflow -g root /opt/project_venv /project

USER airflow
WORKDIR /project

ENV UV_PROJECT_ENVIRONMENT=/opt/project_venv \
    VIRTUAL_ENV=/opt/project_venv \
    UV_NO_SYNC=1

# Cache locked third-party dependencies independently from application code.
COPY --chown=airflow:root pyproject.toml uv.lock .python-version /project/
RUN uv sync --frozen --no-dev --no-install-project

COPY --chown=airflow:root . /project
RUN uv sync --frozen --no-dev

# Bake DuckDB extensions into the image so nothing is downloaded at runtime.
RUN uv run python -c "import duckdb; [duckdb.install_extension(e) for e in ('ducklake', 'httpfs', 'postgres')]"

# Airflow parses these files with its own environment; tasks use the locked
# project environment through `uv run`.
COPY --chown=airflow:root dags /opt/airflow/dags
