# syntax=docker/dockerfile:1
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim@sha256:531f855bda2c73cd6ef67d56b733b357cea384185b3022bd09f05e002cd144ca

RUN groupadd --system --gid 10001 app \
    && useradd --system --uid 10001 --gid app --create-home app \
    && install -d -o app -g app /opt/project_venv /project

USER app
WORKDIR /project

ENV UV_PROJECT_ENVIRONMENT=/opt/project_venv \
    VIRTUAL_ENV=/opt/project_venv \
    UV_NO_SYNC=1 \
    PYTHONPATH=/project

# Cache locked third-party dependencies independently from application code.
COPY --chown=app:app pyproject.toml uv.lock .python-version /project/
RUN uv sync --frozen --no-dev --no-install-project

COPY --chown=app:app . /project
RUN uv sync --frozen --no-dev

EXPOSE 8501
CMD ["uv", "run", "streamlit", "run", "serving/dashboard/app.py", "--server.port", "8501", "--server.address", "0.0.0.0"]
