FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim@sha256:531f855bda2c73cd6ef67d56b733b357cea384185b3022bd09f05e002cd144ca

RUN groupadd --system --gid 10001 app \
    && useradd --system --uid 10001 --gid app --create-home app \
    && install -d -o app -g app /opt/project_venv /project

USER app
WORKDIR /project

ENV UV_PROJECT_ENVIRONMENT=/opt/project_venv \
    VIRTUAL_ENV=/opt/project_venv \
    UV_NO_SYNC=1 \
    PYTHONPATH=/project/src:/project

# Cache locked third-party dependencies independently from application code.
COPY --chown=app:app pyproject.toml uv.lock .python-version /project/
RUN uv sync --frozen --no-dev --no-install-project

COPY --chown=app:app . /project
RUN set -eux; \
    for spec in \
      fetch-open-meteo=vn_climate_risk_monitor.sources.open_meteo.cli \
      auto-loader=vn_climate_risk_monitor.auto_loader.cli \
      auto-process=vn_climate_risk_monitor.auto_process.cli \
      pipeline-health=vn_climate_risk_monitor.quality.health \
      quality-gate=vn_climate_risk_monitor.quality.gates \
      maintain-lakehouse=vn_climate_risk_monitor.operations.maintenance \
      bootstrap-lakehouse=vn_climate_risk_monitor.operations.bootstrap \
      reset-lakehouse=vn_climate_risk_monitor.operations.reset; do \
      name=${spec%%=*}; module=${spec#*=}; \
      printf '#!/bin/sh\nexport PYTHONPATH=/project/src:/project${PYTHONPATH:+:$PYTHONPATH}\nexec /opt/project_venv/bin/python -c "from %s import main; raise SystemExit(main())" "$@"\n' "$module" > "/opt/project_venv/bin/$name"; \
      chmod +x "/opt/project_venv/bin/$name"; \
    done

EXPOSE 8501
CMD ["uv", "run", "streamlit", "run", "serving/dashboard/app.py", "--server.port", "8501", "--server.address", "0.0.0.0"]
