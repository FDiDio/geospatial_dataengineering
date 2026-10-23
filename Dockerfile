# syntax=docker/dockerfile:1

# Base image shared by all the stages: Python 3.12 and the runtime dependencies, installed with uv
FROM python:3.12-slim AS base

COPY --from=ghcr.io/astral-sh/uv:0.13.5 /uv /uvx /bin/

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_PYTHON_DOWNLOADS=never \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PATH="/opt/venv/bin:$PATH"

WORKDIR /app

# Dependencies first, from the lockfile, so code changes do not invalidate the cached layer
COPY pyproject.toml uv.lock .python-version ./
RUN --mount=type=cache,target=/root/.cache/uv uv sync --frozen --no-default-groups

# The pipeline runs as an unprivileged user and writes only to /app/output
RUN useradd --create-home --uid 1000 pipeline \
    && mkdir output \
    && chown pipeline:pipeline output

COPY src/ src/
COPY config/ config/


# Runtime stage: docker run --rm -v ./output:/app/output era5-pipeline --start_date 20220430 --end_date 20220430
FROM base AS runtime

USER pipeline
VOLUME ["/app/output"]
ENTRYPOINT ["python", "src/main_script.py"]


# Platform stage: Dagster (webserver and daemon), dbt and the dashboard
FROM base AS platform

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-default-groups --group orchestration --group dashboard

COPY transform/ transform/
# HOME holds the DuckDB extensions installed below, also when the container runs with another uid
ENV DAGSTER_HOME=/app/dagster_home \
    HOME=/home/pipeline
COPY src/orchestration/dagster.yaml /app/dagster_home/dagster.yaml
RUN mkdir -p /app/dagster_home/storage && chown -R pipeline:pipeline transform /app/dagster_home

USER pipeline
# The Dagster assets are defined from the dbt manifest; the DuckDB extensions are installed
# at build time, so the containers do not download them at run time
RUN cd transform && dbt parse --profiles-dir . \
    && python -c "import duckdb; con = duckdb.connect(); [con.execute(f'INSTALL {e}') for e in ('parquet', 'iceberg', 'h3 FROM community')]"

EXPOSE 3000 8501
CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-m", "orchestration.definitions", "-d", "src"]


# Test stage: docker build --target test -t era5-pipeline:test . && docker run --rm era5-pipeline:test
FROM base AS test

RUN --mount=type=cache,target=/root/.cache/uv uv sync --frozen
COPY transform/ transform/
COPY tests/ tests/
RUN chown -R pipeline:pipeline transform

USER pipeline
ENTRYPOINT ["python", "-m", "pytest", "-p", "no:cacheprovider"]
