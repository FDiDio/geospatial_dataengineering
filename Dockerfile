# Base image shared by the test and runtime stages
FROM python:3.11-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Dependencies first, so code changes do not invalidate the cached layer
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY src/ src/
COPY config/ config/

# The pipeline runs as an unprivileged user and writes only to /app/output
RUN useradd --create-home --uid 1000 pipeline \
    && mkdir output \
    && chown pipeline:pipeline output


# Test stage: docker build --target test -t era5-pipeline:test . && docker run --rm era5-pipeline:test
FROM base AS test

COPY requirements-dev.txt pytest.ini ./
RUN pip install -r requirements-dev.txt
COPY tests/ tests/

USER pipeline
ENTRYPOINT ["python", "-m", "pytest", "-p", "no:cacheprovider"]


# Runtime stage (default): docker run --rm -v ./output:/app/output era5-pipeline --start_date 20220430 --end_date 20220430
FROM base AS runtime

USER pipeline
VOLUME ["/app/output"]
ENTRYPOINT ["python", "src/main_script.py"]
