# syntax=docker/dockerfile:1.7
# Data Sync Service — FastAPI + uv + Postgres + Alembic
# Builds a minimal image with Alembic migrations baked in.
# Runs as non-root; listens on 0.0.0.0:4330 (matches live conventions).
#
# Build context: services/data-sync-service (so pyproject.toml + uv.lock are at root).
# The repo-level .dockerignore + this service's own .dockerignore keep image small.

# ---------- Stage 1: dependencies ----------
FROM python:3.13-slim-bookworm AS deps

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/usr/local \
    UV_COMPILE_BYTECODE=1

# Install uv (pinned version for reproducibility).
COPY --from=ghcr.io/astral-sh/uv:0.5.11 /uv /uvx /usr/local/bin/

WORKDIR /app

# Cache dependency layer: only re-install when uv.lock or pyproject.toml changes.
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project

# ---------- Stage 2: runtime ----------
FROM python:3.13-slim-bookworm AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PORT=4330 \
    HOST=0.0.0.0

# OS deps for psycopg[binary] + healthcheck tooling.
# curl: HEALTHCHECK below.
# libpq5: psycopg runtime.
# tini: clean SIGTERM forwarding (uvicorn graceful shutdown).
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
        libpq5 \
        tini \
    && rm -rf /var/lib/apt/lists/*

# Non-root user.
RUN groupadd --system --gid 1001 karios \
    && useradd --system --uid 1001 --gid karios --home /app --shell /usr/sbin/nologin karios

WORKDIR /app

# Copy the pre-built venv from deps stage.
COPY --from=deps /usr/local /usr/local

# Copy application source (Alembic config + packages).
COPY --chown=karios:karios alembic ./alembic
COPY --chown=karios:karios alembic.ini ./
COPY --chown=karios:karios server_entry.py ./
COPY --chown=karios:karios src ./src

USER karios

EXPOSE 4330

HEALTHCHECK --interval=15s --timeout=5s --start-period=30s --retries=5 \
    CMD curl --fail --silent http://127.0.0.1:4330/healthz \
        | python3 -c "import sys,json; d=json.load(sys.stdin); sys.exit(0 if d.get('db') else 1)" \
        || exit 1

# tini -> uvicorn (replaces PID 1; forwards SIGTERM for graceful shutdown).
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "server_entry.py"]