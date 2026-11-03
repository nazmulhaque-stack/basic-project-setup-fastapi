# ─────────────────────────────────────────────────────────────────────────────
# Stage 1: builder — install Python dependencies into an isolated prefix
# ─────────────────────────────────────────────────────────────────────────────
FROM python:3.12-slim AS builder

# Build tools required for asyncpg/cryptography/pydantic-core wheels
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    libssl-dev \
    libffi-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# ─────────────────────────────────────────────────────────────────────────────
# Stage 2: production — lean final image, non-root user
# ─────────────────────────────────────────────────────────────────────────────
FROM python:3.12-slim AS production

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Runtime-only native libs (libpq for asyncpg) + curl for the healthcheck below
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Installed packages from the builder stage — nothing else from it ships
COPY --from=builder /install /usr/local

RUN groupadd --gid 1001 appgroup && \
    useradd --uid 1001 --gid appgroup --no-create-home --shell /sbin/nologin appuser

WORKDIR /app

# Application source + deployment scripts/config (gunicorn.conf.py, nginx.conf, ...)
COPY --chown=appuser:appgroup app/ ./app/
COPY --chown=appuser:appgroup deploy/ ./deploy/

# /dev/shm is used for Gunicorn worker heartbeats (avoids disk I/O stalls) —
# created here in case the container runtime doesn't mount it by default
RUN mkdir -p /dev/shm

USER appuser

EXPOSE 8000

HEALTHCHECK --interval=15s --timeout=5s --start-period=20s --retries=5 \
    CMD curl -sf http://localhost:8000/healthz || exit 1

# Worker count, bind address, and log level all come from the environment
# (WEB_CONCURRENCY, GUNICORN_BIND, LOG_LEVEL — see deploy/gunicorn.conf.py),
# not hardcoded here, so docker-compose.yml/.env fully control them.
CMD ["gunicorn", "-c", "deploy/gunicorn.conf.py", "app.main:app"]
