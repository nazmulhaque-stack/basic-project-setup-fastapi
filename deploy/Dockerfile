FROM python:3.12-slim

WORKDIR /workspace

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency definition
COPY requirements.txt .

# Install pinned Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source code and deployment scripts
COPY app/ ./app/
COPY deploy/ ./deploy/
COPY alembic/ ./alembic/
COPY alembic.ini .
COPY .env.example .env

# Expose FastAPI port
EXPOSE 8000

# Start Gunicorn with Uvicorn worker
CMD ["gunicorn", "-c", "deploy/gunicorn.conf.py", "app.main:app"]
