"""Gunicorn configuration for production high-concurrency FastAPI deployment.

Worker Architecture:
- Uses Uvicorn's uvloop + httptools worker class.
- Workers count tuned to available CPU cores for pure CPU saturation without context-switch thrashing.
- Heartbeats stored in memory (/dev/shm) to eliminate disk I/O stalls.
- Recycles workers after max_requests + jitter to defend against memory fragmentation.
"""

import multiprocessing
import os

# Server Socket
bind = os.getenv("GUNICORN_BIND", "0.0.0.0:8000")
backlog = 65535  # Match SOMAXCONN for high burst handling

# Worker Processes
# Rule of thumb: 1 worker per physical core for I/O bound async systems
workers = int(os.getenv("WEB_CONCURRENCY", multiprocessing.cpu_count()))
worker_class = "uvicorn.workers.UvicornWorker"

# Worker Lifecycle & Memory Defense
# Restart workers periodically to prevent memory leaks, with randomized jitter
max_requests = 10_000
max_requests_jitter = 1_000

# Timeouts
timeout = 30
graceful_timeout = 30
# keepalive MUST be higher than Nginx upstream keepalive_timeout (e.g. 65s > 60s)
# Prevents race conditions where Gunicorn closes an idle keepalive connection
# at the exact millisecond Nginx assigns a new client request to it (502 Bad Gateway).
keepalive = 65

# High-Performance Shared Memory
# Storing worker heartbeats in tmpfs (/dev/shm) prevents blocking on slow storage/NFS
if os.path.exists("/dev/shm"):
    worker_tmp_dir = "/dev/shm"

# Logging
accesslog = "-"
errorlog = "-"
loglevel = os.getenv("LOG_LEVEL", "info").lower()
access_log_format = '%(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s" %(D)s µs'
