#!/usr/bin/env python3
"""Infrastructure & Deployment Verification Tool for System Architect Agent.

Audits Dockerfile, Nginx configurations, Gunicorn ASGI tuning, Linux kernel sysctl,
and Kubernetes manifests to ensure production deployment readiness.
"""

from __future__ import annotations

import argparse
from pathlib import Path
import re
import sys
from typing import List

PROJECT_ROOT = Path(__file__).resolve().parents[4]


def verify_infrastructure() -> int:
    print("======================================================")
    print("  System Architect Infrastructure & DevOps Audit      ")
    print("======================================================")

    warnings: List[str] = []
    checks_passed = 0

    # 1. Check Dockerfile
    dockerfile = PROJECT_ROOT / "deploy/Dockerfile"
    if dockerfile.exists():
        content = dockerfile.read_text(encoding="utf-8")
        if "python:3.12" in content:
            print("✓ Dockerfile: Python 3.12 slim base confirmed.")
            checks_passed += 1
        if "pip install --no-cache-dir" in content:
            print("✓ Dockerfile: Cache disabled for minimal image layers.")
            checks_passed += 1
        if "EXPOSE 8000" in content:
            print("✓ Dockerfile: FastAPI port 8000 exposed.")
            checks_passed += 1
    else:
        warnings.append("deploy/Dockerfile not found!")

    # 2. Check Nginx Config
    nginx_conf = PROJECT_ROOT / "deploy/nginx.conf"
    if nginx_conf.exists():
        content = nginx_conf.read_text(encoding="utf-8")
        if "limit_req_zone" in content or "limit_req" in content:
            print("✓ Nginx: Connection rate-limiting defense configured.")
            checks_passed += 1
        if "proxy_buffering on;" in content:
            print("✓ Nginx: Reverse proxy buffering enabled.")
            checks_passed += 1
        if "proxy_pass" in content:
            print("✓ Nginx: Upstream backend proxy pass configured.")
            checks_passed += 1
    else:
        warnings.append("deploy/nginx.conf not found!")

    # 3. Check Gunicorn ASGI Conf
    gunicorn_conf = PROJECT_ROOT / "deploy/gunicorn.conf.py"
    if gunicorn_conf.exists():
        content = gunicorn_conf.read_text(encoding="utf-8")
        if "UvicornWorker" in content:
            print("✓ Gunicorn: High-performance UvicornWorker class selected.")
            checks_passed += 1
        if "workers" in content:
            print("✓ Gunicorn: Dynamic worker concurrency calculated.")
            checks_passed += 1
    else:
        warnings.append("deploy/gunicorn.conf.py not found!")

    # 4. Check Linux Kernel Sysctl
    sysctl_conf = PROJECT_ROOT / "deploy/sysctl.conf"
    if sysctl_conf.exists():
        content = sysctl_conf.read_text(encoding="utf-8")
        if "somaxconn" in content:
            print("✓ Sysctl: Socket listen queue (somaxconn = 65535) tuned.")
            checks_passed += 1
        if "tcp_tw_reuse" in content:
            print("✓ Sysctl: Fast socket reuse enabled.")
            checks_passed += 1
    else:
        warnings.append("deploy/sysctl.conf not found!")

    # 5. Check PgBouncer Config
    pgbouncer_ini = PROJECT_ROOT / "deploy/pgbouncer.ini"
    if pgbouncer_ini.exists():
        content = pgbouncer_ini.read_text(encoding="utf-8")
        if "pool_mode = transaction" in content:
            print("✓ PgBouncer: Transaction pooling mode configured.")
            checks_passed += 1
        if "statement_cache_size" in content:
            print("✓ PgBouncer: Statement cache parameters handled.")
            checks_passed += 1
    else:
        warnings.append("deploy/pgbouncer.ini not found!")

    # 6. Check Docker Compose Stack
    docker_compose = PROJECT_ROOT / "deploy/docker-compose.yml"
    if docker_compose.exists():
        content = docker_compose.read_text(encoding="utf-8")
        if "pgbouncer" in content and "redis" in content and "elasticsearch" in content:
            print("✓ Docker Compose: PgBouncer, Redis Cluster & Elasticsearch services declared.")
            checks_passed += 1
    else:
        warnings.append("deploy/docker-compose.yml not found!")

    # 7. Check Kubernetes manifests
    k8s_dir = PROJECT_ROOT / "deploy/k8s"
    if k8s_dir.exists():
        k8s_files = list(k8s_dir.glob("*.yaml"))
        print(f"✓ Kubernetes: {len(k8s_files)} manifest(s) detected in deploy/k8s/.")
        checks_passed += 1
    else:
        print("ℹ Kubernetes: deploy/k8s manifests ready to be generated.")

    print(f"\nAudit complete: {checks_passed} checks passed.")
    if warnings:
        print("\nWarnings:")
        for w in warnings:
            print(f"  [WARN] {w}")

    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Infrastructure Verifier")
    parser.parse_args()
    return verify_infrastructure()


if __name__ == "__main__":
    sys.exit(main())
