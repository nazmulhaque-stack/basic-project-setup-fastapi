---
name: verify-infrastructure
description: >-
  Validates Dockerfile, Nginx reverse proxy, Gunicorn ASGI worker tuning, Linux sysctl network settings, and Kubernetes manifests.
---

# Infrastructure & DevOps Verification Skill

Use this skill before deploying to production to ensure all containerization, reverse proxy, Linux kernel sysctl, and Kubernetes configurations pass verification.

## Execution

```bash
python3 .agents/agents/system-architect-agent/skills/verify_infrastructure.py
```

## Checks Performed
1. **Dockerfile**: Multi-stage build, minimal layer cache, exposed port 8000.
2. **Nginx**: Rate limiting zones, reverse proxy buffering, client body size.
3. **Gunicorn**: Uvicorn worker class selection, dynamic concurrency calculation ($2 \times \text{vCPU} + 1$).
4. **Linux Sysctl**: High-concurrency socket queues (`somaxconn = 65535`), `tcp_tw_reuse`.
5. **Kubernetes (deploy/k8s/)**: Deployment, Service, Ingress, HPA, and ConfigMap verification.
