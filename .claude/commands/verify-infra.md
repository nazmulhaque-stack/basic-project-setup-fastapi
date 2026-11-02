---
description: Verify Docker, Nginx, Gunicorn, sysctl, PgBouncer, docker-compose, and K8s manifests
allowed-tools: Bash(python3 .agents/agents/system-architect-agent/skills/verify_infrastructure.py)
---

```bash
python3 .agents/agents/system-architect-agent/skills/verify_infrastructure.py
```

Summarize any failing checks against `/AGENTS.md` sections 2 and 6. For infra changes (not just verification), delegate to the `system-architect-agent` subagent instead.
