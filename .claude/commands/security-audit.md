---
description: Run the security & architecture audit (secrets, SQL injection, AAD binding, transaction leaks)
allowed-tools: Bash(python3 .agents/agents/reviewer-agent/skills/security_audit.py)
---

```bash
python3 .agents/agents/reviewer-agent/skills/security_audit.py
```

Summarize findings against `.agents/rules/security.md`. Report a verdict — APPROVED / CHANGES_REQUESTED / BLOCKED — per `.agents/agents/reviewer-agent/agent.md`. For a deeper pass, delegate to the `reviewer-agent` subagent instead.
