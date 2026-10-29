---
name: security-audit
description: >-
  Audits the codebase for hardcoded secrets, SQL injection risks, missing cryptographic AAD binding, and repository transaction leaks.
---

# Security & Architecture Audit Skill

Use this skill to audit the codebase for security vulnerabilities, secret leaks, raw SQL string interpolation, or repository layer transaction violations.

## Execution

Run the security audit script:
```bash
python3 .agents/agents/reviewer-agent/skills/security_audit.py
```

## Checks Performed
1. **Secret & Key Leaks**: Scans for hardcoded private keys, tokens, or plaintext passwords.
2. **SQL Injection Risks**: Detects dynamic string formatting inside `session.execute()` or `text()`.
3. **Cryptographic Context Binding**: Ensures all AES-256-GCM calls supply `aad=f"{table}.{field}:{id}"`.
4. **Transaction Decoupling**: Verifies that repositories never call `commit()` or `rollback()`.
