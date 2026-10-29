---
name: git-helper
description: >-
  Inspects Git staging status, blocks secret leaks, validates Conventional Commits, and checks test suite health.
---

# Git Helper & Pre-Commit Skill

Use this skill when staging files, verifying that no sensitive files are leaked, checking commit message formats, or preparing a clean commit.

## Usage Commands

- **Inspect Staging Status**:
  ```bash
  python3 .agents/common/skills/git_helper.py status
  ```
- **Check for Leaked Secrets**:
  ```bash
  python3 .agents/common/skills/git_helper.py check-secrets
  ```
- **Commit Safely with Conventional Commits**:
  ```bash
  python3 .agents/common/skills/git_helper.py commit -m "feat(scope): concise message"
  ```
