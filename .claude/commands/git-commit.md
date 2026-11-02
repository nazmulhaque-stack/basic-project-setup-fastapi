---
description: Safely stage-check, secret-scan, and commit with a Conventional Commits message
argument-hint: "feat(scope): concise description"
allowed-tools: Bash(python3 .agents/common/skills/git_helper.py*), Bash(git status*), Bash(git diff*)
---

1. Confirm no `.env`/`.key`/`.pem`/venv files are staged:
   ```bash
   python3 .agents/common/skills/git_helper.py status
   ```
2. Commit (this also runs the full pytest suite first and validates the Conventional Commits format; it fails closed on either):
   ```bash
   python3 .agents/common/skills/git_helper.py commit -m "$ARGUMENTS"
   ```

Only run step 2 after the user has confirmed they want to commit.
