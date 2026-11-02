#!/usr/bin/env python3
"""PreToolUse hook: block `git commit` when staged files trip the secret/venv patterns.

Only runs the (git-diff-based) check when the Bash command actually invokes
`git commit` instead of on every Bash call, keeping normal tool use quiet.
Exit 2 blocks the tool call and surfaces stderr back to Claude.
"""
from __future__ import annotations

import json
import re
import subprocess
import sys

COMMIT_RE = re.compile(r"\bgit\s+commit\b")


def main() -> int:
    try:
        payload = json.load(sys.stdin)
    except Exception:
        return 0

    command = payload.get("tool_input", {}).get("command", "")
    if not COMMIT_RE.search(command):
        return 0

    result = subprocess.run(
        ["python3", ".agents/common/skills/git_helper.py", "check-secrets"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        sys.stderr.write(result.stderr or result.stdout or "Secret leak check failed.\n")
        return 2

    return 0


if __name__ == "__main__":
    sys.exit(main())
