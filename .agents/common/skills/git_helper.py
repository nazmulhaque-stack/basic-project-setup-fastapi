#!/usr/bin/env python3
"""Shared Git Utility Helper for Agentic Multi-Agent Workflows.

Provides automated Git staging inspection, pre-commit secret leak prevention,
conventional commit message validation, and test suite health checks.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import re
import subprocess
import sys
from typing import List, Tuple

# Sensitive files and directories that must NEVER be staged or committed
BLOCKED_PATTERNS: List[re.Pattern[str]] = [
    re.compile(r"^\.env$"),
    re.compile(r"^\.env\..+$"),
    re.compile(r".*\.pem$"),
    re.compile(r".*\.key$"),
    re.compile(r"^.*__pycache__/.*$"),
    re.compile(r"^\.venv/.*$"),
    re.compile(r"^\.pytest_cache/.*$"),
    re.compile(r"^id_rsa.*$"),
]

# Conventional commit message regex pattern
CONVENTIONAL_COMMIT_REGEX = re.compile(
    r"^(feat|fix|docs|style|refactor|perf|test|build|ci|chore|revert)(\([a-z0-9_-]+\))?:\s.+",
    re.IGNORECASE,
)


def run_command(cmd: List[str]) -> Tuple[int, str, str]:
    """Executes a subprocess command safely returning exit code, stdout, and stderr."""
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return proc.returncode, proc.stdout.strip(), proc.stderr.strip()


def get_staged_files() -> List[str]:
    """Retrieves list of currently staged files in Git."""
    code, stdout, _ = run_command(["git", "diff", "--cached", "--name-only"])
    if code != 0 or not stdout:
        return []
    return [line.strip() for line in stdout.splitlines() if line.strip()]


def check_secrets_and_cleanliness() -> Tuple[bool, List[str]]:
    """Inspects staged files against blocked security patterns."""
    staged = get_staged_files()
    violations: List[str] = []

    for file_path in staged:
        for pattern in BLOCKED_PATTERNS:
            if pattern.search(file_path):
                violations.append(f"Security Alert: Blocked sensitive/binary file staged: {file_path}")

    return len(violations) == 0, violations


def validate_commit_message(message: str) -> bool:
    """Verifies that the commit message adheres to Conventional Commits standards."""
    return bool(CONVENTIONAL_COMMIT_REGEX.match(message))


def run_quick_tests() -> bool:
    """Executes pytest suite to guarantee codebase stability prior to committing."""
    print("Running test suite validation...")
    code, stdout, stderr = run_command([sys.executable, "-m", "pytest", "-q"])
    if code != 0:
        print(f"Test suite failed with exit code {code}:\n{stdout}\n{stderr}", file=sys.stderr)
        return False
    print("Test suite verification passed.")
    return True


def cmd_status() -> int:
    """Prints current branch, staged files, and security check status."""
    code, branch, _ = run_command(["git", "rev-parse", "--abbrev-ref", "HEAD"])
    staged = get_staged_files()
    clean, violations = check_secrets_and_cleanliness()

    print(f"Git Branch : {branch if code == 0 else 'Unknown'}")
    print(f"Staged Files ({len(staged)}):")
    for f in staged:
        print(f"  - {f}")

    if not clean:
        print("\n[FAILED] Security check violations detected:")
        for v in violations:
            print(f"  ! {v}")
        return 1

    print("\n[OK] Security check passed: No sensitive or virtualenv files staged.")
    return 0


def cmd_commit(message: str, skip_tests: bool = False) -> int:
    """Performs safety checks and executes git commit with a conventional message."""
    clean, violations = check_secrets_and_cleanliness()
    if not clean:
        print("[ERROR] Commit aborted due to sensitive files staged:", file=sys.stderr)
        for v in violations:
            print(f"  ! {v}", file=sys.stderr)
        return 1

    if not validate_commit_message(message):
        print(
            f"[ERROR] Invalid commit message: '{message}'.\n"
            "Must adhere to Conventional Commits (e.g., 'feat: add keyset cursor pagination').",
            file=sys.stderr,
        )
        return 1

    if not skip_tests:
        if not run_quick_tests():
            print("[ERROR] Commit aborted: Unit tests failed.", file=sys.stderr)
            return 1

    code, stdout, stderr = run_command(["git", "commit", "-m", message])
    if code != 0:
        print(f"[ERROR] git commit failed:\n{stderr or stdout}", file=sys.stderr)
        return code

    print(f"[SUCCESS] Committed successfully: {stdout}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Shared Git Helper Tool for Multi-Agent Workflows")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # status
    subparsers.add_parser("status", help="Inspect staged files and security checks")

    # check-secrets
    subparsers.add_parser("check-secrets", help="Check for leaked secrets in staged files")

    # commit
    commit_parser = subparsers.add_parser("commit", help="Commit with conventional commit validation")
    commit_parser.add_argument("-m", "--message", required=True, help="Conventional commit message")
    commit_parser.add_argument("--skip-tests", action="store_true", help="Bypass test suite execution")

    args = parser.parse_args()

    if args.command == "status":
        return cmd_status()
    elif args.command == "check-secrets":
        clean, violations = check_secrets_and_cleanliness()
        if not clean:
            for v in violations:
                print(v, file=sys.stderr)
            print('{"decision": "deny", "reason": "Sensitive files staged"}')
            return 1
        print('{"decision": "allow"}')
        return 0
    elif args.command == "commit":
        return cmd_commit(args.message, skip_tests=args.skip_tests)

    return 0


if __name__ == "__main__":
    sys.exit(main())
