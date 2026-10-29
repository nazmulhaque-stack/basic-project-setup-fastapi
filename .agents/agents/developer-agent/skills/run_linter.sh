#!/usr/bin/env bash
# ==============================================================================
# Developer Quality & Static Analysis Pipeline
# Executes bytecode compilation, secret scanning, and automated unit tests.
# ==============================================================================

set -euo pipefail

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
cd "$PROJECT_ROOT"

# Resolve Python binary (prefer virtualenv)
if [ -f "$PROJECT_ROOT/.venv/bin/python3" ]; then
    PYTHON_BIN="$PROJECT_ROOT/.venv/bin/python3"
    PYTEST_BIN="$PROJECT_ROOT/.venv/bin/pytest"
else
    PYTHON_BIN="python3"
    PYTEST_BIN="pytest"
fi

echo -e "${BLUE}======================================================${NC}"
echo -e "${BLUE}  Starting Developer Agent Static & Quality Pipeline  ${NC}"
echo -e "${BLUE}======================================================${NC}"

# 1. Bytecode Compilation Check
echo -e "\n${YELLOW}[1/3] Compiling Python source code...${NC}"
if "$PYTHON_BIN" -m compileall -q app tests; then
    echo -e "${GREEN}✓ All Python files compiled with zero syntax errors.${NC}"
else
    echo -e "${RED}✗ Syntax errors detected during compilation!${NC}"
    exit 1
fi

# 2. Secret & Staged File Inspection
echo -e "\n${YELLOW}[2/3] Checking Git staging area for sensitive leaks...${NC}"
if "$PYTHON_BIN" .agents/common/skills/git_helper.py check-secrets; then
    echo -e "${GREEN}✓ No sensitive or binary files staged in Git.${NC}"
else
    echo -e "${RED}✗ Blocked sensitive files detected in Git staging area!${NC}"
    exit 1
fi

# 3. Test Suite Execution
echo -e "\n${YELLOW}[3/3] Running asynchronous test suite via pytest...${NC}"
if "$PYTEST_BIN" -v; then
    echo -e "\n${GREEN}======================================================${NC}"
    echo -e "${GREEN}  ✓ Quality Pipeline Passed! Ready for Code Review.  ${NC}"
    echo -e "${GREEN}======================================================${NC}"
    exit 0
else
    echo -e "\n${RED}======================================================${NC}"
    echo -e "${RED}  ✗ Test suite failed! Please resolve broken tests.  ${NC}"
    echo -e "${RED}======================================================${NC}"
    exit 1
fi
