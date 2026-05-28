#!/usr/bin/env bash
# Runs the balancer's quality gates: tests + coverage threshold + fairness benchmark.
# Designed to be paste-able into PR descriptions as evidence.

set -e
cd "$(dirname "$0")/.."

export PYTHONPATH="${PYTHONPATH:-.}"
export OPENAI_API_KEY="${OPENAI_API_KEY:-sk-test-dummy}"
export BALANCER_TRACING="${BALANCER_TRACING:-false}"
export K8S_ENABLED="${K8S_ENABLED:-false}"

echo "=== Balancer tests ==="
python -m pytest tests/balancer/ -v

echo ""
echo "=== Coverage gate (>=90%) ==="
# pytest-cov is a required dev dependency. Surface the missing-dep error
# loudly rather than silently skipping; the gate must be enforceable.
if ! python -c "import pytest_cov" 2>/dev/null; then
    echo "ERROR: pytest-cov is required. Install with: uv pip install pytest-cov"
    exit 1
fi
python -m pytest tests/balancer/ \
  --cov=router.src.balancer \
  --cov-report=term-missing \
  --cov-fail-under=90 \
  -q

echo ""
echo "=== Fairness benchmark (Gini at 1000 picks) ==="
python -m pytest tests/balancer/test_fairness_benchmark.py -v

echo ""
echo "=== ALL GATES PASSED ==="
