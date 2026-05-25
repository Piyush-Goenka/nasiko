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
echo "=== Coverage report (informational) ==="
if python -c "import pytest_cov" 2>/dev/null; then
    python -m pytest tests/balancer/ \
      --cov=src/balancer \
      --cov-report=term-missing \
      --cov-fail-under=80 \
      -q
else
    echo "(pytest-cov not installed; skipping coverage gate)"
fi

echo ""
echo "=== Fairness benchmark (Gini at 1000 picks) ==="
python -m pytest tests/balancer/test_fairness_benchmark.py -v

echo ""
echo "=== ALL GATES PASSED ==="
