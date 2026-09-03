#!/usr/bin/env bash
set -euo pipefail

docker compose down --volumes --remove-orphans
rm -rf .mypy_cache .pytest_cache .ruff_cache .venv htmlcov
rm -rf web-console/coverage web-console/dist web-console/node_modules web-console/test-results
find trpc_service tests dev -type d -name __pycache__ -prune -exec rm -rf {} +
