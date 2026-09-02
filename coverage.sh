#!/usr/bin/env bash
set -euo pipefail

uv run pytest tests/unit --cov=trpc_service --cov-report=term-missing --cov-report=html
npm run test:coverage --prefix web-console
