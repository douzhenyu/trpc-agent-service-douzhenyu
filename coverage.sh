#!/usr/bin/env bash
set -euo pipefail

uv run pytest tests/unit \
  --cov=trpc_service.version \
  --cov=dev.fake_external.scenarios \
  --cov-branch \
  --cov-fail-under=80 \
  --cov-report=term-missing \
  --cov-report=html
npm run test:coverage --prefix web-console
