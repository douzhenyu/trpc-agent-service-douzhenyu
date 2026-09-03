#!/usr/bin/env bash
set -euo pipefail

uv sync --frozen
npm ci --prefix web-console
npm run build --prefix web-console
docker compose build
