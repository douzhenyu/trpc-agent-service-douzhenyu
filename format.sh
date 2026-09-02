#!/usr/bin/env bash
set -euo pipefail

uv run ruff format .
uv run ruff check --fix .
npm run format --prefix web-console
