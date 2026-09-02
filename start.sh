#!/usr/bin/env bash
set -euo pipefail

docker compose up --detach --build --wait

echo "Web Console: http://localhost:4173"
echo "Admin API:   http://localhost:8000/api/v1/health"
