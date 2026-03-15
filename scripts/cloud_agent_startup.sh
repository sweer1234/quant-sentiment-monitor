#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

echo "[startup] Ensuring Python runtime dependencies ..."
bash scripts/bootstrap_cloud_env.sh --quiet

echo "[startup] Verifying critical packages ..."
python3 - <<'PY'
import sqlalchemy
import psycopg
import redis
import pytest
import ruff
print("ok")
PY

echo "[startup] Environment ready."
echo "[startup] Optional services:"
echo "  - model service: python3 scripts/run_model_service.py"
echo "  - observability: docker compose --profile observability up -d prometheus grafana loki promtail"
