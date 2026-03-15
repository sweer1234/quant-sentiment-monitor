#!/usr/bin/env bash
set -euo pipefail

echo "[0/1] Bootstrapping environment"
bash scripts/bootstrap_cloud_env.sh --quiet

python3 -m pytest --cov=src/quant_sentiment_monitor --cov-report=term-missing --cov-fail-under=75
