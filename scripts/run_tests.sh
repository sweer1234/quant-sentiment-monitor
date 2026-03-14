#!/usr/bin/env bash
set -euo pipefail

echo "[0/4] Bootstrapping environment"
bash scripts/bootstrap_cloud_env.sh --quiet

echo "[1/4] Running lint"
python3 -m ruff check .

echo "[2/4] Running unit/integration tests"
python3 -m pytest

echo "[3/4] Running pipeline smoke"
python3 scripts/run_pipeline.py

echo "[4/4] Running backtest smoke"
python3 scripts/run_backtest.py --start 2024-01-01 --end 2024-01-31 --symbol AAPL --strategy baseline_sentiment

echo "All tests/smokes completed."
