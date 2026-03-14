#!/usr/bin/env bash
set -euo pipefail

echo "[1/3] Running unit/integration tests"
python3 -m pytest

echo "[2/3] Running pipeline smoke"
python3 scripts/run_pipeline.py

echo "[3/3] Running backtest smoke"
python3 scripts/run_backtest.py --start 2024-01-01 --end 2024-01-31 --symbol AAPL --strategy baseline_sentiment

echo "All tests/smokes completed."
