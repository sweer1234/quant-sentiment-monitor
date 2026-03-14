#!/usr/bin/env bash
set -euo pipefail

python3 -m pytest --cov=src/quant_sentiment_monitor --cov-report=term-missing --cov-fail-under=75
