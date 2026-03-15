#!/usr/bin/env bash
set -euo pipefail

USER_BIN="$(python3 -c 'import site; print(site.USER_BASE + "/bin")')"
if [[ -n "${USER_BIN}" && ":${PATH}:" != *":${USER_BIN}:"* ]]; then
  export PATH="${USER_BIN}:${PATH}"
fi

echo "[0/1] Bootstrapping environment"
bash scripts/bootstrap_cloud_env.sh --quiet

python3 -m pytest --cov=src/quant_sentiment_monitor --cov-report=term-missing --cov-fail-under=75
