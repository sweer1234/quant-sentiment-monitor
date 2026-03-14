#!/usr/bin/env bash
set -euo pipefail

QUIET=0
if [[ "${1:-}" == "--quiet" ]]; then
  QUIET=1
fi

if python3 - <<'PY'
import importlib
modules = [
    "fastapi",
    "uvicorn",
    "pydantic_settings",
    "yaml",
    "sqlalchemy",
    "psycopg",
    "redis",
    "pytest",
    "httpx",
    "pytest_cov",
    "ruff",
]
for m in modules:
    importlib.import_module(m)
print("ok")
PY
then
  if [[ "${QUIET}" -ne 1 ]]; then
    echo "[bootstrap] Python dependencies already satisfied."
  fi
  exit 0
fi

if [[ "${QUIET}" -ne 1 ]]; then
  echo "[bootstrap] Installing dependencies from requirements-dev.txt ..."
fi
python3 -m pip install -U pip
python3 -m pip install -r requirements-dev.txt

if [[ "${QUIET}" -ne 1 ]]; then
  echo "[bootstrap] Done."
fi

