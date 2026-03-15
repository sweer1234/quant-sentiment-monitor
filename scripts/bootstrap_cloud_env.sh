#!/usr/bin/env bash
set -euo pipefail

QUIET=0
CHECK_ONLY=0
if [[ "${1:-}" == "--quiet" ]]; then
  QUIET=1
elif [[ "${1:-}" == "--check-only" ]]; then
  CHECK_ONLY=1
fi

check_runtime() {
  python3 - <<'PY'
import importlib
import subprocess
import sys

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

subprocess.check_call([sys.executable, "-m", "pytest", "--version"], stdout=subprocess.DEVNULL)
subprocess.check_call([sys.executable, "-m", "ruff", "--version"], stdout=subprocess.DEVNULL)
subprocess.check_call(["pytest", "--version"], stdout=subprocess.DEVNULL)
subprocess.check_call(["ruff", "--version"], stdout=subprocess.DEVNULL)
print("ok")
PY
}

if check_runtime >/dev/null 2>&1; then
  if [[ "${QUIET}" -ne 1 ]]; then
    echo "[bootstrap] Python dependencies already satisfied."
  fi
  exit 0
fi

if [[ "${CHECK_ONLY}" -eq 1 ]]; then
  echo "[bootstrap] Missing required dependencies."
  exit 1
fi

if [[ "${QUIET}" -ne 1 ]]; then
  echo "[bootstrap] Installing dependencies from requirements-dev.txt ..."
fi
python3 -m pip install -U pip
python3 -m pip install -r requirements-dev.txt

check_runtime >/dev/null
if [[ "${QUIET}" -ne 1 ]]; then
  echo "[bootstrap] Done."
fi

