#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
import sys

import uvicorn

ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))


def main() -> None:
    uvicorn.run("model_service.main:app", host="0.0.0.0", port=9000, reload=False)


if __name__ == "__main__":
    main()

