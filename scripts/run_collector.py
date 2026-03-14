#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from quant_sentiment_monitor.collector import run_collection_once
from quant_sentiment_monitor.settings import Settings
from quant_sentiment_monitor.store import QuantStore


def main() -> int:
    parser = argparse.ArgumentParser(description="Run source polling collector once.")
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--retries", type=int, default=1)
    args = parser.parse_args()

    settings = Settings()
    store = QuantStore(settings=settings)
    result = run_collection_once(store=store, limit=args.limit, retries=args.retries)
    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

