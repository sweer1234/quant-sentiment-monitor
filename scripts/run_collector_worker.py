#!/usr/bin/env python3
from __future__ import annotations

import argparse
import time
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from quant_sentiment_monitor.collector import run_collection_once
from quant_sentiment_monitor.settings import Settings
from quant_sentiment_monitor.store import QuantStore
from quant_sentiment_monitor.task_queue import build_task_queue


def main() -> int:
    parser = argparse.ArgumentParser(description="Run collector task worker.")
    parser.add_argument("--max-tasks", type=int, default=1, help="Max tasks per poll cycle")
    parser.add_argument("--poll-interval-sec", type=int, default=5)
    parser.add_argument("--once", action="store_true", help="Process once and exit")
    args = parser.parse_args()

    settings = Settings()
    store = QuantStore(settings=settings)
    queue = build_task_queue(settings=settings)

    while True:
        tasks = queue.pop_many(max_items=max(1, args.max_tasks))
        processed = 0
        for task in tasks:
            if str(task.get("kind")) != "collector.run_once":
                continue
            limit = int(task.get("limit", 20))
            retries = int(task.get("retries", 2))
            run_result = run_collection_once(store=store, limit=limit, retries=retries)
            processed += 1
            print({"task_id": task.get("task_id"), "result": run_result})
        if args.once:
            break
        if processed == 0:
            time.sleep(max(1, args.poll_interval_sec))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

