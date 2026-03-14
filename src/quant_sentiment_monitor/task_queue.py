from __future__ import annotations

import json
from typing import Any, Protocol
from uuid import uuid4

import redis

from .settings import Settings


class TaskQueue(Protocol):
    def enqueue(self, payload: dict[str, Any]) -> str:
        ...

    def pop_many(self, max_items: int = 1) -> list[dict[str, Any]]:
        ...

    def size(self) -> int:
        ...

    def backend_name(self) -> str:
        ...


class LocalTaskQueue:
    def __init__(self) -> None:
        self._queue: list[dict[str, Any]] = []

    def enqueue(self, payload: dict[str, Any]) -> str:
        task_id = f"task_{uuid4().hex[:10]}"
        self._queue.append({"task_id": task_id, **payload})
        return task_id

    def pop_many(self, max_items: int = 1) -> list[dict[str, Any]]:
        items = self._queue[:max_items]
        self._queue = self._queue[max_items:]
        return items

    def size(self) -> int:
        return len(self._queue)

    def backend_name(self) -> str:
        return "local"


class RedisTaskQueue:
    def __init__(self, redis_url: str, queue_key: str):
        self.client = redis.Redis.from_url(redis_url, decode_responses=True)
        self.queue_key = queue_key

    def enqueue(self, payload: dict[str, Any]) -> str:
        task_id = f"task_{uuid4().hex[:10]}"
        record = {"task_id": task_id, **payload}
        self.client.rpush(self.queue_key, json.dumps(record, ensure_ascii=False))
        return task_id

    def pop_many(self, max_items: int = 1) -> list[dict[str, Any]]:
        rows = []
        for _ in range(max_items):
            item = self.client.lpop(self.queue_key)
            if item is None:
                break
            try:
                rows.append(json.loads(item))
            except Exception:
                continue
        return rows

    def size(self) -> int:
        return int(self.client.llen(self.queue_key))

    def backend_name(self) -> str:
        return "redis"


def build_task_queue(settings: Settings) -> TaskQueue:
    backend = settings.queue_backend.lower().strip()
    if backend == "redis":
        return RedisTaskQueue(redis_url=settings.queue_redis_url, queue_key=settings.collector_task_queue_key)
    return LocalTaskQueue()

