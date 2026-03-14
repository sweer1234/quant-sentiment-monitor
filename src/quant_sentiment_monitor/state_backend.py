from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Protocol

from sqlalchemy import Column, DateTime, Integer, MetaData, Table, Text, create_engine, select
from sqlalchemy.sql import func

from .settings import Settings


class StateBackend(Protocol):
    def load(self) -> dict[str, Any] | None:
        ...

    def save(self, payload: dict[str, Any]) -> None:
        ...


class FileStateBackend:
    def __init__(self, path: Path):
        self.path = path

    def load(self) -> dict[str, Any] | None:
        if not self.path.exists():
            return None
        try:
            return json.loads(self.path.read_text(encoding="utf-8"))
        except Exception:
            return None

    def save(self, payload: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


class SQLStateBackend:
    def __init__(self, database_url: str, table_name: str = "qsm_state"):
        self.engine = create_engine(database_url, future=True, pool_pre_ping=True)
        self.metadata = MetaData()
        self.table = Table(
            table_name,
            self.metadata,
            Column("state_id", Integer, primary_key=True),
            Column("payload", Text, nullable=False),
            Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()),
        )
        self.metadata.create_all(self.engine)

    def load(self) -> dict[str, Any] | None:
        with self.engine.begin() as conn:
            row = conn.execute(select(self.table.c.payload).where(self.table.c.state_id == 1)).first()
        if not row:
            return None
        try:
            return json.loads(str(row.payload))
        except Exception:
            return None

    def save(self, payload: dict[str, Any]) -> None:
        raw = json.dumps(payload, ensure_ascii=False)
        with self.engine.begin() as conn:
            existing = conn.execute(select(self.table.c.state_id).where(self.table.c.state_id == 1)).first()
            if existing:
                conn.execute(
                    self.table.update().where(self.table.c.state_id == 1).values(payload=raw, updated_at=func.now())
                )
            else:
                conn.execute(self.table.insert().values(state_id=1, payload=raw))


def build_state_backend(settings: Settings) -> StateBackend:
    backend = settings.state_backend.lower().strip()
    if backend == "sql":
        return SQLStateBackend(database_url=settings.database_url, table_name=settings.state_sql_table)
    return FileStateBackend(path=settings.state_path_obj)

