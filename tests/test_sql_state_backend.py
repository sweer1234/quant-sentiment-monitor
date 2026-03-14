from __future__ import annotations

from pathlib import Path

from quant_sentiment_monitor.settings import Settings
from quant_sentiment_monitor.store import QuantStore


def test_sql_state_backend_roundtrip(tmp_path: Path) -> None:
    db_path = tmp_path / "qsm_state.db"
    settings = Settings(
        state_backend="sql",
        database_url=f"sqlite:///{db_path}",
        state_sql_table="qsm_state_test",
        state_path=str(tmp_path / "unused_state.json"),
    )
    store = QuantStore(settings=settings)
    result = store.ingest_event(
        {
            "source_id": "federal_reserve",
            "title": "SQL backend persistence test",
            "content": "testing sql state backend persistence",
            "event_type": "test_event",
        }
    )
    event_id = result["event"]["event_id"]
    assert event_id in store.events

    reloaded = QuantStore(settings=settings)
    assert event_id in reloaded.events

