from __future__ import annotations

from pathlib import Path

from quant_sentiment_monitor.settings import Settings
from quant_sentiment_monitor.store import QuantStore


def test_store_state_persistence_roundtrip(tmp_path: Path) -> None:
    state_file = tmp_path / "state_roundtrip.json"
    settings = Settings(state_path=str(state_file))
    store = QuantStore(settings=settings)

    created = store.ingest_event(
        {
            "source_id": "federal_reserve",
            "title": "Roundtrip test event",
            "content": "Testing persistence for event and alert pipeline.",
            "event_type": "test_event",
            "related_instruments": ["DXY"],
        }
    )
    event_id = created["event"]["event_id"]
    alert = created["alert"]
    assert alert is not None
    alert_id = alert["alert_id"]

    store.create_webhook_subscription(
        username="sweer1234",
        payload={
            "name": "roundtrip-hook",
            "url": "https://example.com/webhook",
            "events": ["event.created"],
            "enabled": True,
        },
    )
    store.dispatch_webhook_test(event_id=event_id)
    store.process_webhook_queue(limit=50, ignore_schedule=True)
    store.ack_alert(alert_id=alert_id, username="sweer1234", note="roundtrip-ack")

    # Rebuild store from same state path and verify key entities are recovered.
    restored = QuantStore(settings=settings)
    assert event_id in restored.events
    assert alert_id in restored.alerts
    assert restored.alerts[alert_id]["status"] == "acked"
    assert len(restored.webhook_subscriptions) >= 1
    assert len(restored.webhook_deliveries) >= 1
