from __future__ import annotations

from fastapi.testclient import TestClient

from quant_sentiment_monitor.api import app, settings


client = TestClient(app)
TOKEN = {"Authorization": f"Bearer {settings.public_api_token}"}


def test_health() -> None:
    response = client.get("/api/v1/health")
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["sources"] > 0


def test_sources_list_and_patch() -> None:
    response = client.get("/api/v1/sources?enabled=true")
    assert response.status_code == 200
    assert response.json()["total"] > 0

    patched = client.patch(
        "/api/v1/sources/new_source_for_test",
        headers=TOKEN,
        json={"enabled": True, "source_weight": 0.91, "region": "US"},
    )
    assert patched.status_code == 200
    assert patched.json()["source_id"] == "new_source_for_test"


def test_event_feed_and_impact() -> None:
    feed = client.get("/api/v1/events/feed?page=1&page_size=5")
    assert feed.status_code == 200
    payload = feed.json()
    assert payload["total"] >= 1
    event_id = payload["events"][0]["event_id"]

    impact = client.get(f"/api/v1/events/{event_id}/impact")
    assert impact.status_code == 200
    assert impact.json()["event_id"] == event_id


def test_sentiment_and_signal() -> None:
    sentiment = client.get("/api/v1/sentiment/DXY")
    assert sentiment.status_code == 200
    s_payload = sentiment.json()
    assert "sentiment_score" in s_payload

    signal = client.get("/api/v1/signals?symbol=DXY&interval=1m")
    assert signal.status_code == 200
    assert signal.json()["signal"] in {"BUY", "SELL", "HOLD"}


def test_impact_batch() -> None:
    response = client.post(
        "/api/v1/impact/batch",
        json={"request_id": "r1", "window": "4h", "instruments": ["DXY", "CL"]},
    )
    assert response.status_code == 200
    payload = response.json()
    assert len(payload["results"]) == 2


def test_manual_message_flow() -> None:
    create = client.post(
        "/api/v1/manual/messages",
        headers=TOKEN,
        json={
            "title": "传某产油国将临时减产，油价盘前异动",
            "content": "盘前市场流传 OPEC+ 某成员国可能追加减产，需持续跟踪官方确认。",
            "operator_id": "u_analyst_001",
            "operator_role": "analyst",
            "related_instruments": ["CL", "USDCAD"],
        },
    )
    assert create.status_code == 200
    record = create.json()
    manual_message_id = record["manual_message_id"]

    fetched = client.get(f"/api/v1/manual/messages/{manual_message_id}")
    assert fetched.status_code == 200
    assert fetched.json()["manual_message_id"] == manual_message_id

    reviewed = client.post(
        f"/api/v1/manual/messages/{manual_message_id}/review",
        headers=TOKEN,
        json={"action": "approve", "review_comment": "已核验"},
    )
    assert reviewed.status_code == 200
    assert reviewed.json()["status"] == "approved"

    reevaluate = client.post(
        f"/api/v1/manual/messages/{manual_message_id}/re-evaluate",
        headers=TOKEN,
    )
    assert reevaluate.status_code == 200
