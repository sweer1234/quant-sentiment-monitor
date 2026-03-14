from __future__ import annotations

from fastapi.testclient import TestClient

from quant_sentiment_monitor.api import app, settings


client = TestClient(app)
TOKEN = {"Authorization": f"Bearer {settings.public_api_token}"}


def _login_headers(username: str = "demo", password: str = "demo123") -> dict[str, str]:
    response = client.post("/api/v1/auth/login", json={"username": username, "password": password})
    assert response.status_code == 200
    access_token = response.json()["access_token"]
    return {"Authorization": f"Bearer {access_token}"}


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


def test_user_topic_portfolio_alert_and_sla_flow() -> None:
    headers = _login_headers()
    admin_headers = _login_headers(username="sweer1234", password="dev123")
    me = client.get("/api/v1/users/me", headers=headers)
    assert me.status_code == 200
    assert me.json()["username"] == "demo"

    pref = client.put(
        "/api/v1/users/me/preferences",
        headers=headers,
        json={
            "focus_domains": ["macro_policy"],
            "focus_keywords": ["加息", "减产"],
            "focus_markets": ["fx", "futures"],
            "focus_instruments": ["DXY", "CL"],
            "alert_level_min": "P1",
        },
    )
    assert pref.status_code == 200

    feed = client.get("/api/v1/users/me/feed?page=1&page_size=10", headers=headers)
    assert feed.status_code == 200
    assert "events" in feed.json()

    topics = client.get("/api/v1/topics/catalog")
    assert topics.status_code == 200
    topic_ids = [item["topic_id"] for item in topics.json()["topics"][:1]]
    sub = client.put("/api/v1/users/me/topic-subscriptions", headers=headers, json={"topic_ids": topic_ids})
    assert sub.status_code == 200

    topic_feed = client.get("/api/v1/users/me/topic-feed?page=1&page_size=5", headers=headers)
    assert topic_feed.status_code == 200

    domains = client.get("/api/v1/domains/catalog")
    assert domains.status_code == 200
    assert domains.json()["total"] >= 1

    event_id = client.get("/api/v1/events/feed?page=1&page_size=1").json()["events"][0]["event_id"]
    credibility = client.get(f"/api/v1/events/{event_id}/credibility")
    assert credibility.status_code == 200

    portfolio = client.post(
        "/api/v1/portfolio/impact",
        headers=headers,
        json={
            "portfolio_id": "p_demo",
            "holdings": [{"instrument": "DXY", "weight": 0.3}, {"instrument": "CL", "weight": 0.4}],
            "event_ids": [],
        },
    )
    assert portfolio.status_code == 200
    assert "net_impact_score" in portfolio.json()

    policies = client.get("/api/v1/alerts/policies", headers=headers)
    assert policies.status_code == 200

    update = client.put(
        "/api/v1/alerts/policies",
        headers=admin_headers,
        json={"dedup_window_minutes": 30, "allow_revoke": True},
    )
    assert update.status_code == 200
    assert update.json()["dedup_window_minutes"] == 30

    revoke = client.post("/api/v1/alerts/alert_test_001/revoke?reason=test", headers=admin_headers)
    assert revoke.status_code == 200
    assert revoke.json()["status"] == "revoked"

    compliance = client.get("/api/v1/sources/federal_reserve/compliance")
    assert compliance.status_code == 200

    feedback = client.post(
        f"/api/v1/feedback/events/{event_id}",
        headers=headers,
        json={"feedback_type": "helpful", "score": 5, "comment": "good"},
    )
    assert feedback.status_code == 200

    billing = client.get("/api/v1/billing/usage?tenant_id=t001&period=2026-03", headers=headers)
    assert billing.status_code == 200
    sla = client.get("/api/v1/sla/status?tenant_id=t001", headers=headers)
    assert sla.status_code == 200

    calendar = client.get("/api/v1/calendar/events?country=US&importance_min=P1")
    assert calendar.status_code == 200
    assert calendar.json()["total"] >= 1
    calendar_id = calendar.json()["events"][0]["calendar_event_id"]
    surprise = client.get(f"/api/v1/calendar/events/{calendar_id}/surprise")
    assert surprise.status_code == 200

    upsert_calendar = client.post(
        "/api/v1/calendar/events",
        headers=admin_headers,
        json={
            "country": "CN",
            "event_name": "NBS PMI",
            "importance_level": "P1",
            "consensus": 50.1,
            "actual": 50.4,
            "unit": "%",
        },
    )
    assert upsert_calendar.status_code == 200

    created_webhook = client.post(
        "/api/v1/webhooks/subscriptions",
        headers=admin_headers,
        json={
            "name": "test-webhook",
            "url": "https://example.com/webhook",
            "events": ["event.created", "alert.triggered"],
            "enabled": True,
        },
    )
    assert created_webhook.status_code == 200
    webhook_id = created_webhook.json()["subscription_id"]
    list_webhook = client.get("/api/v1/webhooks/subscriptions", headers=admin_headers)
    assert list_webhook.status_code == 200
    dispatch = client.post("/api/v1/webhooks/dispatch-test", headers=admin_headers)
    assert dispatch.status_code == 200
    delete_webhook = client.delete(f"/api/v1/webhooks/subscriptions/{webhook_id}", headers=admin_headers)
    assert delete_webhook.status_code == 200
