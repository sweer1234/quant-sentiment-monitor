from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

TEST_STATE_PATH = Path("data/test_state_api.json")
if TEST_STATE_PATH.exists():
    TEST_STATE_PATH.unlink()
os.environ["QSM_STATE_PATH"] = str(TEST_STATE_PATH)

from quant_sentiment_monitor.api import app, settings, store


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


def test_source_version_history_and_rollback() -> None:
    admin_headers = _login_headers(username="sweer1234", password="dev123")
    source_id = "source_version_demo"
    first_patch = client.patch(
        f"/api/v1/sources/{source_id}",
        headers=admin_headers,
        json={"enabled": True, "source_weight": 0.21, "region": "US"},
    )
    assert first_patch.status_code == 200
    second_patch = client.patch(
        f"/api/v1/sources/{source_id}",
        headers=admin_headers,
        json={"source_weight": 0.93, "timeliness_weight": 0.88},
    )
    assert second_patch.status_code == 200
    assert second_patch.json()["source_weight"] == 0.93

    versions = client.get(f"/api/v1/sources/{source_id}/versions?limit=20", headers=admin_headers)
    assert versions.status_code == 200
    payload = versions.json()
    assert payload["total"] >= 2
    assert len(payload["versions"]) >= 2
    latest_version = payload["versions"][0]
    assert "source_weight" in latest_version["changed_fields"]
    rollback_version_id = latest_version["version_id"]

    rollback = client.post(
        f"/api/v1/sources/{source_id}/rollback?version_id={rollback_version_id}",
        headers=admin_headers,
    )
    assert rollback.status_code == 200
    assert rollback.json()["source_weight"] == 0.21
    assert rollback.json()["effective_source_weight"] >= 0

    versions_after = client.get(f"/api/v1/sources/{source_id}/versions?limit=5", headers=admin_headers)
    assert versions_after.status_code == 200
    assert versions_after.json()["total"] >= payload["total"] + 1


def test_source_import_delete_and_signal_thresholds() -> None:
    admin_headers = _login_headers(username="sweer1234", password="dev123")
    import_resp = client.post(
        "/api/v1/sources/import?merge=true",
        headers=admin_headers,
        json={
            "sources": [
                {
                    "source_id": "import_demo_source",
                    "display_name": "Import Demo",
                    "enabled": True,
                    "url": "https://example.com",
                    "source_weight": 0.8,
                }
            ]
        },
    )
    assert import_resp.status_code == 200
    assert import_resp.json()["imported"] >= 1

    thresholds_before = client.get("/api/v1/signals/thresholds", headers=admin_headers)
    assert thresholds_before.status_code == 200
    put_thresholds = client.put(
        "/api/v1/signals/thresholds",
        headers=admin_headers,
        json={"buy_net_threshold": 15, "sell_net_threshold": -15},
    )
    assert put_thresholds.status_code == 200
    assert put_thresholds.json()["buy_net_threshold"] == 15
    assert put_thresholds.json()["sell_net_threshold"] == -15

    delete_resp = client.delete("/api/v1/sources/import_demo_source", headers=admin_headers)
    assert delete_resp.status_code == 200
    assert delete_resp.json()["status"] == "deleted"

    model_status = client.get("/api/v1/model/inference/status", headers=admin_headers)
    assert model_status.status_code == 200
    assert "backend" in model_status.json()

    quota_status = client.get("/api/v1/admin/quotas/users/demo", headers=admin_headers)
    assert quota_status.status_code == 200
    assert quota_status.json()["username"] == "demo"
    put_plan = client.put("/api/v1/admin/quotas/users/demo?plan=pro", headers=admin_headers)
    assert put_plan.status_code == 200
    assert put_plan.json()["plan"] == "pro"
    restore_plan = client.put("/api/v1/admin/quotas/users/demo?plan=basic", headers=admin_headers)
    assert restore_plan.status_code == 200


def test_collector_task_queue_endpoints() -> None:
    admin_headers = _login_headers(username="sweer1234", password="dev123")
    stats_before = client.get("/api/v1/collector/tasks/stats", headers=admin_headers)
    assert stats_before.status_code == 200
    assert "backend" in stats_before.json()

    enqueue = client.post(
        "/api/v1/collector/tasks/enqueue",
        headers=admin_headers,
        json={"limit": 2, "retries": 0},
    )
    assert enqueue.status_code == 200
    assert enqueue.json()["task_id"].startswith("task_")

    with patch(
        "quant_sentiment_monitor.collector._fetch_url",
        return_value=(
            "<rss><channel><item><title>Task queue item</title><description>desc</description></item></channel></rss>",
            None,
        ),
    ):
        processed = client.post("/api/v1/collector/tasks/process?max_tasks=5", headers=admin_headers)
    assert processed.status_code == 200
    assert processed.json()["processed"] >= 1


def test_event_quota_enforcement() -> None:
    demo_headers = _login_headers(username="demo", password="demo123")
    period = store._current_period()
    usage_key = f"demo:{period}"
    basic_quota = store.billing_sla_rules.get("tiers", {}).get("basic", {}).get("monthly_event_quota", 50000)
    store.billing_sla_rules.setdefault("tiers", {}).setdefault("basic", {})["monthly_event_quota"] = 1
    store.user_plans["demo"] = "basic"
    store.usage_counters.pop(usage_key, None)
    try:
        first = client.post(
            "/api/v1/events/ingest",
            headers=demo_headers,
            json={"source_id": "federal_reserve", "title": "quota test one", "content": "quota test one"},
        )
        assert first.status_code == 200
        second = client.post(
            "/api/v1/events/ingest",
            headers=demo_headers,
            json={"source_id": "federal_reserve", "title": "quota test two", "content": "quota test two"},
        )
        assert second.status_code == 422
        assert "quota exceeded" in second.json()["detail"]
    finally:
        store.billing_sla_rules.setdefault("tiers", {}).setdefault("basic", {})["monthly_event_quota"] = basic_quota
        store.usage_counters.pop(usage_key, None)


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

    draft_create = client.post(
        "/api/v1/manual/messages/draft",
        headers=TOKEN,
        json={
            "title": "盘中线索：某国央行可能临时沟通",
            "content": "值班同事反馈央行可能临时释出沟通，待确认。",
            "operator_id": "u_shift_001",
            "operator_role": "analyst",
            "related_instruments": ["DXY"],
        },
    )
    assert draft_create.status_code == 200
    draft_id = draft_create.json()["manual_message_id"]
    assert draft_create.json()["status"] == "draft"

    draft_submit = client.post(f"/api/v1/manual/messages/{draft_id}/submit", headers=TOKEN)
    assert draft_submit.status_code == 200
    assert draft_submit.json()["status"] == "auto_assessed"

    draft_review = client.post(
        f"/api/v1/manual/messages/{draft_id}/review",
        headers=TOKEN,
        json={"action": "approve", "review_comment": "通过"},
    )
    assert draft_review.status_code == 200
    assert draft_review.json()["status"] == "approved"

    draft_publish = client.post(f"/api/v1/manual/messages/{draft_id}/publish", headers=TOKEN)
    assert draft_publish.status_code == 200
    assert draft_publish.json()["status"] == "published"
    assert draft_publish.json()["linked_event_id"] is not None

    batch_create = client.post(
        "/api/v1/manual/messages/batch",
        headers=TOKEN,
        json={
            "as_draft": True,
            "messages": [
                {
                    "title": "批量线索1",
                    "content": "批量导入测试1",
                    "operator_id": "u_batch_1",
                    "operator_role": "analyst",
                },
                {
                    "title": "批量线索2",
                    "content": "批量导入测试2",
                    "operator_id": "u_batch_2",
                    "operator_role": "analyst",
                },
            ],
        },
    )
    assert batch_create.status_code == 200
    assert batch_create.json()["created"] == 2


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

    escalated = client.post("/api/v1/alerts/escalate?force=true&limit=50", headers=admin_headers)
    assert escalated.status_code == 200
    escalations = client.get("/api/v1/alerts/escalations?limit=20", headers=admin_headers)
    assert escalations.status_code == 200
    assert escalations.json()["total"] >= 1

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
    backfill_actual = client.post(
        f"/api/v1/calendar/events/{calendar_id}/actual",
        headers=admin_headers,
        json={"actual": 4.1, "consensus": 3.8, "note": "release update"},
    )
    assert backfill_actual.status_code == 200
    assert backfill_actual.json()["surprise"]["status"] == "available"

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
            "secret": "test-secret",
            "max_retries": 2,
        },
    )
    assert created_webhook.status_code == 200
    webhook_id = created_webhook.json()["subscription_id"]
    list_webhook = client.get("/api/v1/webhooks/subscriptions", headers=admin_headers)
    assert list_webhook.status_code == 200
    dispatch = client.post("/api/v1/webhooks/dispatch-test?force_fail=true", headers=admin_headers)
    assert dispatch.status_code == 200
    assert dispatch.json()["queued_subscriptions"] >= 1

    first_process = client.post("/api/v1/webhooks/queue/process?limit=20", headers=admin_headers)
    assert first_process.status_code == 200
    assert first_process.json()["processed"] >= 1

    retrying_deliveries = client.get("/api/v1/webhooks/deliveries?status=retrying", headers=admin_headers)
    assert retrying_deliveries.status_code == 200
    assert retrying_deliveries.json()["total"] >= 1
    first_delivery = retrying_deliveries.json()["deliveries"][0]
    assert first_delivery["subscription_id"] == webhook_id
    assert first_delivery["signature"] is not None
    assert first_delivery["can_retry"] is True

    second_process = client.post("/api/v1/webhooks/queue/process?limit=20&ignore_schedule=true", headers=admin_headers)
    assert second_process.status_code == 200

    delivered_after_process = client.get("/api/v1/webhooks/deliveries?status=delivered", headers=admin_headers)
    assert delivered_after_process.status_code == 200
    assert delivered_after_process.json()["total"] >= 1

    stats = client.get("/api/v1/webhooks/stats", headers=admin_headers)
    assert stats.status_code == 200
    assert "success_rate_pct" in stats.json()

    # Create a permanently failing webhook and test manual retry endpoint.
    failing_webhook = client.post(
        "/api/v1/webhooks/subscriptions",
        headers=admin_headers,
        json={
            "name": "always-fail",
            "url": "https://fail.example.com/hook",
            "events": ["event.created"],
            "enabled": True,
            "max_retries": 1,
            "rate_limit_per_minute": 1000,
        },
    )
    assert failing_webhook.status_code == 200
    failing_id = failing_webhook.json()["subscription_id"]
    client.post("/api/v1/webhooks/dispatch-test", headers=admin_headers)
    # Process twice to make the failing webhook reach failed status.
    client.post("/api/v1/webhooks/queue/process?limit=50&ignore_schedule=true", headers=admin_headers)
    client.post("/api/v1/webhooks/queue/process?limit=50&ignore_schedule=true", headers=admin_headers)
    dlq_deliveries = client.get(f"/api/v1/webhooks/deliveries?subscription_id={failing_id}&status=dlq", headers=admin_headers)
    assert dlq_deliveries.status_code == 200
    assert dlq_deliveries.json()["total"] >= 1

    dlq_items = client.get("/api/v1/webhooks/dlq?status=pending_replay", headers=admin_headers)
    assert dlq_items.status_code == 200
    assert dlq_items.json()["total"] >= 1

    replay = client.post("/api/v1/webhooks/dlq/replay?limit=10", headers=admin_headers)
    assert replay.status_code == 200
    assert replay.json()["replayed"] >= 1
    replay_process = client.post("/api/v1/webhooks/queue/process?limit=50&ignore_schedule=true", headers=admin_headers)
    assert replay_process.status_code == 200

    sub_stats = client.get("/api/v1/webhooks/stats/subscriptions?top_n=5", headers=admin_headers)
    assert sub_stats.status_code == 200
    assert sub_stats.json()["total"] >= 1

    throttled_webhook = client.post(
        "/api/v1/webhooks/subscriptions",
        headers=admin_headers,
        json={
            "name": "throttled-hook",
            "url": "https://example.com/throttle",
            "events": ["event.created"],
            "enabled": True,
            "rate_limit_per_minute": 1,
        },
    )
    assert throttled_webhook.status_code == 200
    throttled_id = throttled_webhook.json()["subscription_id"]
    client.post("/api/v1/webhooks/dispatch-test", headers=admin_headers)
    client.post("/api/v1/webhooks/dispatch-test", headers=admin_headers)
    throttled_process = client.post("/api/v1/webhooks/queue/process?limit=200&ignore_schedule=true", headers=admin_headers)
    assert throttled_process.status_code == 200
    throttled_deliveries = client.get(
        f"/api/v1/webhooks/deliveries?subscription_id={throttled_id}&status=throttled",
        headers=admin_headers,
    )
    assert throttled_deliveries.status_code == 200
    assert throttled_deliveries.json()["total"] >= 1

    pause_queue = client.post("/api/v1/webhooks/queue/pause", headers=admin_headers)
    assert pause_queue.status_code == 200
    assert pause_queue.json()["paused"] is True
    process_when_paused = client.post("/api/v1/webhooks/queue/process?limit=10&ignore_schedule=true", headers=admin_headers)
    assert process_when_paused.status_code == 200
    assert process_when_paused.json()["status"] == "paused"
    assert process_when_paused.json()["paused"] is True
    paused_stats = client.get("/api/v1/webhooks/stats", headers=admin_headers)
    assert paused_stats.status_code == 200
    assert paused_stats.json()["queue_paused"] is True
    resume_queue = client.post("/api/v1/webhooks/queue/resume", headers=admin_headers)
    assert resume_queue.status_code == 200
    assert resume_queue.json()["paused"] is False
    resumed_stats = client.get("/api/v1/webhooks/stats", headers=admin_headers)
    assert resumed_stats.status_code == 200
    assert resumed_stats.json()["queue_paused"] is False

    notifications = client.get("/api/v1/notifications/outbox?limit=20", headers=admin_headers)
    assert notifications.status_code == 200
    assert notifications.json()["total"] >= 1
    process_notifications = client.post("/api/v1/notifications/process?limit=20", headers=admin_headers)
    assert process_notifications.status_code == 200
    assert process_notifications.json()["processed"] >= 0
    notification_status = client.get("/api/v1/notifications/status", headers=admin_headers)
    assert notification_status.status_code == 200
    assert "backend" in notification_status.json()
    retry_notifications = client.post("/api/v1/notifications/retry-failures?limit=20", headers=admin_headers)
    assert retry_notifications.status_code == 200

    dashboard = client.get("/dashboard")
    assert dashboard.status_code == 200
    assert "金融舆情事件流看板" in dashboard.text
    metrics = client.get("/metrics")
    assert metrics.status_code == 200
    assert "qsm_events_total" in metrics.text

    with patch("quant_sentiment_monitor.collector._fetch_url", return_value=("<rss><channel><item><title>Collector item</title><description>desc</description></item></channel></rss>", None)):
        collector_run = client.post("/api/v1/collector/run-once?limit=2&retries=0", headers=admin_headers)
    assert collector_run.status_code == 200
    assert collector_run.json()["status"] == "ok"

    delete_webhook = client.delete(f"/api/v1/webhooks/subscriptions/{webhook_id}", headers=admin_headers)
    assert delete_webhook.status_code == 200
    delete_failing_webhook = client.delete(f"/api/v1/webhooks/subscriptions/{failing_id}", headers=admin_headers)
    assert delete_failing_webhook.status_code == 200
    delete_throttled_webhook = client.delete(f"/api/v1/webhooks/subscriptions/{throttled_id}", headers=admin_headers)
    assert delete_throttled_webhook.status_code == 200


def test_auth_and_permission_denied_paths() -> None:
    # Invalid login should be rejected.
    invalid_login = client.post("/api/v1/auth/login", json={"username": "demo", "password": "wrong"})
    assert invalid_login.status_code == 401

    analyst_headers = _login_headers(username="demo", password="demo123")
    trader_headers = _login_headers(username="adollman", password="dev123")
    admin_headers = _login_headers(username="sweer1234", password="dev123")

    # Analyst cannot manage alert policies.
    denied_policy = client.put(
        "/api/v1/alerts/policies",
        headers=analyst_headers,
        json={"dedup_window_minutes": 20},
    )
    assert denied_policy.status_code == 403

    # Analyst cannot review manual message.
    create = client.post(
        "/api/v1/manual/messages",
        headers=TOKEN,
        json={
            "title": "测试权限消息",
            "content": "权限链路检查",
            "operator_id": "u_analyst_002",
            "operator_role": "analyst",
        },
    )
    assert create.status_code == 200
    mm_id = create.json()["manual_message_id"]
    denied_review = client.post(
        f"/api/v1/manual/messages/{mm_id}/review",
        headers=analyst_headers,
        json={"action": "approve"},
    )
    assert denied_review.status_code == 403
    denied_manual_batch = client.post(
        "/api/v1/manual/messages/batch",
        headers=analyst_headers,
        json={"as_draft": True, "messages": []},
    )
    assert denied_manual_batch.status_code == 200
    denied_publish = client.post(f"/api/v1/manual/messages/{mm_id}/publish", headers=analyst_headers)
    assert denied_publish.status_code == 403
    denied_reevaluate = client.post(f"/api/v1/manual/messages/{mm_id}/re-evaluate", headers=analyst_headers)
    assert denied_reevaluate.status_code == 403

    # Public token keeps backward compatibility for manual review.
    public_review = client.post(
        f"/api/v1/manual/messages/{mm_id}/review",
        headers=TOKEN,
        json={"action": "approve"},
    )
    assert public_review.status_code == 200

    # Trader can revoke alerts but cannot create calendar events.
    trader_revoke = client.post("/api/v1/alerts/alert_t2/revoke?reason=permission_test", headers=trader_headers)
    assert trader_revoke.status_code == 200
    trader_calendar = client.post(
        "/api/v1/calendar/events",
        headers=trader_headers,
        json={"country": "US", "event_name": "ISM PMI", "importance_level": "P1"},
    )
    assert trader_calendar.status_code == 403
    trader_backfill = client.post(
        "/api/v1/calendar/events/cal_us_nfp_last/actual",
        headers=trader_headers,
        json={"actual": 200, "consensus": 180},
    )
    assert trader_backfill.status_code == 403

    # Admin can create calendar event.
    admin_calendar = client.post(
        "/api/v1/calendar/events",
        headers=admin_headers,
        json={"country": "US", "event_name": "Retail Sales", "importance_level": "P1"},
    )
    assert admin_calendar.status_code == 200

    # Analyst cannot access webhook delivery management.
    denied_webhook_list = client.get("/api/v1/webhooks/deliveries", headers=analyst_headers)
    assert denied_webhook_list.status_code == 403
    denied_webhook_retry = client.post("/api/v1/webhooks/retry-failures", headers=analyst_headers)
    assert denied_webhook_retry.status_code == 403
    denied_webhook_process = client.post("/api/v1/webhooks/queue/process", headers=analyst_headers)
    assert denied_webhook_process.status_code == 403
    denied_webhook_stats = client.get("/api/v1/webhooks/stats", headers=analyst_headers)
    assert denied_webhook_stats.status_code == 403
    denied_webhook_dlq = client.get("/api/v1/webhooks/dlq", headers=analyst_headers)
    assert denied_webhook_dlq.status_code == 403
    denied_webhook_replay = client.post("/api/v1/webhooks/dlq/replay", headers=analyst_headers)
    assert denied_webhook_replay.status_code == 403
    denied_source_versions = client.get("/api/v1/sources/federal_reserve/versions", headers=analyst_headers)
    assert denied_source_versions.status_code == 403
    denied_source_rollback = client.post(
        "/api/v1/sources/federal_reserve/rollback?version_id=sv_unknown",
        headers=analyst_headers,
    )
    assert denied_source_rollback.status_code == 403
    denied_webhook_pause = client.post("/api/v1/webhooks/queue/pause", headers=analyst_headers)
    assert denied_webhook_pause.status_code == 403
    denied_webhook_resume = client.post("/api/v1/webhooks/queue/resume", headers=analyst_headers)
    assert denied_webhook_resume.status_code == 403
    denied_alert_escalate = client.post("/api/v1/alerts/escalate", headers=analyst_headers)
    assert denied_alert_escalate.status_code == 403
    denied_alert_escalations = client.get("/api/v1/alerts/escalations", headers=analyst_headers)
    assert denied_alert_escalations.status_code == 403
    denied_audit = client.get("/api/v1/audit/logs", headers=analyst_headers)
    assert denied_audit.status_code == 403
    denied_metrics = client.get("/api/v1/metrics/summary", headers=analyst_headers)
    assert denied_metrics.status_code == 403
    denied_notifications = client.get("/api/v1/notifications/outbox", headers=analyst_headers)
    assert denied_notifications.status_code == 403
    denied_notification_process = client.post("/api/v1/notifications/process", headers=analyst_headers)
    assert denied_notification_process.status_code == 403
    denied_notification_retry = client.post("/api/v1/notifications/retry-failures", headers=analyst_headers)
    assert denied_notification_retry.status_code == 403
    denied_notification_status = client.get("/api/v1/notifications/status", headers=analyst_headers)
    assert denied_notification_status.status_code == 403
    denied_collector = client.post("/api/v1/collector/run-once", headers=trader_headers)
    assert denied_collector.status_code == 403
    denied_collector_enqueue = client.post("/api/v1/collector/tasks/enqueue", headers=trader_headers, json={})
    assert denied_collector_enqueue.status_code == 403
    denied_collector_stats = client.get("/api/v1/collector/tasks/stats", headers=trader_headers)
    assert denied_collector_stats.status_code == 403
    denied_collector_process = client.post("/api/v1/collector/tasks/process", headers=trader_headers)
    assert denied_collector_process.status_code == 403

    # Missing token should fail.
    no_token = client.get("/api/v1/users/me")
    assert no_token.status_code == 401


def test_event_ingest_and_alert_lifecycle() -> None:
    admin_headers = _login_headers(username="sweer1234", password="dev123")
    trader_headers = _login_headers(username="adollman", password="dev123")
    analyst_headers = _login_headers(username="demo", password="demo123")

    ingest = client.post(
        "/api/v1/events/ingest",
        headers=analyst_headers,
        json={
            "source_id": "federal_reserve",
            "title": "Fed 官方声明偏鹰派",
            "content": "政策声明强调通胀风险，市场提高加息预期。",
            "event_type": "central_bank_policy",
            "related_instruments": ["DXY", "UST10Y"],
            "credibility_level": "official",
            "evidence": ["https://www.federalreserve.gov/newsevents/pressreleases.htm"],
        },
    )
    assert ingest.status_code == 200
    event_id = ingest.json()["event"]["event_id"]
    assert ingest.json()["alert"] is not None
    assert ingest.json()["deduplicated"] is False
    alert_id = ingest.json()["alert"]["alert_id"]

    duplicate_ingest = client.post(
        "/api/v1/events/ingest",
        headers=analyst_headers,
        json={
            "source_id": "federal_reserve",
            "title": "Fed 官方声明偏鹰派",
            "content": "政策声明强调通胀风险，市场提高加息预期。",
            "event_type": "central_bank_policy",
            "related_instruments": ["DXY", "UST10Y"],
        },
    )
    assert duplicate_ingest.status_code == 200
    assert duplicate_ingest.json()["deduplicated"] is True
    assert duplicate_ingest.json()["event"]["event_id"] == event_id

    batch_ingest = client.post(
        "/api/v1/events/batch-ingest",
        headers=analyst_headers,
        json={
            "request_id": "batch-r1",
            "events": [
                {
                    "source_id": "federal_reserve",
                    "title": "Fed 官方声明偏鹰派",
                    "content": "政策声明强调通胀风险，市场提高加息预期。",
                },
                {
                    "source_id": "opec",
                    "title": "OPEC 计划减产",
                    "content": "部分成员国考虑额外减产，原油供给趋紧。",
                    "related_instruments": ["CL", "USDCAD"],
                },
                {
                    "source_id": "",
                    "title": "bad",
                    "content": "bad",
                },
            ],
        },
    )
    assert batch_ingest.status_code == 200
    assert batch_ingest.json()["total"] == 3
    assert batch_ingest.json()["accepted"] >= 1
    assert batch_ingest.json()["deduplicated"] >= 1
    assert batch_ingest.json()["rejected"] >= 1
    assert batch_ingest.json()["idempotent_hit"] is False

    batch_ingest_again = client.post(
        "/api/v1/events/batch-ingest",
        headers=analyst_headers,
        json={
            "request_id": "batch-r1",
            "events": [
                {
                    "source_id": "federal_reserve",
                    "title": "Fed 官方声明偏鹰派",
                    "content": "政策声明强调通胀风险，市场提高加息预期。",
                }
            ],
        },
    )
    assert batch_ingest_again.status_code == 200
    assert batch_ingest_again.json()["idempotent_hit"] is True

    event_detail = client.get(f"/api/v1/events/id/{event_id}")
    assert event_detail.status_code == 200

    analyst_alerts = client.get("/api/v1/alerts/feed?importance_min=P2", headers=analyst_headers)
    assert analyst_alerts.status_code == 200
    assert analyst_alerts.json()["total"] >= 1

    ack = client.post(f"/api/v1/alerts/{alert_id}/ack", headers=trader_headers, json={"note": "checked"})
    assert ack.status_code == 200
    assert ack.json()["status"] == "acked"

    revoke = client.post(f"/api/v1/alerts/{alert_id}/revoke?reason=manual_cancel", headers=admin_headers)
    assert revoke.status_code == 200
    revoked_feed = client.get("/api/v1/alerts/feed?status=revoked", headers=admin_headers)
    assert revoked_feed.status_code == 200
    assert any(item["alert_id"] == alert_id for item in revoked_feed.json()["alerts"])

    denied_ingest = client.post(
        "/api/v1/events/ingest",
        json={
            "source_id": "federal_reserve",
            "title": "无token应拒绝",
            "content": "test",
        },
    )
    assert denied_ingest.status_code == 401

    compliance_block = client.post(
        "/api/v1/events/ingest",
        headers=analyst_headers,
        json={
            "source_id": "unknown_external_source",
            "title": "外部分发合规测试",
            "content": "尝试分发未知来源",
            "publish_external": True,
        },
    )
    assert compliance_block.status_code == 422

    metrics = client.get("/api/v1/metrics/summary", headers=admin_headers)
    assert metrics.status_code == 200
    assert "events_total" in metrics.json()
    assert "ingest_stats" in metrics.json()
    assert metrics.json()["ingest_stats"]["total"] >= 1
    assert "audit_total" in metrics.json()

    audits = client.get("/api/v1/audit/logs?limit=20", headers=admin_headers)
    assert audits.status_code == 200
    assert audits.json()["total"] >= 1
    assert "offset" in audits.json()

    audit_page_2 = client.get("/api/v1/audit/logs?limit=5&offset=1", headers=admin_headers)
    assert audit_page_2.status_code == 200
    assert audit_page_2.json()["offset"] == 1
    assert audit_page_2.json()["total"] >= len(audit_page_2.json()["logs"])

    audit_action = client.get("/api/v1/audit/logs?action=auth.login&limit=50", headers=admin_headers)
    assert audit_action.status_code == 200
    assert audit_action.json()["total"] >= 1
    assert all(item["action"] == "auth.login" for item in audit_action.json()["logs"])

    audit_future = client.get("/api/v1/audit/logs?from=2099-01-01T00:00:00%2B00:00&limit=20", headers=admin_headers)
    assert audit_future.status_code == 200
    assert audit_future.json()["total"] == 0

    exported = client.get("/api/v1/admin/state/export", headers=admin_headers)
    assert exported.status_code == 200
    assert "events" in exported.json()

    reset = client.post("/api/v1/admin/state/reset?reseed=false", headers=admin_headers)
    assert reset.status_code == 200
    assert reset.json()["events_total"] == 0

    import_resp = client.post(
        "/api/v1/admin/state/import?merge=false",
        headers=admin_headers,
        json=exported.json(),
    )
    assert import_resp.status_code == 200
    assert import_resp.json()["imported_events"] >= 1
