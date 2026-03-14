from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

from fastapi import Depends, FastAPI, Header, HTTPException, Query, WebSocket
from fastapi.responses import PlainTextResponse

from .models import (
    AlertPolicyUpdateRequest,
    AlertSubscriptionsRequest,
    AlertAckRequest,
    EventIngestRequest,
    EventFeedResponse,
    FeedbackRequest,
    ImpactBatchRequest,
    ImpactBatchResponse,
    LoginRequest,
    ManualMessageCreateRequest,
    ManualMessageReviewRequest,
    PortfolioImpactRequest,
    SentimentResponse,
    SignalResponse,
    SourcePatchRequest,
    SourcesBatchRequest,
    TopicSubscriptionRequest,
    UserPreferences,
    WebhookSubscriptionRequest,
)
from .settings import Settings
from .store import QuantStore


settings = Settings()
store = QuantStore(settings=settings)

app = FastAPI(
    title="Quant Sentiment Monitor API",
    version="0.3.0",
    description="MVP backend for financial sentiment and event impact monitoring.",
)


def require_token(authorization: str = Header(default="", alias="Authorization")) -> None:
    expected = f"Bearer {settings.public_api_token}"
    if authorization != expected:
        raise HTTPException(status_code=401, detail="Invalid or missing API token")


def get_current_user(authorization: str = Header(default="", alias="Authorization")) -> str:
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing bearer token")
    token = authorization.split(" ", 1)[1].strip()
    username = store.username_by_token(token)
    if not username:
        raise HTTPException(status_code=401, detail="Invalid access token")
    return username


def require_permission(action: str):
    def _dependency(username: str = Depends(get_current_user)) -> str:
        if not store.has_permission(username, action):
            raise HTTPException(status_code=403, detail=f"permission denied: {action}")
        return username

    return _dependency


def require_public_or_permission(action: str):
    def _dependency(authorization: str = Header(default="", alias="Authorization")) -> str:
        expected = f"Bearer {settings.public_api_token}"
        if authorization == expected:
            return "public_token"
        if authorization.startswith("Bearer "):
            token = authorization.split(" ", 1)[1].strip()
            username = store.username_by_token(token)
            if username and store.has_permission(username, action):
                return username
            raise HTTPException(status_code=403, detail=f"permission denied: {action}")
        raise HTTPException(status_code=401, detail="Invalid or missing authorization")

    return _dependency


@app.get("/")
def root() -> dict[str, str]:
    return {
        "service": settings.app_name,
        "version": "0.3.0",
        "docs": "/docs",
        "health": "/api/v1/health",
        "openapi": "/openapi.json",
    }


@app.get("/api/v1/health")
def health() -> dict[str, Any]:
    return {
        "status": "ok",
        "service": settings.app_name,
        "time": datetime.now(timezone.utc).isoformat(),
        "sources": len(store.sources),
        "events": len(store.events),
        "manual_messages": len(store.manual_messages),
        "users": len(store.users),
    }


@app.post("/api/v1/auth/login")
def auth_login(request: LoginRequest) -> dict[str, Any]:
    result = store.login(username=request.username, password=request.password)
    if not result:
        raise HTTPException(status_code=401, detail="username or password invalid")
    return result


@app.get("/api/v1/sentiment/{symbol}", response_model=SentimentResponse)
def get_sentiment(symbol: str) -> SentimentResponse:
    score, confidence, samples = store.sentiment_for_symbol(symbol=symbol)
    return SentimentResponse(
        symbol=symbol.upper(),
        sentiment_score=score,
        confidence=confidence,
        window="4h",
        sample_events=samples,
    )


@app.get("/api/v1/signals", response_model=SignalResponse)
def get_signals(symbol: str = Query(...), interval: str = Query("1m")) -> SignalResponse:
    signal, confidence, reason = store.signal_for_symbol(symbol=symbol)
    return SignalResponse(
        symbol=symbol.upper(),
        interval=interval,
        signal=signal,  # type: ignore[arg-type]
        confidence=confidence,
        reason=reason,
    )


@app.get("/api/v1/events/{event_id}/impact")
def get_event_impact(event_id: str) -> dict[str, Any]:
    event = store.events.get(event_id)
    if not event:
        raise HTTPException(status_code=404, detail="event not found")
    return event.model_dump(mode="json")


@app.get("/api/v1/events/id/{event_id}")
def get_event_detail(event_id: str) -> dict[str, Any]:
    event = store.events.get(event_id)
    if not event:
        raise HTTPException(status_code=404, detail="event not found")
    return event.model_dump(mode="json")


@app.post("/api/v1/events/ingest")
def ingest_event(
    request: EventIngestRequest,
    _: str = Depends(require_permission("events.ingest")),
) -> dict[str, Any]:
    try:
        return store.ingest_event(request.model_dump(mode="json"))
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@app.get("/api/v1/events/feed", response_model=EventFeedResponse)
def get_events_feed(
    from_time: datetime | None = Query(default=None, alias="from"),
    to_time: datetime | None = Query(default=None, alias="to"),
    importance_min: float | None = Query(default=None),
    market: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
) -> EventFeedResponse:
    events = store.filter_events(from_time=from_time, to_time=to_time, importance_min=importance_min, market=market)
    total = len(events)
    start = (page - 1) * page_size
    selected = events[start : start + page_size]

    cards = []
    for event in selected:
        cards.append(
            {
                "event_id": event.event_id,
                "title": event.title,
                "importance_level": event.importance_level,
                "importance_score": event.importance_score,
                "impacted_markets": event.impacted_markets,
                "top_impacted_instruments": [item.instrument for item in event.impacts[:3]],
                "net_bias_score": round(sum(item.net_bias_score for item in event.impacts) / len(event.impacts), 2),
                "detected_at": event.detected_at,
            }
        )
    return EventFeedResponse(page=page, page_size=page_size, total=total, events=cards)


@app.get("/api/v1/stream/events")
def stream_events_metadata() -> dict[str, Any]:
    return {
        "protocol": "websocket",
        "path": "/ws/events",
        "sample_event_count": min(5, len(store.events)),
        "heartbeat_seconds": 10,
    }


@app.websocket("/ws/events")
async def websocket_events(websocket: WebSocket) -> None:
    await websocket.accept()
    for event in store.list_events()[:10]:
        await websocket.send_json(event.model_dump(mode="json"))
    await websocket.close()


@app.post("/api/v1/impact/batch", response_model=ImpactBatchResponse)
def impact_batch(request: ImpactBatchRequest) -> ImpactBatchResponse:
    results = store.impact_batch(instruments=request.instruments, event_ids=request.event_ids)
    return ImpactBatchResponse(window=request.window, request_id=request.request_id, results=results)


@app.post("/api/v1/portfolio/impact")
def portfolio_impact(request: PortfolioImpactRequest, _: str = Depends(get_current_user)) -> dict[str, Any]:
    return store.portfolio_impact(
        portfolio_id=request.portfolio_id,
        holdings=[item.model_dump() for item in request.holdings],
        event_ids=request.event_ids,
    )


@app.get("/api/v1/sources")
def list_sources(
    enabled: bool | None = Query(default=None),
    tier: int | None = Query(default=None),
    region: str | None = Query(default=None),
    category: str | None = Query(default=None),
) -> dict[str, Any]:
    data = store.list_sources(enabled=enabled, tier=tier, region=region, category=category)
    return {"total": len(data), "sources": data}


@app.patch("/api/v1/sources/{source_id}")
def patch_source(
    source_id: str,
    request: SourcePatchRequest,
    _: str = Depends(require_public_or_permission("sources.write")),
) -> dict[str, Any]:
    return store.patch_source(source_id=source_id, patch_data=request.model_dump())


@app.post("/api/v1/sources/batch")
def batch_sources(
    request: SourcesBatchRequest,
    _: str = Depends(require_public_or_permission("sources.write")),
) -> dict[str, Any]:
    return store.batch_update_sources([item.model_dump() for item in request.operations])


@app.post("/api/v1/sources/reload")
def reload_sources(_: str = Depends(require_public_or_permission("sources.write"))) -> dict[str, Any]:
    return store.reload_configs()


@app.get("/api/v1/sources/export")
def export_sources(format: str = Query(default="yaml")) -> PlainTextResponse:
    return PlainTextResponse(store.export_sources(fmt=format))


@app.get("/api/v1/sources/{source_id}/compliance")
def source_compliance(source_id: str) -> dict[str, Any]:
    return store.source_compliance(source_id)


@app.post("/api/v1/manual/messages")
def create_manual_message(request: ManualMessageCreateRequest, _: None = Depends(require_token)) -> dict[str, Any]:
    missing = []
    for field in store.manual_input_rules.get("required_fields", []):
        value = getattr(request, field, None)
        if value in (None, "", []):
            missing.append(field)
    if missing:
        raise HTTPException(status_code=422, detail=f"missing required fields: {','.join(missing)}")
    record = store.create_manual_message(request)
    return record.model_dump(mode="json")


@app.get("/api/v1/manual/messages/{manual_message_id}")
def get_manual_message(manual_message_id: str) -> dict[str, Any]:
    record = store.manual_messages.get(manual_message_id)
    if not record:
        raise HTTPException(status_code=404, detail="manual message not found")
    return record.model_dump(mode="json")


@app.post("/api/v1/manual/messages/{manual_message_id}/review")
def review_manual_message(
    manual_message_id: str,
    request: ManualMessageReviewRequest,
    _: str = Depends(require_public_or_permission("manual.review")),
) -> dict[str, Any]:
    record = store.review_manual_message(manual_message_id=manual_message_id, action=request.action)
    if not record:
        raise HTTPException(status_code=404, detail="manual message not found")
    return {
        "manual_message_id": manual_message_id,
        "status": record.status,
        "review_comment": request.review_comment,
        "updated_at": record.updated_at,
    }


@app.post("/api/v1/manual/messages/{manual_message_id}/re-evaluate")
def reevaluate_manual_message(manual_message_id: str, _: None = Depends(require_token)) -> dict[str, Any]:
    record = store.re_evaluate_manual_message(manual_message_id)
    if not record:
        raise HTTPException(status_code=404, detail="manual message not found")
    return record.model_dump(mode="json")


@app.get("/api/v1/users/me")
def users_me(username: str = Depends(get_current_user)) -> dict[str, Any]:
    return store.get_user_profile(username)


@app.put("/api/v1/users/me/preferences")
def update_preferences(request: UserPreferences, username: str = Depends(get_current_user)) -> dict[str, Any]:
    prefs = store.update_user_preferences(username, request.model_dump())
    return {"username": username, "preferences": prefs}


@app.get("/api/v1/users/me/feed")
def user_feed(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    importance_min: float | None = Query(default=None),
    username: str = Depends(get_current_user),
) -> dict[str, Any]:
    return store.personalized_feed(username, page=page, page_size=page_size, importance_min=importance_min)


@app.put("/api/v1/users/me/alert-subscriptions")
def update_alert_subscriptions(
    request: AlertSubscriptionsRequest,
    username: str = Depends(get_current_user),
) -> dict[str, Any]:
    data = store.update_user_alert_subscriptions(username, request.model_dump())
    return {"username": username, "alert_subscriptions": data}


@app.get("/api/v1/topics/catalog")
def topics_catalog() -> dict[str, Any]:
    topics = store.topic_catalog()
    return {"total": len(topics), "topics": topics}


@app.put("/api/v1/users/me/topic-subscriptions")
def update_topic_subscriptions(request: TopicSubscriptionRequest, username: str = Depends(get_current_user)) -> dict[str, Any]:
    topics = store.update_topic_subscriptions(username, request.topic_ids)
    return {"username": username, "topic_subscriptions": topics}


@app.get("/api/v1/users/me/topic-feed")
def topic_feed(
    topic: list[str] = Query(default=[]),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    username: str = Depends(get_current_user),
) -> dict[str, Any]:
    return store.topic_feed(username, topic_ids=topic, page=page, page_size=page_size)


@app.get("/api/v1/domains/catalog")
def domain_catalog() -> dict[str, Any]:
    domains = store.domain_catalog()
    return {"total": len(domains), "domains": domains}


@app.get("/api/v1/alerts/policies")
def get_alert_policies(_: str = Depends(get_current_user)) -> dict[str, Any]:
    return store.alert_policies


@app.put("/api/v1/alerts/policies")
def put_alert_policies(request: AlertPolicyUpdateRequest, _: str = Depends(require_permission("alerts.write"))) -> dict[str, Any]:
    return store.update_alert_policies(request.model_dump())


@app.get("/api/v1/alerts/feed")
def alerts_feed(
    status: str | None = Query(default=None),
    importance_min: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    username: str = Depends(get_current_user),
) -> dict[str, Any]:
    alerts = store.list_alerts(username=username, status=status, importance_min=importance_min, limit=limit)
    return {"total": len(alerts), "alerts": alerts}


@app.post("/api/v1/alerts/{alert_id}/ack")
def ack_alert(
    alert_id: str,
    request: AlertAckRequest,
    username: str = Depends(require_permission("alerts.ack")),
) -> dict[str, Any]:
    alert = store.ack_alert(alert_id=alert_id, username=username, note=request.note)
    if not alert:
        raise HTTPException(status_code=404, detail="alert not found or not accessible")
    return alert


@app.post("/api/v1/alerts/{alert_id}/revoke")
def revoke_alert(
    alert_id: str,
    reason: str = Query(default="manual_recall"),
    _: str = Depends(require_permission("alerts.revoke")),
) -> dict[str, Any]:
    return store.revoke_alert(alert_id=alert_id, reason=reason)


@app.get("/api/v1/events/{event_id}/credibility")
def event_credibility(event_id: str) -> dict[str, Any]:
    result = store.event_credibility(event_id)
    if not result:
        raise HTTPException(status_code=404, detail="event not found")
    return result


@app.post("/api/v1/feedback/events/{event_id}")
def event_feedback(
    event_id: str,
    request: FeedbackRequest,
    username: str = Depends(require_permission("feedback.write")),
) -> dict[str, Any]:
    if event_id not in store.events:
        raise HTTPException(status_code=404, detail="event not found")
    return store.add_feedback(username=username, event_id=event_id, payload=request.model_dump())


@app.get("/api/v1/billing/usage")
def billing_usage(tenant_id: str = Query(...), period: str = Query(...), _: str = Depends(get_current_user)) -> dict[str, Any]:
    return store.billing_usage(tenant_id=tenant_id, period=period)


@app.get("/api/v1/sla/status")
def sla_status(tenant_id: str = Query(...), _: str = Depends(get_current_user)) -> dict[str, Any]:
    return store.sla_status(tenant_id=tenant_id)


@app.get("/api/v1/calendar/events")
def list_calendar_events(
    country: str | None = Query(default=None),
    importance_min: str | None = Query(default=None),
    from_date: date | None = Query(default=None, alias="from"),
    to_date: date | None = Query(default=None, alias="to"),
) -> dict[str, Any]:
    events = store.list_calendar_events(
        country=country,
        importance_min=importance_min,
        from_date=from_date,
        to_date=to_date,
    )
    return {"total": len(events), "events": events}


@app.get("/api/v1/calendar/events/{calendar_event_id}/surprise")
def get_calendar_surprise(calendar_event_id: str) -> dict[str, Any]:
    result = store.calendar_surprise(calendar_event_id)
    if not result:
        raise HTTPException(status_code=404, detail="calendar event not found")
    return result


@app.post("/api/v1/calendar/events")
def upsert_calendar_event(
    payload: dict[str, Any],
    _: str = Depends(require_permission("calendar.manage")),
) -> dict[str, Any]:
    return store.upsert_calendar_event(payload)


@app.get("/api/v1/webhooks/subscriptions")
def list_webhooks(username: str = Depends(require_permission("webhooks.manage"))) -> dict[str, Any]:
    rows = store.list_webhook_subscriptions(username=username)
    return {"total": len(rows), "subscriptions": rows}


@app.post("/api/v1/webhooks/subscriptions")
def create_webhook(
    request: WebhookSubscriptionRequest,
    username: str = Depends(require_permission("webhooks.manage")),
) -> dict[str, Any]:
    return store.create_webhook_subscription(username=username, payload=request.model_dump())


@app.delete("/api/v1/webhooks/subscriptions/{subscription_id}")
def delete_webhook(
    subscription_id: str,
    username: str = Depends(require_permission("webhooks.manage")),
) -> dict[str, Any]:
    ok = store.delete_webhook_subscription(subscription_id=subscription_id, username=username)
    if not ok:
        raise HTTPException(status_code=404, detail="subscription not found or not allowed")
    return {"subscription_id": subscription_id, "status": "deleted"}


@app.post("/api/v1/webhooks/dispatch-test")
def webhook_dispatch_test(
    event_id: str | None = Query(default=None),
    force_fail: bool = Query(default=False),
    _: str = Depends(require_permission("webhooks.manage")),
) -> dict[str, Any]:
    return store.dispatch_webhook_test(event_id=event_id, force_fail=force_fail)


@app.get("/api/v1/webhooks/deliveries")
def list_webhook_deliveries(
    subscription_id: str | None = Query(default=None),
    status: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    _: str = Depends(require_permission("webhooks.manage")),
) -> dict[str, Any]:
    rows = store.list_webhook_deliveries(subscription_id=subscription_id, status=status, limit=limit)
    return {"total": len(rows), "deliveries": rows}


@app.post("/api/v1/webhooks/retry-failures")
def retry_webhook_failures(
    limit: int = Query(default=20, ge=1, le=200),
    _: str = Depends(require_permission("webhooks.manage")),
) -> dict[str, Any]:
    return store.retry_failed_webhooks(limit=limit)


@app.post("/api/v1/webhooks/queue/process")
def process_webhook_queue(
    limit: int = Query(default=50, ge=1, le=500),
    ignore_schedule: bool = Query(default=False),
    _: str = Depends(require_permission("webhooks.manage")),
) -> dict[str, Any]:
    return store.process_webhook_queue(limit=limit, ignore_schedule=ignore_schedule)


@app.get("/api/v1/webhooks/stats")
def webhook_stats(_: str = Depends(require_permission("webhooks.manage"))) -> dict[str, Any]:
    return store.webhook_stats()
