from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import Depends, FastAPI, Header, HTTPException, Query, WebSocket
from fastapi.responses import PlainTextResponse

from .models import (
    EventFeedResponse,
    ImpactBatchRequest,
    ImpactBatchResponse,
    ManualMessageCreateRequest,
    ManualMessageReviewRequest,
    SentimentResponse,
    SignalResponse,
    SourcePatchRequest,
    SourcesBatchRequest,
)
from .settings import Settings
from .store import QuantStore


settings = Settings()
store = QuantStore(settings=settings)

app = FastAPI(
    title="Quant Sentiment Monitor API",
    version="0.1.0",
    description="MVP backend for financial sentiment and event impact monitoring.",
)


def require_token(authorization: str = Header(default="", alias="Authorization")) -> None:
    expected = f"Bearer {settings.public_api_token}"
    if authorization != expected:
        raise HTTPException(status_code=401, detail="Invalid or missing API token")


@app.get("/api/v1/health")
def health() -> dict[str, Any]:
    return {
        "status": "ok",
        "service": settings.app_name,
        "time": datetime.now(timezone.utc).isoformat(),
        "sources": len(store.sources),
        "events": len(store.events),
        "manual_messages": len(store.manual_messages),
    }


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
def patch_source(source_id: str, request: SourcePatchRequest, _: None = Depends(require_token)) -> dict[str, Any]:
    return store.patch_source(source_id=source_id, patch_data=request.model_dump())


@app.post("/api/v1/sources/batch")
def batch_sources(request: SourcesBatchRequest, _: None = Depends(require_token)) -> dict[str, Any]:
    return store.batch_update_sources([item.model_dump() for item in request.operations])


@app.post("/api/v1/sources/reload")
def reload_sources(_: None = Depends(require_token)) -> dict[str, Any]:
    return store.reload_configs()


@app.get("/api/v1/sources/export")
def export_sources(format: str = Query(default="yaml")) -> PlainTextResponse:
    return PlainTextResponse(store.export_sources(fmt=format))


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
    _: None = Depends(require_token),
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
