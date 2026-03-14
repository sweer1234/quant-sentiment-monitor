from __future__ import annotations

from typing import Any

from fastapi import FastAPI

from quant_sentiment_monitor.engine import classify_event_type, classify_sentiment, extract_entities


app = FastAPI(title="QSM Model Service", version="0.1.0")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "service": "qsm-model-service"}


@app.post("/infer")
def infer(payload: dict[str, Any]) -> dict[str, Any]:
    title = str(payload.get("title", ""))
    content = str(payload.get("content", ""))
    sentiment = classify_sentiment(title=title, content=content)
    event_type = classify_event_type(title=title, content=content)
    entities = extract_entities(title=title, content=content)
    return {
        "sentiment": sentiment,
        "event_type": event_type,
        "entities": entities,
        "provider": "qsm-model-service",
    }

