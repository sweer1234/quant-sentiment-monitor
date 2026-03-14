from __future__ import annotations

import json
from typing import Any, Protocol
from urllib import request as urlrequest

from .engine import classify_event_type, classify_sentiment, extract_entities
from .settings import Settings


class InferenceAdapter(Protocol):
    def analyze(self, *, title: str, content: str) -> dict[str, Any]:
        ...


class LocalInferenceAdapter:
    def analyze(self, *, title: str, content: str) -> dict[str, Any]:
        sentiment = classify_sentiment(title=title, content=content)
        event_type = classify_event_type(title=title, content=content)
        entities = extract_entities(title=title, content=content)
        return {
            "sentiment": sentiment,
            "event_type": event_type,
            "entities": entities,
            "provider": "local_rule",
        }


class HttpInferenceAdapter:
    def __init__(self, *, model_service_url: str, timeout_sec: int = 3):
        self.model_service_url = model_service_url
        self.timeout_sec = timeout_sec
        self._fallback = LocalInferenceAdapter()

    def analyze(self, *, title: str, content: str) -> dict[str, Any]:
        payload = {"title": title, "content": content}
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        req = urlrequest.Request(
            self.model_service_url,
            data=data,
            headers={"Content-Type": "application/json", "User-Agent": "qsm-inference/0.1"},
            method="POST",
        )
        try:
            with urlrequest.urlopen(req, timeout=self.timeout_sec) as resp:
                raw = resp.read().decode("utf-8", errors="ignore")
            parsed = json.loads(raw)
            sentiment = str(parsed.get("sentiment", "")).lower()
            event_type = str(parsed.get("event_type", "")).strip()
            entities = parsed.get("entities", [])
            if sentiment not in {"positive", "neutral", "negative"}:
                raise ValueError("invalid sentiment from model service")
            if not event_type:
                raise ValueError("empty event_type from model service")
            if not isinstance(entities, list):
                raise ValueError("invalid entities from model service")
            return {
                "sentiment": sentiment,
                "event_type": event_type,
                "entities": [str(item) for item in entities],
                "provider": "http_model_service",
            }
        except Exception:
            result = self._fallback.analyze(title=title, content=content)
            result["provider"] = "fallback_local_rule"
            return result


def build_inference_adapter(settings: Settings) -> InferenceAdapter:
    backend = settings.model_backend.lower().strip()
    if backend == "http":
        return HttpInferenceAdapter(
            model_service_url=settings.model_service_url,
            timeout_sec=settings.model_service_timeout_sec,
        )
    return LocalInferenceAdapter()

