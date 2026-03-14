from __future__ import annotations

import json
from unittest.mock import patch

from quant_sentiment_monitor.inference import HttpInferenceAdapter


class _FakeResponse:
    def __init__(self, payload: dict):
        self._payload = payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def read(self) -> bytes:
        return json.dumps(self._payload, ensure_ascii=False).encode("utf-8")


def test_http_inference_adapter_success() -> None:
    adapter = HttpInferenceAdapter(model_service_url="http://example.com/infer", timeout_sec=1)
    payload = {"sentiment": "positive", "event_type": "macro_data_release", "entities": ["美国"]}
    with patch("quant_sentiment_monitor.inference.urlrequest.urlopen", return_value=_FakeResponse(payload)):
        result = adapter.analyze(title="US CPI 超预期", content="通胀高于预期")
    assert result["provider"] == "http_model_service"
    assert result["sentiment"] == "positive"
    assert result["event_type"] == "macro_data_release"
    assert result["entities"] == ["美国"]


def test_http_inference_adapter_fallback() -> None:
    adapter = HttpInferenceAdapter(model_service_url="http://example.com/infer", timeout_sec=1)
    with patch("quant_sentiment_monitor.inference.urlrequest.urlopen", side_effect=RuntimeError("boom")):
        result = adapter.analyze(title="美联储偏鹰", content="加息路径上修")
    assert result["provider"] == "fallback_local_rule"
    assert result["sentiment"] in {"positive", "neutral", "negative"}
    assert isinstance(result["entities"], list)

