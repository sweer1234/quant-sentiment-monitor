from __future__ import annotations

from collections import defaultdict
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Any
from uuid import uuid4

import yaml

from .engine import (
    aggregate_signal,
    calculate_effective_source_weight,
    infer_markets_and_impacts,
    level_from_score,
    now_utc,
)
from .models import Event, ImpactItem, ManualMessageCreateRequest, ManualMessageRecord
from .settings import Settings


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as file:
        data = yaml.safe_load(file) or {}
    return data if isinstance(data, dict) else {}


class QuantStore:
    def __init__(self, settings: Settings):
        self.settings = settings
        self._lock = Lock()
        self.sources: list[dict[str, Any]] = []
        self.source_weight_rules: dict[str, Any] = {}
        self.manual_input_rules: dict[str, Any] = {}
        self.events: dict[str, Event] = {}
        self.manual_messages: dict[str, ManualMessageRecord] = {}
        self.alert_policies: dict[str, Any] = {
            "dedup_window_minutes": 45,
            "cooldown_minutes": {"P0": 5, "P1": 10, "P2": 30},
            "channels_order": ["app", "im", "email"],
            "allow_revoke": True,
        }
        self._source_by_id: dict[str, dict[str, Any]] = {}
        self.reload_configs()
        self._seed_events()

    def reload_configs(self) -> dict[str, Any]:
        with self._lock:
            default_data = _load_yaml(self.settings.source_registry_default_path)
            override_data = _load_yaml(self.settings.source_registry_override_path)
            self.source_weight_rules = _load_yaml(self.settings.source_weight_rules_path)
            self.manual_input_rules = _load_yaml(self.settings.manual_input_rules_path)

            defaults = default_data.get("sources", [])
            if not isinstance(defaults, list):
                defaults = []
            source_map = {item["source_id"]: deepcopy(item) for item in defaults if "source_id" in item}

            for patch in override_data.get("overrides", []):
                source_id = patch.get("source_id")
                if not source_id:
                    continue
                if source_id not in source_map:
                    source_map[source_id] = {"source_id": source_id, "enabled": True}
                source_map[source_id].update({k: v for k, v in patch.items() if k != "source_id"})

            self.sources = list(source_map.values())
            self._source_by_id = {item["source_id"]: item for item in self.sources}
            return {
                "status": "ok",
                "default_sources_loaded": len(defaults),
                "override_sources_loaded": len(override_data.get("overrides", [])),
                "effective_sources_loaded": len(self.sources),
                "reloaded_at": now_utc().isoformat(),
            }

    def _next_event_id(self) -> str:
        return f"evt_{uuid4().hex[:12]}"

    def _next_manual_id(self) -> str:
        return f"mm_{uuid4().hex[:12]}"

    def _score_event(self, source_id: str, impacts: list[ImpactItem]) -> tuple[float, str]:
        source = self._source_by_id.get(source_id, {"source_id": source_id, "category": "manual", "tier": 1})
        effective_source_weight = calculate_effective_source_weight(source)
        event_severity = max((impact.impact_score for impact in impacts), default=50) / 100
        surprise_degree = 0.60
        cross_market_span = min(1.0, len({impact.asset_class for impact in impacts}) / 4)
        market_confirmation = 0.55

        score = 100 * (
            0.32 * effective_source_weight
            + 0.25 * event_severity
            + 0.18 * surprise_degree
            + 0.15 * cross_market_span
            + 0.10 * market_confirmation
        )
        score = max(0.0, min(100.0, round(score, 2)))
        return score, level_from_score(score)

    def _build_event(self, source_id: str, title: str, content: str, related_instruments: list[str] | None = None) -> Event:
        markets, impacts = infer_markets_and_impacts(title=title, content=content, related_instruments=related_instruments)
        importance_score, importance_level = self._score_event(source_id=source_id, impacts=impacts)
        event_type = "macro_policy" if "利率" in title or "央行" in title else "market_event"
        now = now_utc()
        return Event(
            event_id=self._next_event_id(),
            source_id=source_id,
            title=title,
            content=content,
            published_at=now,
            detected_at=now,
            event_type=event_type,
            importance_level=importance_level,  # type: ignore[arg-type]
            importance_score=importance_score,
            impacted_markets=markets,
            impacts=impacts,
            credibility_level="verified",
            evidence=["auto_generated_rule_engine"],
        )

    def _seed_events(self) -> None:
        seeded = [
            ("federal_reserve", "美联储官员释放偏鹰派信号", "讲话提及通胀风险仍高，市场上调年内利率路径。"),
            ("opec", "OPEC+ 讨论延长减产窗口", "若减产延期，原油供需缺口可能扩大。"),
            ("sec", "监管机构加密资产合规执法升级", "交易平台合规要求趋严，短线风险偏好回落。"),
        ]
        for source_id, title, content in seeded:
            event = self._build_event(source_id=source_id, title=title, content=content)
            self.events[event.event_id] = event

    def list_sources(self, *, enabled: bool | None = None, tier: int | None = None, region: str | None = None, category: str | None = None) -> list[dict[str, Any]]:
        result = []
        for item in self.sources:
            if enabled is not None and bool(item.get("enabled", True)) != enabled:
                continue
            if tier is not None and int(item.get("tier", 1)) != tier:
                continue
            if region and str(item.get("region", "")).upper() != region.upper():
                continue
            if category and str(item.get("category", "")).lower() != category.lower():
                continue
            enriched = deepcopy(item)
            enriched["effective_source_weight"] = round(calculate_effective_source_weight(item), 4)
            result.append(enriched)
        return result

    def patch_source(self, source_id: str, patch_data: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            if source_id not in self._source_by_id:
                self._source_by_id[source_id] = {
                    "source_id": source_id,
                    "enabled": True,
                    "tier": 1,
                    "category": "media",
                    "region": "GLOBAL",
                    "source_weight": 0.7,
                    "credibility_weight": 0.75,
                    "timeliness_weight": 0.7,
                    "coverage_weight": 0.65,
                    "noise_penalty": 0.1,
                }
                self.sources.append(self._source_by_id[source_id])
            self._source_by_id[source_id].update({k: v for k, v in patch_data.items() if v is not None})
            source = deepcopy(self._source_by_id[source_id])
            source["effective_source_weight"] = round(calculate_effective_source_weight(source), 4)
            return source

    def batch_update_sources(self, operations: list[dict[str, Any]]) -> dict[str, Any]:
        updated: list[dict[str, Any]] = []
        for operation in operations:
            source_id = operation.get("source_id")
            if not source_id:
                continue
            op = operation.get("op")
            if op == "disable":
                updated.append(self.patch_source(source_id, {"enabled": False}))
                continue
            payload = {k: v for k, v in operation.items() if k not in {"op", "source_id"}}
            updated.append(self.patch_source(source_id, payload))
        return {"updated": len(updated), "sources": updated}

    def export_sources(self, fmt: str = "yaml") -> str:
        payload = {"sources": self.sources}
        if fmt.lower() == "json":
            import json

            return json.dumps(payload, ensure_ascii=False, indent=2)
        return yaml.safe_dump(payload, allow_unicode=True, sort_keys=False)

    def list_events(self) -> list[Event]:
        return sorted(self.events.values(), key=lambda item: item.detected_at, reverse=True)

    def filter_events(
        self,
        *,
        from_time: datetime | None,
        to_time: datetime | None,
        importance_min: float | None,
        market: str | None,
    ) -> list[Event]:
        result: list[Event] = []
        for event in self.list_events():
            if from_time and event.detected_at < from_time:
                continue
            if to_time and event.detected_at > to_time:
                continue
            if importance_min is not None and event.importance_score < importance_min:
                continue
            if market and market.lower() not in [m.lower() for m in event.impacted_markets]:
                continue
            result.append(event)
        return result

    def create_manual_message(self, request: ManualMessageCreateRequest) -> ManualMessageRecord:
        role_mult = float(self.manual_input_rules.get("operator_role_multipliers", {}).get(request.operator_role, 1.0))
        history_mult = 1.02
        evidence_mult = 1.10 if request.attachments else 0.95
        manual_weight = float(self.manual_input_rules.get("manual_source_weight", {}).get("base_manual_weight", 0.78))
        score_seed = manual_weight * role_mult * history_mult * evidence_mult

        event = self._build_event(
            source_id="manual_operator_input",
            title=request.title,
            content=request.content,
            related_instruments=request.related_instruments,
        )
        importance_score = max(event.importance_score, round(100 * score_seed * 0.9, 2))
        importance_level = level_from_score(importance_score)
        top_impacted = [impact.instrument for impact in event.impacts[:3]]
        long_score = round(sum(item.long_score for item in event.impacts) / len(event.impacts), 2)
        short_score = round(sum(item.short_score for item in event.impacts) / len(event.impacts), 2)
        net_bias = round(long_score - short_score, 2)
        now = now_utc()

        record = ManualMessageRecord(
            manual_message_id=self._next_manual_id(),
            status="auto_assessed",
            request=request,
            importance_level=importance_level,  # type: ignore[arg-type]
            importance_score=importance_score,
            impacted_markets=event.impacted_markets,
            top_impacted_instruments=top_impacted,
            long_score=long_score,
            short_score=short_score,
            net_bias_score=net_bias,
            assessment_explanation=[
                "event_type_supply_shock" if "减产" in request.content else "general_event_pattern",
                "cross_market_link_detected",
                f"operator_role_multiplier_{role_mult:.2f}",
            ],
            created_at=now,
            updated_at=now,
        )
        self.manual_messages[record.manual_message_id] = record

        if importance_score >= float(self.manual_input_rules.get("auto_assessment", {}).get("publish_gate", {}).get("min_importance_score", 60)):
            event.importance_score = importance_score
            event.importance_level = importance_level  # type: ignore[assignment]
            self.events[event.event_id] = event
            record.linked_event_id = event.event_id
        return record

    def review_manual_message(self, manual_message_id: str, action: str) -> ManualMessageRecord | None:
        record = self.manual_messages.get(manual_message_id)
        if not record:
            return None
        if action == "approve":
            record.status = "approved"
        elif action == "reject":
            record.status = "rejected"
        elif action == "revoke":
            record.status = "revoked"
            if record.linked_event_id and record.linked_event_id in self.events:
                del self.events[record.linked_event_id]
        record.updated_at = now_utc()
        return record

    def re_evaluate_manual_message(self, manual_message_id: str) -> ManualMessageRecord | None:
        existing = self.manual_messages.get(manual_message_id)
        if not existing:
            return None
        # Re-score using current rules and keep the same id.
        refreshed = self.create_manual_message(existing.request)
        refreshed.manual_message_id = manual_message_id
        refreshed.created_at = existing.created_at
        self.manual_messages[manual_message_id] = refreshed
        return refreshed

    def sentiment_for_symbol(self, symbol: str) -> tuple[float, float, int]:
        symbol = symbol.upper()
        relevant: list[ImpactItem] = []
        for event in self.events.values():
            relevant.extend([impact for impact in event.impacts if impact.instrument.upper() == symbol])
        if not relevant:
            return 0.0, 0.5, 0
        avg_long = sum(item.long_score for item in relevant) / len(relevant)
        avg_short = sum(item.short_score for item in relevant) / len(relevant)
        score = round((avg_long - avg_short) / 100, 4)
        confidence = round(sum(item.confidence for item in relevant) / len(relevant), 4)
        return score, confidence, len(relevant)

    def signal_for_symbol(self, symbol: str) -> tuple[str, float, str]:
        _, _, _ = symbol, self.events, self.manual_messages
        sentiment, confidence, samples = self.sentiment_for_symbol(symbol)
        long_score = (sentiment + 1) * 50
        short_score = 100 - long_score
        signal, base_conf = aggregate_signal(long_score, short_score)
        return signal, round((base_conf + confidence) / 2, 4), f"samples={samples}, sentiment={sentiment}"

    def impact_batch(self, instruments: list[str], event_ids: list[str]) -> list[dict[str, Any]]:
        selected_events = [self.events[event_id] for event_id in event_ids if event_id in self.events] if event_ids else list(self.events.values())
        bucket: dict[str, list[ImpactItem]] = defaultdict(list)
        allowed = {item.upper() for item in instruments}
        for event in selected_events:
            for impact in event.impacts:
                if impact.instrument.upper() in allowed:
                    bucket[impact.instrument.upper()].append(impact)

        result = []
        for instrument in instruments:
            impacts = bucket.get(instrument.upper(), [])
            if impacts:
                long_score = round(sum(item.long_score for item in impacts) / len(impacts), 2)
                short_score = round(sum(item.short_score for item in impacts) / len(impacts), 2)
            else:
                long_score = 50.0
                short_score = 50.0
            net = round(long_score - short_score, 2)
            dominant = "NEUTRAL"
            if net > 8:
                dominant = "LONG"
            elif net < -8:
                dominant = "SHORT"
            result.append(
                {
                    "instrument": instrument.upper(),
                    "events_count": len(impacts),
                    "long_score": long_score,
                    "short_score": short_score,
                    "net_bias_score": net,
                    "dominant_direction": dominant,
                }
            )
        return result
