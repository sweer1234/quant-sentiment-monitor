from __future__ import annotations

from collections import defaultdict
from copy import deepcopy
from datetime import date, datetime, timedelta
import hashlib
import hmac
import json
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
        self.multi_user_rules: dict[str, Any] = {}
        self.topic_taxonomy: dict[str, Any] = {}
        self.investment_event_catalog: dict[str, Any] = {}
        self.alert_governance_rules: dict[str, Any] = {}
        self.source_compliance_registry: dict[str, Any] = {}
        self.portfolio_impact_rules: dict[str, Any] = {}
        self.feedback_learning_rules: dict[str, Any] = {}
        self.billing_sla_rules: dict[str, Any] = {}
        self.event_calendar_rules: dict[str, Any] = {}
        self.webhook_delivery_rules: dict[str, Any] = {}
        self.events: dict[str, Event] = {}
        self.manual_messages: dict[str, ManualMessageRecord] = {}
        self.alert_policies: dict[str, Any] = {
            "dedup_window_minutes": 45,
            "cooldown_minutes": {"P0": 5, "P1": 10, "P2": 30},
            "channels_order": ["app", "im", "email"],
            "allow_revoke": True,
        }
        self.users: dict[str, dict[str, Any]] = {
            "sweer1234": {"password": "dev123", "role": "admin"},
            "adollman": {"password": "dev123", "role": "trader"},
            "demo": {"password": "demo123", "role": "analyst"},
        }
        self.tokens: dict[str, str] = {}
        self.user_preferences: dict[str, dict[str, Any]] = {}
        self.user_alert_subscriptions: dict[str, dict[str, Any]] = {}
        self.user_topic_subscriptions: dict[str, set[str]] = {}
        self.revoked_alerts: dict[str, dict[str, Any]] = {}
        self.feedback_records: list[dict[str, Any]] = []
        self.webhook_subscriptions: dict[str, dict[str, Any]] = {}
        self.webhook_deliveries: list[dict[str, Any]] = []
        self.webhook_queue: list[dict[str, Any]] = []
        self.calendar_events: dict[str, dict[str, Any]] = {}
        self._source_by_id: dict[str, dict[str, Any]] = {}
        self.reload_configs()
        self._seed_events()
        self._seed_calendar_events()

    def reload_configs(self) -> dict[str, Any]:
        with self._lock:
            default_data = _load_yaml(self.settings.source_registry_default_path)
            override_data = _load_yaml(self.settings.source_registry_override_path)
            self.source_weight_rules = _load_yaml(self.settings.source_weight_rules_path)
            self.manual_input_rules = _load_yaml(self.settings.manual_input_rules_path)
            self.multi_user_rules = _load_yaml(self.settings.multi_user_rules_path)
            self.topic_taxonomy = _load_yaml(self.settings.topic_taxonomy_path)
            self.investment_event_catalog = _load_yaml(self.settings.investment_event_catalog_path)
            self.alert_governance_rules = _load_yaml(self.settings.alert_governance_rules_path)
            self.source_compliance_registry = _load_yaml(self.settings.source_compliance_registry_path)
            self.portfolio_impact_rules = _load_yaml(self.settings.portfolio_impact_rules_path)
            self.feedback_learning_rules = _load_yaml(self.settings.feedback_learning_rules_path)
            self.billing_sla_rules = _load_yaml(self.settings.billing_sla_rules_path)
            self.event_calendar_rules = _load_yaml(self.settings.event_calendar_rules_path)
            self.webhook_delivery_rules = _load_yaml(self.settings.webhook_delivery_rules_path)

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
            self.alert_policies = {
                "dedup_window_minutes": self.alert_governance_rules.get("deduplication", {}).get("time_window_minutes", 45),
                "cooldown_minutes": self.alert_governance_rules.get("suppression", {}).get(
                    "cooldown_minutes", {"P0": 5, "P1": 10, "P2": 30}
                ),
                "channels_order": ["app", "im", "email"],
                "allow_revoke": self.alert_governance_rules.get("correction_and_recall", {}).get("allow_revoke", True),
            }
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

    def _seed_calendar_events(self) -> None:
        if self.calendar_events:
            return
        base = now_utc()
        seeded = [
            {
                "calendar_event_id": "cal_us_cpi_next",
                "country": "US",
                "event_name": "US CPI YoY",
                "importance_level": "P0",
                "event_time": (base + timedelta(days=2)).isoformat(),
                "consensus": 3.2,
                "actual": None,
                "unit": "%",
                "source": "bls",
            },
            {
                "calendar_event_id": "cal_us_nfp_last",
                "country": "US",
                "event_name": "US Nonfarm Payrolls",
                "importance_level": "P1",
                "event_time": (base - timedelta(days=8)).isoformat(),
                "consensus": 180.0,
                "actual": 235.0,
                "unit": "k",
                "source": "bls",
            },
            {
                "calendar_event_id": "cal_eu_rate_next",
                "country": "EU",
                "event_name": "ECB Rate Decision",
                "importance_level": "P0",
                "event_time": (base + timedelta(days=5)).isoformat(),
                "consensus": 4.0,
                "actual": None,
                "unit": "%",
                "source": "ecb",
            },
        ]
        for item in seeded:
            self.calendar_events[item["calendar_event_id"]] = item

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

    def login(self, username: str, password: str) -> dict[str, Any] | None:
        user = self.users.get(username)
        if not user or user.get("password") != password:
            return None
        access_token = f"token-{username}-{uuid4().hex[:10]}"
        self.tokens[access_token] = username
        if username not in self.user_preferences:
            self.user_preferences[username] = {
                "focus_domains": [],
                "focus_keywords": [],
                "focus_markets": [],
                "focus_instruments": [],
                "alert_level_min": "P2",
            }
        if username not in self.user_alert_subscriptions:
            self.user_alert_subscriptions[username] = {"channels": ["app"], "level_min": "P2", "muted": False}
        return {
            "access_token": access_token,
            "token_type": "bearer",
            "expires_in": int(self.multi_user_rules.get("auth", {}).get("access_token_expire_minutes", 120) * 60),
            "user": {"username": username, "role": user.get("role", "analyst")},
        }

    def username_by_token(self, token: str) -> str | None:
        return self.tokens.get(token)

    def user_role(self, username: str) -> str:
        return str(self.users.get(username, {}).get("role", "analyst"))

    def has_permission(self, username: str, action: str) -> bool:
        role = self.user_role(username)
        permissions = {
            "admin": {
                "sources.write",
                "alerts.write",
                "alerts.revoke",
                "manual.review",
                "webhooks.manage",
                "calendar.manage",
                "feedback.write",
            },
            "trader": {"alerts.revoke", "feedback.write", "webhooks.manage"},
            "analyst": {"feedback.write"},
        }
        return action in permissions.get(role, set())

    def get_user_profile(self, username: str) -> dict[str, Any]:
        user = self.users.get(username, {"role": "analyst"})
        return {
            "username": username,
            "role": user.get("role", "analyst"),
            "preferences": deepcopy(self.user_preferences.get(username, {})),
            "alert_subscriptions": deepcopy(self.user_alert_subscriptions.get(username, {})),
            "topic_subscriptions": sorted(self.user_topic_subscriptions.get(username, set())),
        }

    def update_user_preferences(self, username: str, payload: dict[str, Any]) -> dict[str, Any]:
        current = self.user_preferences.setdefault(
            username,
            {"focus_domains": [], "focus_keywords": [], "focus_markets": [], "focus_instruments": [], "alert_level_min": "P2"},
        )
        for key, value in payload.items():
            if value is not None:
                current[key] = value
        return deepcopy(current)

    def update_user_alert_subscriptions(self, username: str, payload: dict[str, Any]) -> dict[str, Any]:
        current = self.user_alert_subscriptions.setdefault(username, {"channels": ["app"], "level_min": "P2", "muted": False})
        for key, value in payload.items():
            if value is not None:
                current[key] = value
        return deepcopy(current)

    def update_topic_subscriptions(self, username: str, topic_ids: list[str]) -> list[str]:
        available = {item.get("topic_id") for item in self.topic_taxonomy.get("topics", [])}
        selected = {topic_id for topic_id in topic_ids if topic_id in available}
        self.user_topic_subscriptions[username] = selected
        return sorted(selected)

    def topic_catalog(self) -> list[dict[str, Any]]:
        return deepcopy(self.topic_taxonomy.get("topics", []))

    def domain_catalog(self) -> list[dict[str, Any]]:
        return deepcopy(self.investment_event_catalog.get("domains", []))

    def personalized_feed(self, username: str, *, page: int, page_size: int, importance_min: float | None = None) -> dict[str, Any]:
        preferences = self.user_preferences.get(username, {})
        keyword_set = {item.lower() for item in preferences.get("focus_keywords", [])}
        market_set = {item.lower() for item in preferences.get("focus_markets", [])}
        instrument_set = {item.upper() for item in preferences.get("focus_instruments", [])}
        domain_set = {item.lower() for item in preferences.get("focus_domains", [])}

        ranked_events = []
        for event in self.list_events():
            if importance_min is not None and event.importance_score < importance_min:
                continue
            text = f"{event.title} {event.content}".lower()
            event_markets = {item.lower() for item in event.impacted_markets}
            event_instruments = {impact.instrument.upper() for impact in event.impacts}
            domain_match = 1.0 if any(domain in event.event_type.lower() for domain in domain_set) and domain_set else 0.0
            keyword_match = 1.0 if keyword_set and any(keyword in text for keyword in keyword_set) else 0.0
            market_match = len(event_markets & market_set) / max(len(market_set), 1) if market_set else 0.0
            instrument_match = len(event_instruments & instrument_set) / max(len(instrument_set), 1) if instrument_set else 0.0
            relevance = round(0.40 * domain_match + 0.30 * keyword_match + 0.20 * instrument_match + 0.10 * market_match, 4)
            payload = event.model_dump(mode="json")
            payload["user_relevance_score"] = relevance
            ranked_events.append(payload)

        ranked_events.sort(key=lambda item: (item["user_relevance_score"], item["importance_score"]), reverse=True)
        start = (page - 1) * page_size
        return {"page": page, "page_size": page_size, "total": len(ranked_events), "events": ranked_events[start : start + page_size]}

    def topic_feed(self, username: str, topic_ids: list[str], *, page: int, page_size: int) -> dict[str, Any]:
        selected = set(topic_ids) if topic_ids else self.user_topic_subscriptions.get(username, set())
        topic_map = {item.get("topic_id"): item for item in self.topic_taxonomy.get("topics", [])}
        keywords = []
        for topic_id in selected:
            keywords.extend(topic_map.get(topic_id, {}).get("keywords", []))
        keyword_set = {item.lower() for item in keywords}

        result = []
        for event in self.list_events():
            text = f"{event.title} {event.content}".lower()
            if keyword_set and not any(keyword in text for keyword in keyword_set):
                continue
            payload = event.model_dump(mode="json")
            payload["matched_topics"] = sorted(selected)
            result.append(payload)
        start = (page - 1) * page_size
        return {"page": page, "page_size": page_size, "total": len(result), "events": result[start : start + page_size]}

    def portfolio_impact(self, portfolio_id: str, holdings: list[dict[str, Any]], event_ids: list[str]) -> dict[str, Any]:
        selected_events = [self.events[event_id] for event_id in event_ids if event_id in self.events] if event_ids else list(self.events.values())
        impact_by_instrument: dict[str, list[ImpactItem]] = defaultdict(list)
        for event in selected_events:
            for impact in event.impacts:
                impact_by_instrument[impact.instrument.upper()].append(impact)

        net_impact = 0.0
        drivers: list[dict[str, Any]] = []
        for holding in holdings:
            instrument = str(holding.get("instrument", "")).upper()
            weight = float(holding.get("weight", 0.0))
            impacts = impact_by_instrument.get(instrument, [])
            if not impacts:
                continue
            avg_net = sum(item.net_bias_score for item in impacts) / len(impacts)
            contribution = avg_net * weight
            net_impact += contribution
            drivers.append(
                {
                    "instrument": instrument,
                    "weight": weight,
                    "avg_net_bias": round(avg_net, 2),
                    "contribution": round(contribution, 2),
                    "events_count": len(impacts),
                }
            )

        risk_delta = {
            "equity_beta": round(net_impact / 200, 4),
            "duration": round(-net_impact / 250, 4),
            "fx_exposure": round(net_impact / 180, 4),
            "crypto_var": round(abs(net_impact) / 120, 4),
        }
        return {
            "portfolio_id": portfolio_id,
            "net_impact_score": round(max(-100.0, min(100.0, net_impact)), 2),
            "risk_delta": risk_delta,
            "drivers": sorted(drivers, key=lambda item: abs(item["contribution"]), reverse=True),
        }

    def event_credibility(self, event_id: str) -> dict[str, Any] | None:
        event = self.events.get(event_id)
        if not event:
            return None
        source = self._source_by_id.get(event.source_id, {})
        score = round(
            0.45 * float(source.get("credibility_weight", 0.75))
            + 0.35 * float(source.get("source_weight", 0.7))
            + 0.20 * (1.0 if event.credibility_level == "official" else 0.7),
            4,
        )
        level = "official" if score >= 0.9 else "verified" if score >= 0.65 else "rumor"
        return {"event_id": event_id, "credibility_score": score, "credibility_level": level, "evidence": event.evidence}

    def source_compliance(self, source_id: str) -> dict[str, Any]:
        default_policy = self.source_compliance_registry.get("default_policy", {})
        source_items = {item.get("source_id"): item for item in self.source_compliance_registry.get("sources", [])}
        detail = deepcopy(source_items.get(source_id, {}))
        return {"source_id": source_id, "default_policy": default_policy, "compliance": detail}

    def update_alert_policies(self, payload: dict[str, Any]) -> dict[str, Any]:
        for key, value in payload.items():
            if value is not None:
                self.alert_policies[key] = value
        return deepcopy(self.alert_policies)

    def revoke_alert(self, alert_id: str, reason: str) -> dict[str, Any]:
        revoked = {
            "alert_id": alert_id,
            "status": "revoked",
            "reason": reason,
            "revoked_at": now_utc().isoformat(),
            "correction_notice_required": bool(self.alert_governance_rules.get("correction_and_recall", {}).get("correction_notice_required", True)),
        }
        self.revoked_alerts[alert_id] = revoked
        return revoked

    def add_feedback(self, username: str, event_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        record = {
            "feedback_id": f"fb_{uuid4().hex[:10]}",
            "event_id": event_id,
            "username": username,
            "feedback_type": payload.get("feedback_type"),
            "score": payload.get("score"),
            "comment": payload.get("comment", ""),
            "created_at": now_utc().isoformat(),
        }
        self.feedback_records.append(record)
        return record

    def billing_usage(self, tenant_id: str, period: str) -> dict[str, Any]:
        total_events = len(self.events)
        total_alerts = len(self.events) + len(self.manual_messages)
        return {
            "tenant_id": tenant_id,
            "period": period,
            "events_used": total_events,
            "alerts_used": total_alerts,
            "plan": "pro",
            "quota": self.billing_sla_rules.get("tiers", {}).get("pro", {}),
        }

    def sla_status(self, tenant_id: str) -> dict[str, Any]:
        pro_sla = self.billing_sla_rules.get("tiers", {}).get("pro", {}).get("sla", {})
        return {
            "tenant_id": tenant_id,
            "availability_target": pro_sla.get("availability", 99.9),
            "p95_latency_target_ms": pro_sla.get("p95_latency_ms", 1000),
            "availability_rolling_30d": 99.97,
            "p95_latency_ms": 680,
            "status": "healthy",
        }

    def list_calendar_events(
        self,
        *,
        country: str | None,
        importance_min: str | None,
        from_date: date | None,
        to_date: date | None,
    ) -> list[dict[str, Any]]:
        rank = {"P0": 3, "P1": 2, "P2": 1}
        min_rank = rank.get(importance_min or "P2", 1)
        items = []
        for item in self.calendar_events.values():
            event_dt = datetime.fromisoformat(str(item.get("event_time")))
            if country and str(item.get("country", "")).upper() != country.upper():
                continue
            if rank.get(str(item.get("importance_level", "P2")), 1) < min_rank:
                continue
            if from_date and event_dt.date() < from_date:
                continue
            if to_date and event_dt.date() > to_date:
                continue
            items.append(deepcopy(item))
        items.sort(key=lambda row: row["event_time"])
        return items

    def calendar_surprise(self, calendar_event_id: str) -> dict[str, Any] | None:
        item = self.calendar_events.get(calendar_event_id)
        if not item:
            return None
        consensus = item.get("consensus")
        actual = item.get("actual")
        if actual is None or consensus is None:
            return {
                "calendar_event_id": calendar_event_id,
                "status": "pending",
                "message": "actual value not released",
            }
        surprise = 0.0 if float(consensus) == 0 else (float(actual) - float(consensus)) / abs(float(consensus))
        surprise = round(surprise, 4)
        direction = "positive" if surprise > 0 else "negative" if surprise < 0 else "neutral"
        return {
            "calendar_event_id": calendar_event_id,
            "status": "available",
            "consensus": consensus,
            "actual": actual,
            "surprise_ratio": surprise,
            "direction": direction,
        }

    def upsert_calendar_event(self, payload: dict[str, Any]) -> dict[str, Any]:
        event_id = payload.get("calendar_event_id") or f"cal_{uuid4().hex[:10]}"
        current = self.calendar_events.get(event_id, {})
        current.update(payload)
        current["calendar_event_id"] = event_id
        current.setdefault("importance_level", "P1")
        current.setdefault("event_time", now_utc().isoformat())
        self.calendar_events[event_id] = current
        return deepcopy(current)

    def _build_webhook_payload(self, sample_event: dict[str, Any] | None) -> dict[str, Any]:
        return {
            "event_type": "event.created",
            "sent_at": now_utc().isoformat(),
            "data": sample_event or {},
        }

    def _sign_webhook_payload(self, payload: dict[str, Any], secret: str | None) -> tuple[str | None, str]:
        payload_raw = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
        payload_hash = hashlib.sha256(payload_raw).hexdigest()
        if not secret:
            return None, payload_hash
        signature = hmac.new(secret.encode("utf-8"), payload_raw, hashlib.sha256).hexdigest()
        return signature, payload_hash

    def _simulate_webhook_send(self, url: str, *, force_fail: bool = False) -> tuple[bool, str]:
        if force_fail:
            return False, "forced_failure"
        if "fail" in url.lower():
            return False, "simulated_endpoint_failure"
        return True, "ok"

    def _webhook_retry_policy(self) -> dict[str, Any]:
        retry = self.webhook_delivery_rules.get("retry", {})
        return {
            "base_delay_sec": int(retry.get("base_delay_sec", 2)),
            "backoff_multiplier": float(retry.get("backoff_multiplier", 2.0)),
            "max_delay_sec": int(retry.get("max_delay_sec", 60)),
            "max_retries_default": int(retry.get("max_retries_default", 2)),
            "timeout_sec_default": int(retry.get("timeout_sec_default", 5)),
        }

    def _build_webhook_payload(self, sample_event: dict[str, Any] | None) -> dict[str, Any]:
        return {
            "event_type": "event.created",
            "sent_at": now_utc().isoformat(),
            "data": sample_event or {},
        }

    def _sign_webhook_payload(self, payload: dict[str, Any], secret: str | None) -> tuple[str | None, str]:
        payload_raw = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
        payload_hash = hashlib.sha256(payload_raw).hexdigest()
        if not secret:
            return None, payload_hash
        signature = hmac.new(secret.encode("utf-8"), payload_raw, hashlib.sha256).hexdigest()
        return signature, payload_hash

    def _simulate_webhook_send(self, url: str, *, force_fail: bool = False) -> tuple[bool, str]:
        if force_fail:
            return False, "forced_failure"
        if "fail" in url.lower():
            return False, "simulated_endpoint_failure"
        return True, "ok"

    def _calc_backoff_delay(self, retry_count: int) -> int:
        policy = self._webhook_retry_policy()
        delay = int(policy["base_delay_sec"] * (policy["backoff_multiplier"] ** max(retry_count - 1, 0)))
        return min(delay, int(policy["max_delay_sec"]))

    def _mask_webhook_record(self, record: dict[str, Any]) -> dict[str, Any]:
        public_record = deepcopy(record)
        if public_record.get("secret"):
            public_record["secret"] = "***"
        return public_record

    def create_webhook_subscription(self, username: str, payload: dict[str, Any]) -> dict[str, Any]:
        policy = self._webhook_retry_policy()
        subscription_id = f"wh_{uuid4().hex[:10]}"
        record = {
            "subscription_id": subscription_id,
            "owner": username,
            "name": payload.get("name", subscription_id),
            "url": payload.get("url"),
            "events": payload.get("events", ["event.created"]),
            "enabled": bool(payload.get("enabled", True)),
            "secret": payload.get("secret"),
            "max_retries": int(payload.get("max_retries", policy["max_retries_default"])),
            "timeout_sec": int(payload.get("timeout_sec", policy["timeout_sec_default"])),
            "created_at": now_utc().isoformat(),
            "last_delivery_at": None,
            "deliveries": 0,
            "failed_deliveries": 0,
        }
        self.webhook_subscriptions[subscription_id] = record
        return self._mask_webhook_record(record)

    def list_webhook_subscriptions(self, username: str | None = None) -> list[dict[str, Any]]:
        rows = []
        for record in self.webhook_subscriptions.values():
            if username and record.get("owner") != username:
                continue
            rows.append(self._mask_webhook_record(record))
        rows.sort(key=lambda item: item["created_at"], reverse=True)
        return rows

    def delete_webhook_subscription(self, subscription_id: str, username: str) -> bool:
        record = self.webhook_subscriptions.get(subscription_id)
        if not record:
            return False
        if record.get("owner") != username and self.user_role(username) != "admin":
            return False
        del self.webhook_subscriptions[subscription_id]
        return True

    def dispatch_webhook_test(self, event_id: str | None = None, *, force_fail: bool = False) -> dict[str, Any]:
        queued = 0
        sample_event = None
        if event_id and event_id in self.events:
            sample_event = self.events[event_id].model_dump(mode="json")
        elif self.events:
            sample_event = next(iter(self.events.values())).model_dump(mode="json")
        payload = self._build_webhook_payload(sample_event)
        now = now_utc().isoformat()
        for subscription_id, subscription in self.webhook_subscriptions.items():
            if not subscription.get("enabled", True):
                continue
            signature, payload_hash = self._sign_webhook_payload(payload, subscription.get("secret"))
            delivery = {
                "delivery_id": f"wd_{uuid4().hex[:10]}",
                "subscription_id": subscription_id,
                "status": "queued",
                "retry_count": 0,
                "max_retries": int(subscription.get("max_retries", 2)),
                "can_retry": int(subscription.get("max_retries", 2)) > 0,
                "error_reason": None,
                "signature": signature,
                "payload_hash": payload_hash,
                "payload": payload,
                "created_at": now,
                "delivered_at": None,
                "next_retry_at": now,
            }
            self.webhook_deliveries.append(delivery)
            self.webhook_queue.append(
                {
                    "job_id": f"wq_{uuid4().hex[:10]}",
                    "delivery_id": delivery["delivery_id"],
                    "subscription_id": subscription_id,
                    "event_id": sample_event.get("event_id") if sample_event else None,
                    "status": "queued",
                    "scheduled_at": now,
                    "force_fail_once": force_fail,
                }
            )
            queued += 1
        return {
            "status": "ok",
            "queued_subscriptions": queued,
            "event_id": sample_event.get("event_id") if sample_event else None,
            "dispatched_at": now,
            "force_fail": force_fail,
        }

    def list_webhook_deliveries(
        self,
        *,
        subscription_id: str | None = None,
        status: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        rows = []
        for delivery in reversed(self.webhook_deliveries):
            if subscription_id and delivery.get("subscription_id") != subscription_id:
                continue
            if status and str(delivery.get("status")) != status:
                continue
            rows.append(deepcopy(delivery))
            if len(rows) >= limit:
                break
        return rows

    def retry_failed_webhooks(self, *, limit: int = 20) -> dict[str, Any]:
        retried = 0
        requeued = 0
        for delivery in self.webhook_deliveries:
            if retried >= limit:
                break
            if delivery.get("status") != "failed":
                continue
            subscription = self.webhook_subscriptions.get(str(delivery.get("subscription_id")))
            if not subscription or not subscription.get("enabled", True):
                continue

            retried += 1
            delivery["retry_count"] = 0
            delivery["can_retry"] = True
            delivery["status"] = "retrying"
            delivery["error_reason"] = None
            delivery["next_retry_at"] = now_utc().isoformat()
            self.webhook_queue.append(
                {
                    "job_id": f"wq_{uuid4().hex[:10]}",
                    "delivery_id": delivery["delivery_id"],
                    "subscription_id": delivery["subscription_id"],
                    "event_id": delivery.get("payload", {}).get("data", {}).get("event_id"),
                    "status": "queued",
                    "scheduled_at": now_utc().isoformat(),
                    "force_fail_once": False,
                }
            )
            requeued += 1

        return {
            "status": "ok",
            "retried": retried,
            "requeued": requeued,
        }

    def process_webhook_queue(self, *, limit: int = 50) -> dict[str, Any]:
        processed = 0
        delivered = 0
        failed = 0
        requeued = 0
        now = now_utc()
        remaining_jobs: list[dict[str, Any]] = []
        for job in self.webhook_queue:
            if processed >= limit:
                remaining_jobs.append(job)
                continue
            scheduled_at = datetime.fromisoformat(str(job.get("scheduled_at")))
            if scheduled_at > now:
                remaining_jobs.append(job)
                continue

            delivery = next((item for item in self.webhook_deliveries if item.get("delivery_id") == job.get("delivery_id")), None)
            subscription = self.webhook_subscriptions.get(str(job.get("subscription_id")))
            if not delivery or not subscription or not subscription.get("enabled", True):
                continue

            processed += 1
            force_fail = bool(job.get("force_fail_once", False)) and int(delivery.get("retry_count", 0)) == 0
            ok, reason = self._simulate_webhook_send(str(subscription.get("url", "")), force_fail=force_fail)
            if ok:
                delivery["status"] = "delivered"
                delivery["error_reason"] = None
                delivery["can_retry"] = False
                delivery["delivered_at"] = now_utc().isoformat()
                delivery["next_retry_at"] = None
                subscription["deliveries"] = int(subscription.get("deliveries", 0)) + 1
                subscription["last_delivery_at"] = now_utc().isoformat()
                delivered += 1
                continue

            # failed attempt
            retry_count = int(delivery.get("retry_count", 0)) + 1
            max_retries = int(delivery.get("max_retries", 0))
            delivery["retry_count"] = retry_count
            delivery["error_reason"] = reason
            subscription["failed_deliveries"] = int(subscription.get("failed_deliveries", 0)) + 1
            subscription["last_delivery_at"] = now_utc().isoformat()
            can_retry = retry_count <= max_retries
            delivery["can_retry"] = can_retry
            if can_retry:
                delay = self._calc_backoff_delay(retry_count)
                next_retry = (now_utc() + timedelta(seconds=delay)).isoformat()
                delivery["status"] = "retrying"
                delivery["next_retry_at"] = next_retry
                requeued += 1
                remaining_jobs.append(
                    {
                        "job_id": f"wq_{uuid4().hex[:10]}",
                        "delivery_id": delivery["delivery_id"],
                        "subscription_id": delivery["subscription_id"],
                        "event_id": job.get("event_id"),
                        "status": "queued",
                        "scheduled_at": next_retry,
                        "force_fail_once": False,
                    }
                )
            else:
                delivery["status"] = "failed"
                delivery["next_retry_at"] = None
                failed += 1

        self.webhook_queue = remaining_jobs
        return {
            "status": "ok",
            "processed": processed,
            "delivered": delivered,
            "failed": failed,
            "requeued": requeued,
            "queue_size": len(self.webhook_queue),
        }

    def webhook_stats(self) -> dict[str, Any]:
        deliveries = len(self.webhook_deliveries)
        delivered = sum(1 for item in self.webhook_deliveries if item.get("status") == "delivered")
        failed = sum(1 for item in self.webhook_deliveries if item.get("status") == "failed")
        queued = sum(1 for item in self.webhook_deliveries if item.get("status") == "queued")
        retrying = sum(1 for item in self.webhook_deliveries if item.get("status") == "retrying")
        success_rate = round((delivered / deliveries) * 100, 2) if deliveries else 0.0

        retry_counts = sorted(int(item.get("retry_count", 0)) for item in self.webhook_deliveries)
        if retry_counts:
            p50_index = int(round(0.50 * (len(retry_counts) - 1)))
            p95_index = int(round(0.95 * (len(retry_counts) - 1)))
            retry_p50 = retry_counts[p50_index]
            retry_p95 = retry_counts[p95_index]
        else:
            retry_p50 = 0
            retry_p95 = 0

        return {
            "subscriptions_total": len(self.webhook_subscriptions),
            "deliveries_total": deliveries,
            "delivered_total": delivered,
            "failed_total": failed,
            "queued_total": queued,
            "retrying_total": retrying,
            "queue_size": len(self.webhook_queue),
            "success_rate_pct": success_rate,
            "retry_count_p50": retry_p50,
            "retry_count_p95": retry_p95,
            "retry_policy": self._webhook_retry_policy(),
        }
