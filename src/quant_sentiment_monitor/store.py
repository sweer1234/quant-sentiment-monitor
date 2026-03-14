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
        self.alerts: dict[str, dict[str, Any]] = {}
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
        self.alert_acks: list[dict[str, Any]] = []
        self.alert_escalations: list[dict[str, Any]] = []
        self.feedback_records: list[dict[str, Any]] = []
        self.ingest_stats: dict[str, int] = {"total": 0, "deduplicated": 0, "accepted": 0}
        self.audit_logs: list[dict[str, Any]] = []
        self.webhook_subscriptions: dict[str, dict[str, Any]] = {}
        self.webhook_deliveries: list[dict[str, Any]] = []
        self.webhook_queue: list[dict[str, Any]] = []
        self.webhook_dlq: list[dict[str, Any]] = []
        self.calendar_events: dict[str, dict[str, Any]] = {}
        self._source_by_id: dict[str, dict[str, Any]] = {}
        self.reload_configs()
        loaded = self._load_state()
        if not loaded:
            self._seed_events()
            self._seed_calendar_events()
            self._persist_state()
        elif not self.calendar_events:
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

    def _persist_state(self) -> None:
        state_file = self.settings.state_path_obj
        state_file.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "events": [event.model_dump(mode="json") for event in self.events.values()],
            "manual_messages": [item.model_dump(mode="json") for item in self.manual_messages.values()],
            "alerts": list(self.alerts.values()),
            "alert_acks": self.alert_acks,
            "alert_escalations": self.alert_escalations,
            "user_preferences": self.user_preferences,
            "user_alert_subscriptions": self.user_alert_subscriptions,
            "user_topic_subscriptions": {k: sorted(v) for k, v in self.user_topic_subscriptions.items()},
            "feedback_records": self.feedback_records,
            "ingest_stats": self.ingest_stats,
            "audit_logs": self.audit_logs,
            "webhook_subscriptions": list(self.webhook_subscriptions.values()),
            "webhook_deliveries": self.webhook_deliveries,
            "webhook_queue": self.webhook_queue,
            "webhook_dlq": self.webhook_dlq,
            "calendar_events": list(self.calendar_events.values()),
        }
        with state_file.open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

    def _load_state(self) -> bool:
        state_file = self.settings.state_path_obj
        if not state_file.exists():
            return False
        try:
            with state_file.open("r", encoding="utf-8") as f:
                payload = json.load(f)
        except Exception:
            return False

        try:
            self.events = {item["event_id"]: Event(**item) for item in payload.get("events", [])}
            self.manual_messages = {item["manual_message_id"]: ManualMessageRecord(**item) for item in payload.get("manual_messages", [])}
            self.alerts = {item["alert_id"]: item for item in payload.get("alerts", [])}
            self.alert_acks = list(payload.get("alert_acks", []))
            self.alert_escalations = list(payload.get("alert_escalations", []))
            self.user_preferences = dict(payload.get("user_preferences", {}))
            self.user_alert_subscriptions = dict(payload.get("user_alert_subscriptions", {}))
            self.user_topic_subscriptions = {k: set(v) for k, v in dict(payload.get("user_topic_subscriptions", {})).items()}
            self.feedback_records = list(payload.get("feedback_records", []))
            self.ingest_stats = dict(payload.get("ingest_stats", {"total": 0, "deduplicated": 0, "accepted": 0}))
            self.audit_logs = list(payload.get("audit_logs", []))
            self.webhook_subscriptions = {
                item["subscription_id"]: item for item in payload.get("webhook_subscriptions", []) if "subscription_id" in item
            }
            self.webhook_deliveries = list(payload.get("webhook_deliveries", []))
            self.webhook_queue = list(payload.get("webhook_queue", []))
            self.webhook_dlq = list(payload.get("webhook_dlq", []))
            self.calendar_events = {
                item["calendar_event_id"]: item for item in payload.get("calendar_events", []) if "calendar_event_id" in item
            }
            return True
        except Exception:
            return False

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
            self._create_alert_for_event(event)

    def _event_fingerprint(self, event: Event) -> str:
        key = "|".join(
            [
                event.title.strip().lower(),
                event.event_type.strip().lower(),
                ",".join(sorted(item.lower() for item in event.impacted_markets)),
            ]
        )
        return hashlib.sha256(key.encode("utf-8")).hexdigest()

    def _alert_level_rank(self, level: str) -> int:
        return {"P0": 3, "P1": 2, "P2": 1}.get(level, 1)

    def _event_matches_user(self, username: str, event: Event) -> bool:
        preferences = self.user_preferences.get(username, {})
        keywords = {item.lower() for item in preferences.get("focus_keywords", [])}
        markets = {item.lower() for item in preferences.get("focus_markets", [])}
        instruments = {item.upper() for item in preferences.get("focus_instruments", [])}
        min_level = preferences.get("alert_level_min") or self.user_alert_subscriptions.get(username, {}).get("level_min", "P2")
        if self._alert_level_rank(event.importance_level) < self._alert_level_rank(str(min_level)):
            return False
        if self.user_alert_subscriptions.get(username, {}).get("muted", False):
            return False

        text = f"{event.title} {event.content}".lower()
        event_markets = {item.lower() for item in event.impacted_markets}
        event_instruments = {impact.instrument.upper() for impact in event.impacts}
        keyword_ok = not keywords or any(keyword in text for keyword in keywords)
        market_ok = not markets or bool(event_markets & markets)
        instrument_ok = not instruments or bool(event_instruments & instruments)
        return keyword_ok and market_ok and instrument_ok

    def _create_alert_for_event(self, event: Event) -> dict[str, Any] | None:
        threshold = {"P0": 85, "P1": 70, "P2": 0}
        if event.importance_score < threshold.get(event.importance_level, 0):
            return None

        dedup_window = int(self.alert_policies.get("dedup_window_minutes", 45))
        event_fp = self._event_fingerprint(event)
        now = now_utc()
        for existing in self.alerts.values():
            if existing.get("fingerprint") != event_fp:
                continue
            created_at = datetime.fromisoformat(str(existing.get("created_at")))
            if (now - created_at) <= timedelta(minutes=dedup_window):
                existing["suppressed_duplicates"] = int(existing.get("suppressed_duplicates", 0)) + 1
                return None

        target_users = []
        for username in self.users.keys():
            if self._event_matches_user(username, event):
                target_users.append(username)

        alert_id = f"al_{uuid4().hex[:10]}"
        alert = {
            "alert_id": alert_id,
            "event_id": event.event_id,
            "title": event.title,
            "importance_level": event.importance_level,
            "importance_score": event.importance_score,
            "status": "active",
            "acked_by": None,
            "acked_at": None,
            "channels": self.alert_policies.get("channels_order", ["app", "im", "email"]),
            "target_users": target_users,
            "fingerprint": event_fp,
            "suppressed_duplicates": 0,
            "created_at": now.isoformat(),
            "updated_at": now.isoformat(),
        }
        self.alerts[alert_id] = alert
        return alert

    def _find_duplicate_event(
        self,
        *,
        source_id: str,
        title: str,
        content: str,
        published_at: datetime | None,
    ) -> Event | None:
        normalized_title = title.strip().lower()
        normalized_content = content.strip().lower()
        window_minutes = int(self.alert_policies.get("dedup_window_minutes", 45))
        baseline = published_at or now_utc()
        for event in self.events.values():
            if event.source_id != source_id:
                continue
            if event.title.strip().lower() != normalized_title:
                continue
            if event.content.strip().lower() != normalized_content:
                continue
            if abs((event.published_at - baseline).total_seconds()) > window_minutes * 60:
                continue
            return event
        return None

    def ingest_event(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.ingest_stats["total"] = int(self.ingest_stats.get("total", 0)) + 1
        source_id = str(payload.get("source_id", "")).strip()
        if not source_id:
            raise ValueError("source_id required")
        title = str(payload.get("title", "")).strip()
        content = str(payload.get("content", "")).strip()
        if not title or not content:
            raise ValueError("title/content required")
        if bool(payload.get("publish_external", False)):
            allowed, reason = self.can_publish_source_externally(source_id)
            if not allowed:
                raise ValueError(f"compliance blocked external publish: {reason}")

        published_at = None
        if payload.get("published_at"):
            published_at = datetime.fromisoformat(str(payload["published_at"]))
        duplicate = self._find_duplicate_event(source_id=source_id, title=title, content=content, published_at=published_at)
        if duplicate:
            self.ingest_stats["deduplicated"] = int(self.ingest_stats.get("deduplicated", 0)) + 1
            self._audit("event.ingest_deduplicated", "system", {"event_id": duplicate.event_id, "source_id": source_id})
            self._persist_state()
            return {
                "event": duplicate.model_dump(mode="json"),
                "alert": None,
                "deduplicated": True,
            }

        event = self._build_event(
            source_id=source_id,
            title=title,
            content=content,
            related_instruments=payload.get("related_instruments", []),
        )
        if payload.get("event_type"):
            event.event_type = str(payload["event_type"])
        if payload.get("language"):
            event.language = str(payload["language"])
        if published_at:
            event.published_at = published_at
        if payload.get("credibility_level"):
            event.credibility_level = str(payload["credibility_level"])
        if payload.get("evidence"):
            event.evidence = list(payload.get("evidence", []))

        self.events[event.event_id] = event
        alert = self._create_alert_for_event(event)
        self.ingest_stats["accepted"] = int(self.ingest_stats.get("accepted", 0)) + 1
        self._audit("event.ingest", "system", {"event_id": event.event_id, "source_id": source_id, "alert_created": bool(alert)})
        self._persist_state()
        return {
            "event": event.model_dump(mode="json"),
            "alert": deepcopy(alert) if alert else None,
            "deduplicated": False,
        }

    def batch_ingest_events(self, payloads: list[dict[str, Any]], request_id: str | None = None) -> dict[str, Any]:
        results = []
        accepted = 0
        deduplicated = 0
        rejected = 0
        for item in payloads:
            try:
                result = self.ingest_event(item)
                results.append({"ok": True, **result})
                if result.get("deduplicated"):
                    deduplicated += 1
                else:
                    accepted += 1
            except Exception as exc:
                rejected += 1
                results.append({"ok": False, "error": str(exc), "event": item})
        return {
            "request_id": request_id,
            "total": len(payloads),
            "accepted": accepted,
            "deduplicated": deduplicated,
            "rejected": rejected,
            "results": results,
        }

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
            self._persist_state()
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
            self._create_alert_for_event(event)
        self._persist_state()
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
        self._persist_state()
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
        self._persist_state()
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
        self._audit("auth.login", username, {"role": user.get("role", "analyst")})
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
                "alerts.ack",
                "alerts.escalate",
                "manual.review",
                "webhooks.manage",
                "calendar.manage",
                "calendar.backfill",
                "feedback.write",
                "events.ingest",
                "admin.state",
                "audit.read",
            },
            "trader": {"alerts.revoke", "alerts.ack", "feedback.write", "webhooks.manage", "events.ingest"},
            "analyst": {"alerts.ack", "feedback.write", "events.ingest"},
        }
        return action in permissions.get(role, set())

    def _audit(self, action: str, actor: str, detail: dict[str, Any] | None = None) -> None:
        record = {
            "audit_id": f"audit_{uuid4().hex[:10]}",
            "action": action,
            "actor": actor,
            "detail": detail or {},
            "created_at": now_utc().isoformat(),
        }
        self.audit_logs.append(record)

    def list_audit_logs(
        self,
        *,
        action: str | None = None,
        actor: str | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        rows = []
        for item in reversed(self.audit_logs):
            if action and str(item.get("action")) != action:
                continue
            if actor and str(item.get("actor")) != actor:
                continue
            rows.append(deepcopy(item))
            if len(rows) >= limit:
                break
        return rows

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
        self._persist_state()
        return deepcopy(current)

    def update_user_alert_subscriptions(self, username: str, payload: dict[str, Any]) -> dict[str, Any]:
        current = self.user_alert_subscriptions.setdefault(username, {"channels": ["app"], "level_min": "P2", "muted": False})
        for key, value in payload.items():
            if value is not None:
                current[key] = value
        self._persist_state()
        return deepcopy(current)

    def update_topic_subscriptions(self, username: str, topic_ids: list[str]) -> list[str]:
        available = {item.get("topic_id") for item in self.topic_taxonomy.get("topics", [])}
        selected = {topic_id for topic_id in topic_ids if topic_id in available}
        self.user_topic_subscriptions[username] = selected
        self._persist_state()
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

    def can_publish_source_externally(self, source_id: str) -> tuple[bool, str]:
        compliance = self.source_compliance(source_id)
        default_policy = compliance.get("default_policy", {})
        detail = compliance.get("compliance", {})
        usage_scope = str(detail.get("usage_scope", ""))
        redistribution = str(detail.get("redistribution", ""))
        if not detail:
            allowed_default = bool(default_policy.get("allowed_for_external_api_redistribution", False))
            if not allowed_default:
                return False, "source has no explicit external redistribution permission"
            return True, "allowed_by_default_policy"
        if usage_scope == "internal_research_only":
            return False, "usage scope is internal_research_only"
        if redistribution in {"restricted"}:
            return False, "redistribution is restricted"
        return True, "allowed"

    def update_alert_policies(self, payload: dict[str, Any]) -> dict[str, Any]:
        for key, value in payload.items():
            if value is not None:
                self.alert_policies[key] = value
        self._audit("alert.policy.update", "system", {"fields": [k for k, v in payload.items() if v is not None]})
        self._persist_state()
        return deepcopy(self.alert_policies)

    def list_alerts(
        self,
        *,
        username: str | None = None,
        status: str | None = None,
        importance_min: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        min_rank = self._alert_level_rank(importance_min or "P2")
        rows = []
        for alert in sorted(self.alerts.values(), key=lambda item: item.get("created_at", ""), reverse=True):
            if status and str(alert.get("status")) != status:
                continue
            if self._alert_level_rank(str(alert.get("importance_level", "P2"))) < min_rank:
                continue
            if username and username not in set(alert.get("target_users", [])) and self.user_role(username) != "admin":
                continue
            rows.append(deepcopy(alert))
            if len(rows) >= limit:
                break
        return rows

    def ack_alert(self, alert_id: str, username: str, note: str = "") -> dict[str, Any] | None:
        alert = self.alerts.get(alert_id)
        if not alert:
            return None
        if username not in set(alert.get("target_users", [])) and self.user_role(username) != "admin":
            return None
        alert["status"] = "acked"
        alert["acked_by"] = username
        alert["acked_at"] = now_utc().isoformat()
        alert["updated_at"] = now_utc().isoformat()
        self.alert_acks.append(
            {
                "alert_id": alert_id,
                "username": username,
                "note": note,
                "acked_at": alert["acked_at"],
            }
        )
        self._audit("alert.ack", username, {"alert_id": alert_id, "note": note})
        self._persist_state()
        return deepcopy(alert)

    def revoke_alert(self, alert_id: str, reason: str) -> dict[str, Any]:
        revoked = {
            "alert_id": alert_id,
            "status": "revoked",
            "reason": reason,
            "revoked_at": now_utc().isoformat(),
            "correction_notice_required": bool(self.alert_governance_rules.get("correction_and_recall", {}).get("correction_notice_required", True)),
        }
        self.revoked_alerts[alert_id] = revoked
        if alert_id in self.alerts:
            self.alerts[alert_id]["status"] = "revoked"
            self.alerts[alert_id]["updated_at"] = revoked["revoked_at"]
            self.alerts[alert_id]["revoke_reason"] = reason
        self._audit("alert.revoke", "system", {"alert_id": alert_id, "reason": reason})
        self._persist_state()
        return revoked

    def _alert_escalation_minutes(self, level: str) -> int:
        esc = self.alert_governance_rules.get("escalation", {})
        defaults = {"P0": 3, "P1": 10, "P2": 30}
        if isinstance(esc.get("p0_unacked_escalate_minutes"), (int, float)):
            defaults["P0"] = int(esc["p0_unacked_escalate_minutes"])
        if isinstance(esc.get("p1_unacked_escalate_minutes"), (int, float)):
            defaults["P1"] = int(esc["p1_unacked_escalate_minutes"])
        if isinstance(esc.get("p2_unacked_escalate_minutes"), (int, float)):
            defaults["P2"] = int(esc["p2_unacked_escalate_minutes"])
        return defaults.get(level, 10)

    def escalate_alerts(self, *, actor: str, limit: int = 100, force: bool = False) -> dict[str, Any]:
        escalated = 0
        skipped = 0
        now = now_utc()
        for alert in sorted(self.alerts.values(), key=lambda item: item.get("created_at", "")):
            if escalated >= limit:
                break
            if alert.get("status") not in {"active"}:
                skipped += 1
                continue
            if alert.get("acked_at") is not None:
                skipped += 1
                continue
            created_at = datetime.fromisoformat(str(alert.get("created_at")))
            age_minutes = (now - created_at).total_seconds() / 60
            threshold = self._alert_escalation_minutes(str(alert.get("importance_level", "P2")))
            if not force and age_minutes < threshold:
                skipped += 1
                continue
            alert["status"] = "escalated"
            alert["escalated_at"] = now.isoformat()
            alert["updated_at"] = now.isoformat()
            alert["escalation_count"] = int(alert.get("escalation_count", 0)) + 1
            alert["escalation_channels"] = self.alert_policies.get("channels_order", ["app", "im", "email", "phone"])
            escalation = {
                "escalation_id": f"esc_{uuid4().hex[:10]}",
                "alert_id": alert["alert_id"],
                "event_id": alert.get("event_id"),
                "importance_level": alert.get("importance_level"),
                "age_minutes": round(age_minutes, 2),
                "threshold_minutes": threshold,
                "channels": alert["escalation_channels"],
                "actor": actor,
                "created_at": now.isoformat(),
            }
            self.alert_escalations.append(escalation)
            escalated += 1

        self._audit("alert.escalate", actor, {"escalated": escalated, "skipped": skipped, "force": force, "limit": limit})
        self._persist_state()
        return {"status": "ok", "escalated": escalated, "skipped": skipped}

    def list_alert_escalations(self, *, limit: int = 100) -> list[dict[str, Any]]:
        return [deepcopy(item) for item in sorted(self.alert_escalations, key=lambda x: x.get("created_at", ""), reverse=True)[:limit]]

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
        self._persist_state()
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
        self._audit("calendar.upsert", "system", {"calendar_event_id": event_id})
        self._persist_state()
        return deepcopy(current)

    def backfill_calendar_actual(
        self,
        *,
        calendar_event_id: str,
        actual: float,
        consensus: float | None,
        note: str,
        actor: str,
    ) -> dict[str, Any] | None:
        item = self.calendar_events.get(calendar_event_id)
        if not item:
            return None
        item["actual"] = float(actual)
        if consensus is not None:
            item["consensus"] = float(consensus)
        item["actual_note"] = note
        item["actual_updated_at"] = now_utc().isoformat()

        surprise = self.calendar_surprise(calendar_event_id)
        generated_event_id = None
        if surprise and surprise.get("status") == "available":
            abs_surprise = abs(float(surprise.get("surprise_ratio", 0)))
            threshold = float(self.event_calendar_rules.get("importance_boost", {}).get("high_surprise_abs_gte", 0.2))
            if abs_surprise >= threshold:
                event = self._build_event(
                    source_id=str(item.get("source", "calendar")),
                    title=f"{item.get('event_name', 'Calendar Event')} actual released",
                    content=(
                        f"consensus={item.get('consensus')} actual={item.get('actual')} "
                        f"surprise={surprise.get('surprise_ratio')}"
                    ),
                    related_instruments=[],
                )
                boost = float(self.event_calendar_rules.get("importance_boost", {}).get("boost_points", 8))
                event.importance_score = min(100.0, round(event.importance_score + boost, 2))
                event.importance_level = level_from_score(event.importance_score)  # type: ignore[assignment]
                event.event_type = "calendar_release_surprise"
                event.evidence = [f"calendar_event_id:{calendar_event_id}"]
                self.events[event.event_id] = event
                self._create_alert_for_event(event)
                generated_event_id = event.event_id

        self._audit(
            "calendar.backfill_actual",
            actor,
            {
                "calendar_event_id": calendar_event_id,
                "actual": actual,
                "consensus": item.get("consensus"),
                "generated_event_id": generated_event_id,
            },
        )
        self._persist_state()
        return {
            "calendar_event": deepcopy(item),
            "surprise": surprise,
            "generated_event_id": generated_event_id,
        }

    def _webhook_retry_policy(self) -> dict[str, Any]:
        retry = self.webhook_delivery_rules.get("retry", {})
        return {
            "base_delay_sec": int(retry.get("base_delay_sec", 2)),
            "backoff_multiplier": float(retry.get("backoff_multiplier", 2.0)),
            "max_delay_sec": int(retry.get("max_delay_sec", 60)),
            "max_retries_default": int(retry.get("max_retries_default", 2)),
            "timeout_sec_default": int(retry.get("timeout_sec_default", 5)),
        }

    def _webhook_rate_limit_default(self) -> int:
        rate_limit = self.webhook_delivery_rules.get("rate_limit", {})
        return int(rate_limit.get("per_subscription_per_minute_default", 30))

    def _webhook_dlq_enabled(self) -> bool:
        dlq = self.webhook_delivery_rules.get("dlq", {})
        return bool(dlq.get("enabled", True))

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

    def _consume_subscription_rate_limit(self, subscription: dict[str, Any]) -> bool:
        limit = int(subscription.get("rate_limit_per_minute", self._webhook_rate_limit_default()))
        now_bucket = now_utc().strftime("%Y-%m-%dT%H:%M")
        if subscription.get("_rl_bucket") != now_bucket:
            subscription["_rl_bucket"] = now_bucket
            subscription["_rl_count"] = 0
        if int(subscription.get("_rl_count", 0)) >= limit:
            return False
        subscription["_rl_count"] = int(subscription.get("_rl_count", 0)) + 1
        return True

    def _mask_webhook_record(self, record: dict[str, Any]) -> dict[str, Any]:
        public_record = deepcopy(record)
        if public_record.get("secret"):
            public_record["secret"] = "***"
        public_record.pop("_rl_bucket", None)
        public_record.pop("_rl_count", None)
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
            "rate_limit_per_minute": int(payload.get("rate_limit_per_minute", self._webhook_rate_limit_default())),
            "created_at": now_utc().isoformat(),
            "last_delivery_at": None,
            "deliveries": 0,
            "failed_deliveries": 0,
            "throttled_count": 0,
            "_rl_bucket": None,
            "_rl_count": 0,
        }
        self.webhook_subscriptions[subscription_id] = record
        self._audit("webhook.subscription.create", username, {"subscription_id": subscription_id, "url": record.get("url")})
        self._persist_state()
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
        self._audit("webhook.subscription.delete", username, {"subscription_id": subscription_id})
        self._persist_state()
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
        self._audit("webhook.dispatch_test", "system", {"queued_subscriptions": queued, "force_fail": force_fail})
        self._persist_state()
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
        self._audit("webhook.retry_failed", "system", {"retried": retried, "requeued": requeued})
        self._persist_state()
        return {
            "status": "ok",
            "retried": retried,
            "requeued": requeued,
        }

    def list_webhook_dlq(self, *, status: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        rows = []
        for item in reversed(self.webhook_dlq):
            if status and str(item.get("status")) != status:
                continue
            rows.append(deepcopy(item))
            if len(rows) >= limit:
                break
        return rows

    def replay_webhook_dlq(self, *, limit: int = 20) -> dict[str, Any]:
        replayed = 0
        for item in self.webhook_dlq:
            if replayed >= limit:
                break
            if item.get("status") != "pending_replay":
                continue
            delivery = next((d for d in self.webhook_deliveries if d.get("delivery_id") == item.get("delivery_id")), None)
            if not delivery:
                item["status"] = "discarded"
                continue
            subscription = self.webhook_subscriptions.get(str(item.get("subscription_id")))
            if not subscription or not subscription.get("enabled", True):
                continue
            item["status"] = "replayed"
            item["replayed_at"] = now_utc().isoformat()
            delivery["status"] = "retrying"
            delivery["can_retry"] = True
            delivery["next_retry_at"] = now_utc().isoformat()
            self.webhook_queue.append(
                {
                    "job_id": f"wq_{uuid4().hex[:10]}",
                    "delivery_id": delivery["delivery_id"],
                    "subscription_id": delivery["subscription_id"],
                    "event_id": item.get("event_id"),
                    "status": "queued",
                    "scheduled_at": now_utc().isoformat(),
                    "force_fail_once": False,
                }
            )
            replayed += 1
        self._audit("webhook.dlq.replay", "system", {"replayed": replayed, "limit": limit})
        self._persist_state()
        return {"status": "ok", "replayed": replayed, "queue_size": len(self.webhook_queue)}

    def process_webhook_queue(self, *, limit: int = 50, ignore_schedule: bool = False) -> dict[str, Any]:
        processed = 0
        delivered = 0
        failed = 0
        requeued = 0
        throttled = 0
        dlq_moved = 0
        now = now_utc()
        remaining_jobs: list[dict[str, Any]] = []
        for job in self.webhook_queue:
            if processed >= limit:
                remaining_jobs.append(job)
                continue
            scheduled_at = datetime.fromisoformat(str(job.get("scheduled_at")))
            if scheduled_at > now and not ignore_schedule:
                remaining_jobs.append(job)
                continue

            delivery = next((item for item in self.webhook_deliveries if item.get("delivery_id") == job.get("delivery_id")), None)
            subscription = self.webhook_subscriptions.get(str(job.get("subscription_id")))
            if not delivery or not subscription or not subscription.get("enabled", True):
                continue

            processed += 1
            # Per-subscription soft rate limit.
            if not self._consume_subscription_rate_limit(subscription):
                throttled += 1
                subscription["throttled_count"] = int(subscription.get("throttled_count", 0)) + 1
                delivery["status"] = "throttled"
                delivery["next_retry_at"] = (now_utc() + timedelta(seconds=1)).isoformat()
                remaining_jobs.append(
                    {
                        "job_id": f"wq_{uuid4().hex[:10]}",
                        "delivery_id": delivery["delivery_id"],
                        "subscription_id": delivery["subscription_id"],
                        "event_id": job.get("event_id"),
                        "status": "queued",
                        "scheduled_at": delivery["next_retry_at"],
                        "force_fail_once": False,
                    }
                )
                continue

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
                if self._webhook_dlq_enabled():
                    delivery["status"] = "dlq"
                    dlq_record = {
                        "dlq_id": f"dlq_{uuid4().hex[:10]}",
                        "delivery_id": delivery["delivery_id"],
                        "subscription_id": delivery["subscription_id"],
                        "event_id": job.get("event_id"),
                        "reason": reason,
                        "retry_count": retry_count,
                        "max_retries": max_retries,
                        "moved_at": now_utc().isoformat(),
                        "status": "pending_replay",
                    }
                    self.webhook_dlq.append(dlq_record)
                    dlq_moved += 1
                else:
                    delivery["status"] = "failed"
                delivery["next_retry_at"] = None
                failed += 1

        self.webhook_queue = remaining_jobs
        self._audit(
            "webhook.queue.process",
            "system",
            {"processed": processed, "delivered": delivered, "failed": failed, "requeued": requeued, "throttled": throttled, "dlq_moved": dlq_moved},
        )
        self._persist_state()
        return {
            "status": "ok",
            "processed": processed,
            "delivered": delivered,
            "failed": failed,
            "requeued": requeued,
            "throttled": throttled,
            "dlq_moved": dlq_moved,
            "queue_size": len(self.webhook_queue),
        }

    def webhook_stats(self) -> dict[str, Any]:
        deliveries = len(self.webhook_deliveries)
        delivered = sum(1 for item in self.webhook_deliveries if item.get("status") == "delivered")
        failed = sum(1 for item in self.webhook_deliveries if item.get("status") == "failed")
        queued = sum(1 for item in self.webhook_deliveries if item.get("status") == "queued")
        retrying = sum(1 for item in self.webhook_deliveries if item.get("status") == "retrying")
        throttled = sum(1 for item in self.webhook_deliveries if item.get("status") == "throttled")
        dlq = sum(1 for item in self.webhook_deliveries if item.get("status") == "dlq")
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
            "throttled_total": throttled,
            "dlq_total": dlq,
            "queue_size": len(self.webhook_queue),
            "dlq_size": len(self.webhook_dlq),
            "success_rate_pct": success_rate,
            "retry_count_p50": retry_p50,
            "retry_count_p95": retry_p95,
            "retry_policy": self._webhook_retry_policy(),
        }

    def webhook_subscription_stats(self, *, top_n: int = 10) -> list[dict[str, Any]]:
        per_sub: dict[str, dict[str, Any]] = {}
        for sub_id, sub in self.webhook_subscriptions.items():
            per_sub[sub_id] = {
                "subscription_id": sub_id,
                "name": sub.get("name"),
                "owner": sub.get("owner"),
                "url": sub.get("url"),
                "deliveries": 0,
                "delivered": 0,
                "failed": 0,
                "retrying": 0,
                "throttled": 0,
                "dlq": 0,
                "max_retries": sub.get("max_retries"),
                "rate_limit_per_minute": sub.get("rate_limit_per_minute"),
            }
        for item in self.webhook_deliveries:
            sub_id = item.get("subscription_id")
            if sub_id not in per_sub:
                per_sub[sub_id] = {
                    "subscription_id": sub_id,
                    "name": None,
                    "owner": None,
                    "url": None,
                    "deliveries": 0,
                    "delivered": 0,
                    "failed": 0,
                    "retrying": 0,
                    "throttled": 0,
                    "dlq": 0,
                    "max_retries": None,
                    "rate_limit_per_minute": None,
                }
            row = per_sub[sub_id]
            row["deliveries"] += 1
            status = str(item.get("status"))
            if status in {"delivered", "failed", "retrying", "throttled", "dlq"}:
                row[status] += 1
        rows = list(per_sub.values())
        for row in rows:
            total = int(row["deliveries"])
            row["success_rate_pct"] = round((row["delivered"] / total) * 100, 2) if total else 0.0
        rows.sort(key=lambda x: (x["failed"] + x["dlq"], -x["success_rate_pct"]), reverse=True)
        return rows[:top_n]

    def metrics_summary(self) -> dict[str, Any]:
        webhook = self.webhook_stats()
        events = list(self.events.values())
        avg_importance = round(sum(item.importance_score for item in events) / len(events), 2) if events else 0.0
        p0_count = sum(1 for item in events if item.importance_level == "P0")
        p1_count = sum(1 for item in events if item.importance_level == "P1")
        p2_count = sum(1 for item in events if item.importance_level == "P2")
        active_alerts = sum(1 for item in self.alerts.values() if item.get("status") == "active")
        acked_alerts = sum(1 for item in self.alerts.values() if item.get("status") == "acked")
        return {
            "events_total": len(events),
            "events_p0": p0_count,
            "events_p1": p1_count,
            "events_p2": p2_count,
            "events_avg_importance": avg_importance,
            "alerts_total": len(self.alerts),
            "alerts_active": active_alerts,
            "alerts_acked": acked_alerts,
            "users_total": len(self.users),
            "feedback_total": len(self.feedback_records),
            "audit_total": len(self.audit_logs),
            "ingest_stats": self.ingest_stats,
            "webhook": webhook,
        }

    def export_state_snapshot(self) -> dict[str, Any]:
        return {
            "events": [event.model_dump(mode="json") for event in self.events.values()],
            "manual_messages": [item.model_dump(mode="json") for item in self.manual_messages.values()],
            "alerts": list(self.alerts.values()),
            "alert_acks": self.alert_acks,
            "user_preferences": self.user_preferences,
            "user_alert_subscriptions": self.user_alert_subscriptions,
            "user_topic_subscriptions": {k: sorted(v) for k, v in self.user_topic_subscriptions.items()},
            "feedback_records": self.feedback_records,
            "ingest_stats": self.ingest_stats,
            "webhook_subscriptions": list(self.webhook_subscriptions.values()),
            "webhook_deliveries": self.webhook_deliveries,
            "webhook_queue": self.webhook_queue,
            "webhook_dlq": self.webhook_dlq,
            "calendar_events": list(self.calendar_events.values()),
        }

    def import_state_snapshot(self, payload: dict[str, Any], *, merge: bool = False) -> dict[str, Any]:
        if not merge:
            self.events = {}
            self.manual_messages = {}
            self.alerts = {}
            self.alert_acks = []
            self.user_preferences = {}
            self.user_alert_subscriptions = {}
            self.user_topic_subscriptions = {}
            self.feedback_records = []
            self.ingest_stats = {"total": 0, "deduplicated": 0, "accepted": 0}
            self.webhook_subscriptions = {}
            self.webhook_deliveries = []
            self.webhook_queue = []
            self.webhook_dlq = []
            self.calendar_events = {}

        imported_events = 0
        for item in payload.get("events", []):
            try:
                event = Event(**item)
                self.events[event.event_id] = event
                imported_events += 1
            except Exception:
                continue

        imported_manual = 0
        for item in payload.get("manual_messages", []):
            try:
                record = ManualMessageRecord(**item)
                self.manual_messages[record.manual_message_id] = record
                imported_manual += 1
            except Exception:
                continue

        for item in payload.get("alerts", []):
            if "alert_id" in item:
                self.alerts[item["alert_id"]] = item
        self.alert_acks.extend(payload.get("alert_acks", []))
        self.feedback_records.extend(payload.get("feedback_records", []))

        for key, value in dict(payload.get("user_preferences", {})).items():
            self.user_preferences[key] = value
        for key, value in dict(payload.get("user_alert_subscriptions", {})).items():
            self.user_alert_subscriptions[key] = value
        for key, value in dict(payload.get("user_topic_subscriptions", {})).items():
            self.user_topic_subscriptions[key] = set(value)

        for item in payload.get("webhook_subscriptions", []):
            if "subscription_id" in item:
                self.webhook_subscriptions[item["subscription_id"]] = item
        self.webhook_deliveries.extend(payload.get("webhook_deliveries", []))
        self.webhook_queue.extend(payload.get("webhook_queue", []))
        self.webhook_dlq.extend(payload.get("webhook_dlq", []))
        for item in payload.get("calendar_events", []):
            if "calendar_event_id" in item:
                self.calendar_events[item["calendar_event_id"]] = item

        incoming_stats = dict(payload.get("ingest_stats", {}))
        for field in ("total", "deduplicated", "accepted"):
            self.ingest_stats[field] = int(incoming_stats.get(field, self.ingest_stats.get(field, 0)))

        self._audit(
            "admin.state.import",
            "system",
            {"merge": merge, "imported_events": imported_events, "imported_manual_messages": imported_manual},
        )
        self._persist_state()
        return {
            "status": "ok",
            "merge": merge,
            "imported_events": imported_events,
            "imported_manual_messages": imported_manual,
            "alerts_total": len(self.alerts),
            "webhook_subscriptions_total": len(self.webhook_subscriptions),
        }

    def reset_runtime_state(self, *, reseed: bool = True) -> dict[str, Any]:
        self.events = {}
        self.manual_messages = {}
        self.alerts = {}
        self.alert_acks = []
        self.user_preferences = {}
        self.user_alert_subscriptions = {}
        self.user_topic_subscriptions = {}
        self.feedback_records = []
        self.ingest_stats = {"total": 0, "deduplicated": 0, "accepted": 0}
        self.webhook_subscriptions = {}
        self.webhook_deliveries = []
        self.webhook_queue = []
        self.webhook_dlq = []
        self.calendar_events = {}
        if reseed:
            self._seed_events()
            self._seed_calendar_events()
        self._audit("admin.state.reset", "system", {"reseed": reseed})
        self._persist_state()
        return {
            "status": "ok",
            "reseed": reseed,
            "events_total": len(self.events),
            "alerts_total": len(self.alerts),
            "calendar_events_total": len(self.calendar_events),
        }
