from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


Direction = Literal["long", "short", "neutral"]
ImportanceLevel = Literal["P0", "P1", "P2"]


class ImpactItem(BaseModel):
    asset_class: str
    instrument: str
    direction: Direction
    confidence: float = Field(ge=0.0, le=1.0)
    impact_score: float = Field(ge=0.0, le=100.0)
    long_score: float = Field(ge=0.0, le=100.0)
    short_score: float = Field(ge=0.0, le=100.0)
    net_bias_score: float = Field(ge=-100.0, le=100.0)
    horizon: str = "intra-day"
    explanation: str = ""


class Event(BaseModel):
    event_id: str
    source_id: str
    title: str
    content: str
    language: str = "zh"
    published_at: datetime
    detected_at: datetime
    event_type: str
    importance_level: ImportanceLevel
    importance_score: float = Field(ge=0.0, le=100.0)
    impacted_markets: list[str]
    impacts: list[ImpactItem]
    credibility_level: Literal["official", "verified", "rumor"] = "verified"
    evidence: list[str] = Field(default_factory=list)


class ManualMessageCreateRequest(BaseModel):
    title: str
    content: str
    event_time: datetime | None = None
    source_hint: str | None = None
    related_instruments: list[str] = Field(default_factory=list)
    operator_id: str
    operator_role: str
    attachments: list[str] = Field(default_factory=list)


class ManualMessageReviewRequest(BaseModel):
    action: Literal["approve", "reject", "revoke"]
    review_comment: str = ""


class ManualMessageRecord(BaseModel):
    manual_message_id: str
    status: Literal["submitted", "auto_assessed", "approved", "rejected", "revoked"]
    request: ManualMessageCreateRequest
    importance_level: ImportanceLevel
    importance_score: float
    impacted_markets: list[str]
    top_impacted_instruments: list[str]
    long_score: float
    short_score: float
    net_bias_score: float
    assessment_explanation: list[str] = Field(default_factory=list)
    created_at: datetime
    updated_at: datetime
    linked_event_id: str | None = None


class SourcesBatchOperation(BaseModel):
    op: Literal["upsert", "disable"]
    source_id: str
    enabled: bool | None = None
    source_weight: float | None = None
    credibility_weight: float | None = None
    timeliness_weight: float | None = None
    coverage_weight: float | None = None
    noise_penalty: float | None = None
    poll_interval_sec: int | None = None
    category: str | None = None
    region: str | None = None
    tier: int | None = None
    url: str | None = None


class SourcesBatchRequest(BaseModel):
    operations: list[SourcesBatchOperation]


class SourcePatchRequest(BaseModel):
    enabled: bool | None = None
    source_weight: float | None = None
    credibility_weight: float | None = None
    timeliness_weight: float | None = None
    coverage_weight: float | None = None
    noise_penalty: float | None = None
    poll_interval_sec: int | None = None


class ImpactBatchRequest(BaseModel):
    request_id: str | None = None
    window: str = "4h"
    instruments: list[str]
    event_ids: list[str] = Field(default_factory=list)


class ImpactBatchItem(BaseModel):
    instrument: str
    events_count: int
    long_score: float
    short_score: float
    net_bias_score: float
    dominant_direction: Literal["LONG", "SHORT", "NEUTRAL"]


class ImpactBatchResponse(BaseModel):
    window: str
    request_id: str | None = None
    results: list[ImpactBatchItem]


class SentimentResponse(BaseModel):
    symbol: str
    sentiment_score: float
    confidence: float
    window: str
    sample_events: int


class SignalResponse(BaseModel):
    symbol: str
    interval: str
    signal: Literal["BUY", "SELL", "HOLD"]
    confidence: float
    reason: str


class EventFeedResponse(BaseModel):
    page: int
    page_size: int
    total: int
    events: list[dict[str, Any]]
