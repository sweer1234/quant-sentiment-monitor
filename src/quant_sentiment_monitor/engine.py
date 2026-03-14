from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .models import ImpactItem


MARKET_TO_INSTRUMENTS: dict[str, list[str]] = {
    "fx": ["EURUSD", "USDJPY", "DXY", "USDCAD"],
    "global_equity": ["SPX", "NDX", "HSI"],
    "stock": ["AAPL", "TSLA", "XOM"],
    "futures": ["CL", "GC", "NQ"],
    "bond": ["UST10Y", "UST2Y"],
    "metals": ["XAUUSD", "XAGUSD"],
    "derivatives": ["SPX_0DTE_CALL", "VIX_FUT"],
    "crypto": ["BTCUSDT", "ETHUSDT"],
}

INSTRUMENT_TO_MARKET: dict[str, str] = {
    instrument: market for market, instruments in MARKET_TO_INSTRUMENTS.items() for instrument in instruments
}


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def level_from_score(score: float) -> str:
    if score >= 85:
        return "P0"
    if score >= 70:
        return "P1"
    return "P2"


def clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def calculate_effective_source_weight(source: dict[str, Any]) -> float:
    value = (
        0.35 * float(source.get("source_weight", 0.7))
        + 0.30 * float(source.get("credibility_weight", 0.75))
        + 0.20 * float(source.get("timeliness_weight", 0.7))
        + 0.15 * float(source.get("coverage_weight", 0.65))
        - 0.20 * float(source.get("noise_penalty", 0.1))
    )
    tier_mult = 1.1 if str(source.get("tier", "1")) == "0" else 1.0
    category = str(source.get("category", "media"))
    category_mult = {
        "central_bank": 1.10,
        "regulator": 1.08,
        "statistics": 1.05,
        "exchange": 1.03,
        "policy": 1.00,
        "fixed_income": 1.00,
        "media": 0.95,
        "manual": 0.92,
    }.get(category, 1.0)
    return clamp(value * tier_mult * category_mult, 0.0, 1.0)


def infer_markets_and_impacts(title: str, content: str, related_instruments: list[str] | None = None) -> tuple[list[str], list[ImpactItem]]:
    text = f"{title} {content}".lower()
    related_instruments = related_instruments or []
    markets: set[str] = set()
    impacts: list[ImpactItem] = []

    # Rule-based minimal NLP for MVP.
    if any(k in text for k in ["加息", "hawkish", "鹰派", "利率上调"]):
        markets.update(["fx", "bond", "metals"])
        impacts.append(
            ImpactItem(
                asset_class="fx",
                instrument="DXY",
                direction="long",
                confidence=0.78,
                impact_score=81,
                long_score=76,
                short_score=24,
                net_bias_score=52,
                explanation="鹰派政策通常利多美元",
            )
        )
        impacts.append(
            ImpactItem(
                asset_class="bond",
                instrument="UST10Y",
                direction="short",
                confidence=0.72,
                impact_score=74,
                long_score=28,
                short_score=72,
                net_bias_score=-44,
                explanation="加息预期通常压制长久期债券",
            )
        )
    if any(k in text for k in ["减产", "supply shock", "地缘冲突", "中断"]):
        markets.update(["futures", "fx", "stock"])
        impacts.append(
            ImpactItem(
                asset_class="futures",
                instrument="CL",
                direction="long",
                confidence=0.8,
                impact_score=83,
                long_score=79,
                short_score=21,
                net_bias_score=58,
                explanation="供给冲击通常利多原油",
            )
        )
        impacts.append(
            ImpactItem(
                asset_class="fx",
                instrument="USDCAD",
                direction="short",
                confidence=0.66,
                impact_score=70,
                long_score=31,
                short_score=69,
                net_bias_score=-38,
                explanation="油价上行通常支撑加元",
            )
        )
    if any(k in text for k in ["黑客", "hack", "漏洞", "监管趋严"]):
        markets.update(["crypto"])
        impacts.append(
            ImpactItem(
                asset_class="crypto",
                instrument="BTCUSDT",
                direction="short",
                confidence=0.71,
                impact_score=76,
                long_score=29,
                short_score=71,
                net_bias_score=-42,
                explanation="安全或监管风险通常压制加密资产",
            )
        )
    if any(k in text for k in ["降息", "dovish", "宽松", "qe"]):
        markets.update(["global_equity", "metals", "crypto"])
        impacts.append(
            ImpactItem(
                asset_class="global_equity",
                instrument="SPX",
                direction="long",
                confidence=0.7,
                impact_score=75,
                long_score=73,
                short_score=27,
                net_bias_score=46,
                explanation="流动性宽松通常利多风险资产",
            )
        )
        impacts.append(
            ImpactItem(
                asset_class="metals",
                instrument="XAUUSD",
                direction="long",
                confidence=0.67,
                impact_score=71,
                long_score=68,
                short_score=32,
                net_bias_score=36,
                explanation="宽松周期通常支撑黄金",
            )
        )

    for instrument in related_instruments:
        normalized = instrument.upper()
        market = INSTRUMENT_TO_MARKET.get(normalized)
        if market:
            markets.add(market)
        impacts.append(
            ImpactItem(
                asset_class=market or "custom",
                instrument=normalized,
                direction="neutral",
                confidence=0.55,
                impact_score=60,
                long_score=50,
                short_score=50,
                net_bias_score=0,
                explanation="来自人工关联标的，待行情确认",
            )
        )

    if not impacts:
        markets.update(["global_equity"])
        impacts.append(
            ImpactItem(
                asset_class="global_equity",
                instrument="SPX",
                direction="neutral",
                confidence=0.5,
                impact_score=50,
                long_score=50,
                short_score=50,
                net_bias_score=0,
                explanation="缺少显著信号，保持中性",
            )
        )

    return sorted(markets), impacts


def aggregate_signal(long_score: float, short_score: float) -> tuple[str, float]:
    net = long_score - short_score
    if net > 12:
        return "BUY", clamp(abs(net) / 100 + 0.55, 0.0, 0.99)
    if net < -12:
        return "SELL", clamp(abs(net) / 100 + 0.55, 0.0, 0.99)
    return "HOLD", 0.55
