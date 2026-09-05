from __future__ import annotations

import math
from typing import Any


DEFAULT_WEIGHTS = {
    "group_sentiment_weight": 1.0,
    "pe_weight": 1.0,
    "pb_weight": 1.0,
    "peg_weight": 1.0,
    "dividend_weight": 1.0,
    "momentum_weight": 1.0,
}


def _num(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        number = float(value)
        return number if math.isfinite(number) else None
    except (TypeError, ValueError):
        return None


def _factor(value: float, reason: str | None) -> tuple[float, str | None]:
    return value, reason


def personalized_recommendation(
    group: dict[str, Any],
    stock: dict[str, Any],
    weights: dict[str, Any] | None = None,
) -> dict[str, Any]:
    applied_weights = {}
    for key, default in DEFAULT_WEIGHTS.items():
        value = _num((weights or {}).get(key))
        applied_weights[key] = default if value is None else value
    fundamentals = stock.get("fundamentals") or {}
    raw_score = _num(group.get("raw_score")) or 0.0
    weekly_return = _num(stock.get("weekly_return")) or 0.0
    pb = _num(fundamentals.get("priceToBook"))
    pe = _num(fundamentals.get("trailingPE") or fundamentals.get("forwardPE"))
    peg = _num(fundamentals.get("pegRatio"))
    dividend_yield = _num(fundamentals.get("dividendYield"))

    direction = group.get("direction") or "Neutral"
    strength = _num(group.get("strength")) or 0
    group_kind = "region" if group.get("region") else "sector"
    sentiment_reason = f"{direction.lower()} {group_kind} sentiment with strength {strength:.0f}"
    factors: dict[str, tuple[float, str | None]] = {
        "group_sentiment_weight": _factor(raw_score * 1.5, sentiment_reason),
        "pe_weight": _factor(0.0, None),
        "pb_weight": _factor(0.0, None),
        "peg_weight": _factor(0.0, None),
        "dividend_weight": _factor(0.0, None),
        "momentum_weight": _factor(0.0, None),
    }

    if pe is not None:
        if pe <= 0:
            factors["pe_weight"] = _factor(-0.4, "negative P/E limits earnings visibility")
        elif pe < 15:
            factors["pe_weight"] = _factor(0.6, "low P/E supports valuation")
        elif pe <= 35:
            factors["pe_weight"] = _factor(0.2, "moderate P/E")
        elif pe > 50:
            factors["pe_weight"] = _factor(-0.7, "high P/E adds valuation risk")
        else:
            factors["pe_weight"] = _factor(-0.2, "somewhat elevated P/E")

    if pb is not None:
        if 0 < pb < 2:
            factors["pb_weight"] = _factor(0.4, "lower P/B")
        elif pb > 12:
            factors["pb_weight"] = _factor(-0.5, "high P/B")
        elif pb > 0:
            factors["pb_weight"] = _factor(0.1, "reasonable P/B range")

    if peg is not None:
        if peg <= 0:
            factors["peg_weight"] = _factor(-0.3, "negative PEG limits growth visibility")
        elif peg < 1:
            factors["peg_weight"] = _factor(0.7, "PEG below 1")
        elif peg <= 2:
            factors["peg_weight"] = _factor(0.2, "balanced PEG")
        else:
            factors["peg_weight"] = _factor(-0.5, "high PEG")

    if dividend_yield is not None:
        if dividend_yield >= 4:
            factors["dividend_weight"] = _factor(0.35, "high dividend yield")
        elif dividend_yield >= 2:
            factors["dividend_weight"] = _factor(0.2, "meaningful dividend yield")

    if weekly_return > 0.03:
        factors["momentum_weight"] = _factor(0.25, "positive weekly momentum")
    elif weekly_return < -0.05:
        factors["momentum_weight"] = _factor(-0.35, "weak weekly momentum")

    weighted_values = {
        key: value * float(applied_weights[key])
        for key, (value, _) in factors.items()
    }
    score = sum(weighted_values.values())
    action = "Buy" if score >= 0.8 else "Sell" if score <= -0.8 else "Hold"
    confidence = min(100, max(0, round(abs(score) * 45 + 35)))
    reasons = []
    for key, (_, reason) in factors.items():
        if not reason:
            continue
        weight = float(applied_weights[key])
        reasons.append(f"{reason} ({weight:.1f}x)" if weight != 1 else reason)

    return {
        "action": action,
        "score": round(score, 3),
        "confidence": confidence,
        "rationale": "; ".join(reasons[:5]),
        "factor_contributions": {
            key: round(value, 4) for key, value in weighted_values.items()
        },
    }
