from __future__ import annotations

import csv
import json
import math
import re
from datetime import datetime, timezone
from pathlib import Path
from shutil import copy2
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
APP_HTML = ROOT / "stare_app.html"
REPORT_JSON = ROOT / "reports" / "sector_dashboard.json"
REPORT_CSV = ROOT / "reports" / "sector_dashboard_top10.csv"
FUNDAMENTALS_CSV = ROOT / "reports" / "fundamentals_sp500_latest.csv"
LOGO = ROOT / "Logo.png"
DOCS = ROOT / "docs"

DATA_RE = re.compile(
    r'(<script id="stare-data" type="application/json">)(.*?)(</script>)',
    re.DOTALL,
)


def _num(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        n = float(value)
        return n if math.isfinite(n) else None
    except (TypeError, ValueError):
        return None


def _text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and not math.isfinite(value):
        return None
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null"}:
        return None
    return text


def _json_safe(value: Any) -> Any:
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    return value


def _fmt_num(value: Any, digits: int = 2) -> str:
    n = _num(value)
    return "n/a" if n is None else f"{n:.{digits}f}"


def _fmt_pct(value: Any) -> str:
    n = _num(value)
    if n is None:
        return "n/a"
    return f"{n:.2f}%" if abs(n) > 1 else f"{n * 100:.2f}%"


def _fmt_yield(value: Any) -> str:
    n = _num(value)
    return "n/a" if n is None else f"{n:.2f}%"


def _valuation_note(pb: float | None, pe: float | None, peg: float | None) -> str:
    if pe is None and pb is None and peg is None:
        return "valuation data is limited"
    notes = []
    if pe is not None:
        if pe < 15:
            notes.append("low P/E")
        elif pe > 40:
            notes.append("elevated P/E")
        else:
            notes.append("moderate P/E")
    if pb is not None:
        if pb < 2:
            notes.append("lower P/B")
        elif pb > 8:
            notes.append("high P/B")
        else:
            notes.append("mid-range P/B")
    if peg is not None:
        if peg < 1:
            notes.append("PEG below 1")
        elif peg > 2:
            notes.append("higher PEG")
        else:
            notes.append("balanced PEG")
    return ", ".join(notes)


def _yield_note(dividend_yield: float | None) -> str:
    if dividend_yield is None:
        return "no dividend yield reported"
    if dividend_yield == 0:
        return "no dividend yield"
    if dividend_yield >= 4:
        return "high dividend yield"
    if dividend_yield >= 2:
        return "meaningful dividend yield"
    return "modest dividend yield"


def _label(value: str, detail: str) -> dict[str, str]:
    return {"label": value, "detail": detail}


def _decision_snapshot_for_stock(stock: dict[str, Any]) -> dict[str, Any]:
    fundamentals = stock.get("fundamentals") or {}
    recommendation = stock.get("recommendation") or {}
    pe = _num(fundamentals.get("trailingPE") or fundamentals.get("forwardPE"))
    pb = _num(fundamentals.get("priceToBook"))
    peg = _num(fundamentals.get("pegRatio"))
    dividend_yield = _num(fundamentals.get("dividendYield"))
    payout_ratio = _num(fundamentals.get("payoutRatio"))
    roe = _num(fundamentals.get("returnOnEquity"))
    profit_margin = _num(fundamentals.get("profitMargins"))
    revenue_growth = _num(fundamentals.get("revenueGrowth"))
    earnings_growth = _num(fundamentals.get("earningsGrowth"))
    beta = _num(fundamentals.get("beta"))
    debt_to_equity = _num(fundamentals.get("debtToEquity"))
    current_ratio = _num(fundamentals.get("currentRatio"))
    weekly_return = _num(stock.get("weekly_return"))

    valuation_points = 0
    valuation_notes = []
    if pe is not None:
        if pe <= 0:
            valuation_points -= 2
            valuation_notes.append("negative or unavailable earnings")
        elif pe < 15:
            valuation_points += 2
            valuation_notes.append("low P/E")
        elif pe <= 35:
            valuation_points += 1
            valuation_notes.append("moderate P/E")
        elif pe > 50:
            valuation_points -= 2
            valuation_notes.append("very high P/E")
        else:
            valuation_points -= 1
            valuation_notes.append("elevated P/E")
    if pb is not None:
        if 0 < pb < 2:
            valuation_points += 1
            valuation_notes.append("low P/B")
        elif pb > 12:
            valuation_points -= 1
            valuation_notes.append("high P/B")
    if peg is not None:
        if 0 < peg < 1:
            valuation_points += 2
            valuation_notes.append("PEG below 1")
        elif peg > 2:
            valuation_points -= 1
            valuation_notes.append("high PEG")

    if valuation_points >= 2:
        valuation = _label("Attractive", "; ".join(valuation_notes[:3]) or "valuation ratios look supportive")
    elif valuation_points <= -2:
        valuation = _label("Expensive", "; ".join(valuation_notes[:3]) or "valuation ratios look stretched")
    else:
        valuation = _label("Fair", "; ".join(valuation_notes[:3]) or "valuation inputs are mixed or limited")

    quality_points = 0
    quality_notes = []
    if roe is not None:
        if roe >= 0.18:
            quality_points += 2
            quality_notes.append("strong ROE")
        elif roe <= 0:
            quality_points -= 2
            quality_notes.append("weak ROE")
    if profit_margin is not None:
        if profit_margin >= 0.15:
            quality_points += 1
            quality_notes.append("solid profit margin")
        elif profit_margin < 0:
            quality_points -= 2
            quality_notes.append("negative profit margin")
    if revenue_growth is not None:
        if revenue_growth >= 0.08:
            quality_points += 1
            quality_notes.append("revenue growing")
        elif revenue_growth < -0.05:
            quality_points -= 1
            quality_notes.append("revenue shrinking")
    if earnings_growth is not None:
        if earnings_growth >= 0.08:
            quality_points += 1
            quality_notes.append("earnings growing")
        elif earnings_growth < -0.05:
            quality_points -= 1
            quality_notes.append("earnings shrinking")

    if quality_points >= 3:
        quality = _label("Strong", "; ".join(quality_notes[:3]) or "quality metrics look strong")
    elif quality_points <= -2:
        quality = _label("Weak", "; ".join(quality_notes[:3]) or "quality metrics look weak")
    else:
        quality = _label("Mixed", "; ".join(quality_notes[:3]) or "quality metrics are mixed or limited")

    risk_points = 0
    risk_notes = []
    if beta is not None:
        if beta >= 1.5:
            risk_points += 2
            risk_notes.append("high beta")
        elif beta <= 0.8:
            risk_points -= 1
            risk_notes.append("lower beta")
    if debt_to_equity is not None:
        if debt_to_equity > 150:
            risk_points += 2
            risk_notes.append("high debt-to-equity")
        elif 0 <= debt_to_equity < 50:
            risk_points -= 1
            risk_notes.append("lower debt-to-equity")
    if current_ratio is not None:
        if current_ratio < 1:
            risk_points += 1
            risk_notes.append("current ratio below 1")
        elif current_ratio >= 1.5:
            risk_points -= 1
            risk_notes.append("healthy current ratio")

    if risk_points >= 3:
        risk = _label("High", "; ".join(risk_notes[:3]) or "risk inputs are elevated")
    elif risk_points <= -2:
        risk = _label("Low", "; ".join(risk_notes[:3]) or "risk inputs look contained")
    else:
        risk = _label("Medium", "; ".join(risk_notes[:3]) or "risk inputs are mixed or limited")

    if weekly_return is None:
        momentum = _label("Neutral", "weekly momentum is unavailable")
    elif weekly_return >= 0.03:
        momentum = _label("Positive", f"weekly return {_fmt_pct(weekly_return)}")
    elif weekly_return <= -0.05:
        momentum = _label("Negative", f"weekly return {_fmt_pct(weekly_return)}")
    else:
        momentum = _label("Neutral", f"weekly return {_fmt_pct(weekly_return)}")

    income_notes = []
    if dividend_yield is not None:
        income_notes.append(f"dividend yield {_fmt_yield(dividend_yield)}")
    if payout_ratio is not None:
        income_notes.append(f"payout ratio {_fmt_pct(payout_ratio)}")
    if dividend_yield is None or dividend_yield == 0:
        income = _label("None", "no dividend yield reported")
    elif payout_ratio is not None and payout_ratio > 0.85:
        income = _label("Watch", "; ".join(income_notes[:2]))
    elif dividend_yield >= 2:
        income = _label("Supportive", "; ".join(income_notes[:2]))
    else:
        income = _label("Modest", "; ".join(income_notes[:2]))

    return {
        "valuation": valuation,
        "quality": quality,
        "risk": risk,
        "momentum": momentum,
        "income": income,
        "summary": (
            f"Valuation {valuation['label']}; Quality {quality['label']}; "
            f"Risk {risk['label']}; Momentum {momentum['label']}; Income {income['label']}. "
            f"Overall model signal: {recommendation.get('action', 'Hold')} "
            f"({recommendation.get('confidence', 'n/a')} confidence)."
        ),
    }


def _market_sentiment_note(sector: dict[str, Any]) -> str:
    direction = sector.get("direction") or "Neutral"
    strength = _num(sector.get("strength")) or 0
    return f"{direction.lower()} sector sentiment with strength {strength:.0f}"


def _recommendation_for_stock(sector: dict[str, Any], stock: dict[str, Any]) -> dict[str, Any]:
    fundamentals = stock.get("fundamentals") or {}
    raw_score = _num(sector.get("raw_score")) or 0.0
    weekly_return = _num(stock.get("weekly_return")) or 0.0
    pb = _num(fundamentals.get("priceToBook"))
    pe = _num(fundamentals.get("trailingPE") or fundamentals.get("forwardPE"))
    peg = _num(fundamentals.get("pegRatio"))
    dividend_yield = _num(fundamentals.get("dividendYield"))

    score = raw_score * 1.5
    reasons = [_market_sentiment_note(sector)]

    if pe is not None:
        if pe <= 0:
            score -= 0.4
            reasons.append("negative P/E limits earnings visibility")
        elif pe < 15:
            score += 0.6
            reasons.append("low P/E supports valuation")
        elif pe <= 35:
            score += 0.2
            reasons.append("moderate P/E")
        elif pe > 50:
            score -= 0.7
            reasons.append("high P/E adds valuation risk")
        else:
            score -= 0.2
            reasons.append("somewhat elevated P/E")

    if pb is not None:
        if 0 < pb < 2:
            score += 0.4
            reasons.append("lower P/B")
        elif pb > 12:
            score -= 0.5
            reasons.append("high P/B")
        elif pb > 0:
            score += 0.1
            reasons.append("reasonable P/B range")

    if peg is not None:
        if peg <= 0:
            score -= 0.3
            reasons.append("negative PEG limits growth visibility")
        elif peg < 1:
            score += 0.7
            reasons.append("PEG below 1")
        elif peg <= 2:
            score += 0.2
            reasons.append("balanced PEG")
        else:
            score -= 0.5
            reasons.append("high PEG")

    if dividend_yield is not None:
        if dividend_yield >= 4:
            score += 0.35
            reasons.append("high dividend yield")
        elif dividend_yield >= 2:
            score += 0.2
            reasons.append("meaningful dividend yield")

    if weekly_return > 0.03:
        score += 0.25
        reasons.append("positive weekly momentum")
    elif weekly_return < -0.05:
        score -= 0.35
        reasons.append("weak weekly momentum")

    if score >= 0.8:
        action = "Buy"
    elif score <= -0.8:
        action = "Sell"
    else:
        action = "Hold"

    confidence = min(100, max(0, round(abs(score) * 45 + 35)))
    return {
        "action": action,
        "score": round(score, 3),
        "confidence": confidence,
        "rationale": "; ".join(reasons[:5]),
    }


def _load_peg_fallbacks() -> dict[str, float | None]:
    if not FUNDAMENTALS_CSV.exists():
        return {}

    out: dict[str, float | None] = {}
    with FUNDAMENTALS_CSV.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            ticker = (row.get("ticker") or "").strip()
            if ticker:
                out[ticker] = _num(row.get("pegRatio"))
    return out


def _summary_for_stock(sector: dict[str, Any], stock: dict[str, Any]) -> str:
    fundamentals = stock.get("fundamentals") or {}
    recommendation = stock.get("recommendation") or {}
    ticker = stock.get("ticker") or "Ticker"
    name = _text(fundamentals.get("shortName")) or _text(fundamentals.get("industry")) or ticker
    pb = _num(fundamentals.get("priceToBook"))
    pe = _num(fundamentals.get("trailingPE") or fundamentals.get("forwardPE"))
    peg = _num(fundamentals.get("pegRatio"))
    dividend_yield = _num(fundamentals.get("dividendYield"))

    return (
        f"{ticker} ({name}) is a top active {sector.get('sector')} stock for this run. "
        f"Last close: {_fmt_num(stock.get('currentPrice'))}. "
        f"Model signal: {recommendation.get('action', 'Hold')} "
        f"({recommendation.get('confidence', 'n/a')} confidence). "
        f"Fundamentals snapshot: P/B {_fmt_num(pb)}, P/E {_fmt_num(pe)}, "
        f"PEG {_fmt_num(peg)}, dividend yield {_fmt_yield(dividend_yield)}. "
        f"Read-through: {_valuation_note(pb, pe, peg)} with {_yield_note(dividend_yield)}. "
        f"Decision snapshot: {(stock.get('decision_snapshot') or {}).get('summary', 'n/a')} "
        f"Signal rationale: {recommendation.get('rationale', 'n/a')}."
    )


def enrich_dashboard_data(data: dict[str, Any]) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    data["last_refresh"] = {
        "iso_utc": now.isoformat(timespec="seconds"),
        "display": now.strftime("%Y-%m-%d %H:%M:%S GMT"),
        "timezone": "GMT",
    }

    peg_fallbacks = _load_peg_fallbacks()
    for sector in data.get("sectors", []):
        for stock in sector.get("top10_active", []):
            fundamentals = stock.setdefault("fundamentals", {})
            ticker = stock.get("ticker")
            if ticker and fundamentals.get("pegRatio") is None:
                fundamentals["pegRatio"] = peg_fallbacks.get(ticker)
            stock["recommendation"] = _recommendation_for_stock(sector, stock)
            stock["decision_snapshot"] = _decision_snapshot_for_stock(stock)
            stock["daily_summary"] = _summary_for_stock(sector, stock)

        selected = []
        seen = set()
        for stock in sector.get("top10_active", []):
            ticker = stock.get("ticker")
            if ticker in seen:
                continue
            seen.add(ticker)
            selected.append(stock)
            if len(selected) == 3:
                break

        sector["top3_explanations"] = []
        for stock in selected:
            sector["top3_explanations"].append(
                {
                    "rank": stock.get("rank"),
                    "ticker": stock.get("ticker"),
                    "summary": stock.get("daily_summary"),
                }
            )

    return data


def load_compact_dashboard_json() -> str:
    data = json.loads(REPORT_JSON.read_text(encoding="utf-8"))
    data = enrich_dashboard_data(data)
    return json.dumps(
        _json_safe(data),
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    ).replace(
        "</", "<\\/"
    )


def update_embedded_data(html: str, compact_json: str) -> str:
    if not DATA_RE.search(html):
        raise RuntimeError(f"Could not find embedded STARE data block in {APP_HTML}")
    return DATA_RE.sub(lambda m: f"{m.group(1)}{compact_json}{m.group(3)}", html, count=1)


def publish() -> None:
    if not APP_HTML.exists():
        raise RuntimeError(f"Missing app HTML: {APP_HTML}")
    if not REPORT_JSON.exists():
        raise RuntimeError(f"Missing dashboard JSON: {REPORT_JSON}")

    app_html = APP_HTML.read_text(encoding="utf-8")
    app_html = update_embedded_data(app_html, load_compact_dashboard_json())

    APP_HTML.write_text(app_html, encoding="utf-8")

    DOCS.mkdir(parents=True, exist_ok=True)
    (DOCS / "index.html").write_text(app_html, encoding="utf-8")
    copy2(REPORT_JSON, DOCS / "sector_dashboard.json")
    if REPORT_CSV.exists():
        copy2(REPORT_CSV, DOCS / "sector_dashboard_top10.csv")
    if LOGO.exists():
        copy2(LOGO, DOCS / LOGO.name)
    (DOCS / ".nojekyll").write_text("", encoding="utf-8")

    print("Published STARE app to:")
    print(f"- {APP_HTML.relative_to(ROOT)}")
    print(f"- {(DOCS / 'index.html').relative_to(ROOT)}")


if __name__ == "__main__":
    publish()
