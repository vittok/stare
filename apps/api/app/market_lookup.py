from __future__ import annotations

import asyncio
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from .scoring import DEFAULT_WEIGHTS, personalized_recommendation

YAHOO_BASE_URL = "https://query2.finance.yahoo.com"
YAHOO_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; STARE/1.0)"}
SYMBOL_PATTERN = re.compile(r"^[A-Z0-9.^=-]{1,32}$")
FUNDAMENTAL_TYPES = ",".join(
    [
        "trailingMarketCap",
        "trailingPeRatio",
        "trailingPbRatio",
        "trailingPegRatio",
    ]
)

_search_cache: dict[str, tuple[float, list[dict[str, Any]]]] = {}
_analysis_cache: dict[str, tuple[float, dict[str, Any]]] = {}


def normalize_symbol(symbol: str) -> str:
    normalized = symbol.strip().upper()
    if not SYMBOL_PATTERN.fullmatch(normalized):
        raise ValueError("Enter a valid market symbol")
    return normalized


def _number(value: Any) -> float | None:
    try:
        number = float(value)
        return number if number == number and abs(number) != float("inf") else None
    except (TypeError, ValueError):
        return None


def _market_location(symbol: str, exchange: str | None) -> tuple[str, str | None]:
    suffixes = {
        ".L": ("EMEA", "United Kingdom"),
        ".DE": ("EMEA", "Germany"),
        ".PA": ("EMEA", "France"),
        ".AS": ("EMEA", "Netherlands"),
        ".MI": ("EMEA", "Italy"),
        ".MC": ("EMEA", "Spain"),
        ".T": ("APAC", "Japan"),
        ".HK": ("APAC", "Hong Kong"),
        ".AX": ("APAC", "Australia"),
        ".KS": ("APAC", "South Korea"),
        ".KQ": ("APAC", "South Korea"),
        ".SA": ("LAC", "Brazil"),
        ".MX": ("LAC", "Mexico"),
        ".BA": ("LAC", "Argentina"),
        ".SN": ("LAC", "Chile"),
    }
    for suffix, location in suffixes.items():
        if symbol.endswith(suffix):
            return location
    if exchange and exchange.upper() in {"NMS", "NGM", "NCM", "NYQ", "PCX", "NASDAQ", "NYSE"}:
        return "NA", "United States"
    return "Other", None


def _latest_timeseries_values(payload: dict[str, Any]) -> dict[str, float]:
    values: dict[str, float] = {}
    for result in payload.get("timeseries", {}).get("result", []):
        types = result.get("meta", {}).get("type", [])
        if not types:
            continue
        key = types[0]
        observations = result.get(key) or []
        if not observations:
            continue
        value = _number(observations[-1].get("reportedValue", {}).get("raw"))
        if value is not None:
            values[key] = value
    return values


def _decision_snapshot(fundamentals: dict[str, Any], weekly_return: float | None, recommendation: dict[str, Any]) -> dict[str, Any]:
    pe = _number(fundamentals.get("trailingPE") or fundamentals.get("forwardPE"))
    pb = _number(fundamentals.get("priceToBook"))
    peg = _number(fundamentals.get("pegRatio"))
    dividend = _number(fundamentals.get("dividendYield"))
    valuation_points = 0
    valuation_notes = []
    if pe is not None:
        valuation_points += 2 if 0 < pe < 15 else 1 if pe <= 35 else -2 if pe > 50 or pe <= 0 else -1
        valuation_notes.append(f"P/E {pe:.2f}")
    if pb is not None:
        valuation_points += 1 if 0 < pb < 2 else -1 if pb > 12 else 0
        valuation_notes.append(f"P/B {pb:.2f}")
    if peg is not None:
        valuation_points += 2 if 0 < peg < 1 else -1 if peg <= 0 or peg > 2 else 0
        valuation_notes.append(f"PEG {peg:.2f}")
    valuation_label = "Attractive" if valuation_points >= 2 else "Expensive" if valuation_points <= -2 else "Fair"
    momentum_label = "Positive" if weekly_return is not None and weekly_return >= 0.03 else "Negative" if weekly_return is not None and weekly_return <= -0.05 else "Neutral"
    income_label = "Supportive" if dividend is not None and dividend >= 2 else "Modest" if dividend else "None"
    return {
        "valuation": {"label": valuation_label, "detail": "; ".join(valuation_notes) or "Valuation data is limited."},
        "quality": {"label": "Limited", "detail": "The on-demand source does not provide a complete profitability history."},
        "risk": {"label": "Limited", "detail": "Review volatility, leverage, and company filings before making a decision."},
        "momentum": {"label": momentum_label, "detail": "One-week close performance is used for current momentum context."},
        "income": {"label": income_label, "detail": f"Indicated trailing dividend yield is {dividend:.2f}%." if dividend is not None else "No dividend was captured in the trailing year."},
        "summary": f"Valuation {valuation_label}; Quality Limited; Risk Limited; Momentum {momentum_label}; Income {income_label}. Overall model signal: {recommendation['action']} ({recommendation['confidence']} confidence).",
    }


async def search_stocks(query: str) -> list[dict[str, Any]]:
    normalized = query.strip()
    if len(normalized) < 1 or len(normalized) > 80:
        raise ValueError("Enter a ticker or company name")
    cache_key = normalized.casefold()
    cached = _search_cache.get(cache_key)
    if cached and time.monotonic() - cached[0] < 300:
        return cached[1]

    async with httpx.AsyncClient(timeout=10, headers=YAHOO_HEADERS) as client:
        response = await client.get(
            f"{YAHOO_BASE_URL}/v1/finance/search",
            params={"q": normalized, "quotesCount": 8, "newsCount": 0},
        )
        response.raise_for_status()
    matches = []
    for quote in response.json().get("quotes", []):
        if quote.get("quoteType") != "EQUITY" or not quote.get("symbol"):
            continue
        matches.append(
            {
                "symbol": quote["symbol"],
                "name": quote.get("longname") or quote.get("shortname") or quote["symbol"],
                "exchange": quote.get("exchDisp") or quote.get("exchange"),
                "sector": quote.get("sector"),
                "industry": quote.get("industry"),
            }
        )
    _search_cache[cache_key] = (time.monotonic(), matches)
    return matches


async def analyze_stock(symbol: str, weights: dict[str, Any] | None = None) -> dict[str, Any]:
    ticker = normalize_symbol(symbol)
    cached = _analysis_cache.get(ticker)
    if cached and time.monotonic() - cached[0] < 300:
        base = cached[1]
    else:
        end = datetime.now(timezone.utc) + timedelta(days=1)
        start = end - timedelta(days=1095)
        async with httpx.AsyncClient(timeout=15, headers=YAHOO_HEADERS) as client:
            chart_request = client.get(
                f"{YAHOO_BASE_URL}/v8/finance/chart/{ticker}",
                params={"range": "1y", "interval": "1d", "events": "div"},
            )
            fundamentals_request = client.get(
                f"{YAHOO_BASE_URL}/ws/fundamentals-timeseries/v1/finance/timeseries/{ticker}",
                params={
                    "symbol": ticker,
                    "type": FUNDAMENTAL_TYPES,
                    "merge": "false",
                    "period1": int(start.timestamp()),
                    "period2": int(end.timestamp()),
                },
            )
            search_request = client.get(
                f"{YAHOO_BASE_URL}/v1/finance/search",
                params={"q": ticker, "quotesCount": 6, "newsCount": 0},
            )
            chart_response, fundamentals_response, search_response = await asyncio.gather(
                chart_request, fundamentals_request, search_request
            )
        chart_response.raise_for_status()
        fundamentals_response.raise_for_status()
        search_response.raise_for_status()

        chart_results = chart_response.json().get("chart", {}).get("result") or []
        if not chart_results:
            raise LookupError("No market data was found for that symbol")
        chart = chart_results[0]
        meta = chart.get("meta", {})
        timestamps = chart.get("timestamp") or []
        quote = (chart.get("indicators", {}).get("quote") or [{}])[0]
        closes = quote.get("close") or []
        volumes = quote.get("volume") or []
        sessions = [
            (timestamp, _number(close), _number(volumes[index]) if index < len(volumes) else None)
            for index, (timestamp, close) in enumerate(zip(timestamps, closes))
            if _number(close) is not None
        ]
        if not sessions:
            raise LookupError("No closing prices were found for that symbol")
        latest_timestamp, current_price, latest_volume = sessions[-1]
        previous_timestamp, previous_close, _ = sessions[-2] if len(sessions) > 1 else sessions[-1]
        weekly_base = sessions[-6][1] if len(sessions) > 5 else sessions[0][1]
        weekly_return = (current_price / weekly_base - 1) if current_price and weekly_base else None
        recent_volumes = [session[2] for session in sessions[-21:-1] if session[2] is not None]
        average_volume = sum(recent_volumes) / len(recent_volumes) if recent_volumes else None
        volume_ratio = latest_volume / average_volume if latest_volume and average_volume else None
        close_change = current_price - previous_close if current_price is not None and previous_close is not None else None
        close_change_pct = close_change / previous_close if close_change is not None and previous_close else None

        search_quotes = search_response.json().get("quotes", [])
        identity = next(
            (item for item in search_quotes if str(item.get("symbol", "")).upper() == ticker),
            {},
        )
        series = _latest_timeseries_values(fundamentals_response.json())
        dividend_total = sum(
            _number(event.get("amount")) or 0
            for event in chart.get("events", {}).get("dividends", {}).values()
        )
        dividend_yield = dividend_total / current_price * 100 if dividend_total and current_price else None
        exchange_code = identity.get("exchange") or meta.get("exchangeName")
        exchange_name = identity.get("exchDisp") or meta.get("fullExchangeName") or exchange_code
        region, country = _market_location(ticker, exchange_code)
        company_name = identity.get("longname") or identity.get("shortname") or meta.get("longName") or meta.get("shortName") or ticker
        sector = identity.get("sector")
        industry = identity.get("industry")
        fundamentals = {
            "shortName": company_name,
            "exchange": exchange_name,
            "currency": meta.get("currency"),
            "sector": sector,
            "industry": industry,
            "country": country,
            "longBusinessSummary": f"{company_name} is listed on {exchange_name or 'its reported exchange'}{f' and operates in {industry}' if industry else ''}.",
            "marketCap": series.get("trailingMarketCap"),
            "trailingPE": series.get("trailingPeRatio"),
            "priceToBook": series.get("trailingPbRatio"),
            "pegRatio": series.get("trailingPegRatio"),
            "dividendYield": dividend_yield,
            "fiftyTwoWeekLow": min(session[1] for session in sessions if session[1] is not None),
            "fiftyTwoWeekHigh": max(session[1] for session in sessions if session[1] is not None),
        }
        base = {
            "ticker": ticker,
            "company_name": company_name,
            "region": region,
            "market": exchange_name,
            "country": country,
            "sector": sector,
            "volume_date": datetime.fromtimestamp(latest_timestamp, timezone.utc).date().isoformat(),
            "price_date": datetime.fromtimestamp(latest_timestamp, timezone.utc).date().isoformat(),
            "current_price": current_price,
            "previous_close": previous_close,
            "previous_close_date": datetime.fromtimestamp(previous_timestamp, timezone.utc).date().isoformat(),
            "close_change": close_change,
            "close_change_pct": close_change_pct,
            "close_direction": "up" if close_change and close_change > 0 else "down" if close_change and close_change < 0 else "flat",
            "weekly_return": weekly_return,
            "dollar_vol_latest": current_price * latest_volume if current_price and latest_volume else None,
            "latest_volume": latest_volume,
            "vol_ratio": volume_ratio,
            "market_cap": fundamentals["marketCap"],
            "trailing_pe": fundamentals["trailingPE"],
            "price_to_book": fundamentals["priceToBook"],
            "peg_ratio": fundamentals["pegRatio"],
            "dividend_yield": fundamentals["dividendYield"],
            "currency": meta.get("currency"),
            "exchange": exchange_name,
            "industry": industry,
            "fundamentals": fundamentals,
        }
        _analysis_cache[ticker] = (time.monotonic(), base)

    group = {"region": "Custom", "direction": "Neutral", "strength": 0, "raw_score": 0}
    standard = personalized_recommendation(group, base, DEFAULT_WEIGHTS)
    personalized = personalized_recommendation(group, base, weights)
    result = {
        **base,
        "action": standard["action"],
        "score": standard["score"],
        "confidence": standard["confidence"],
        "rationale": standard["rationale"],
        "personalized_action": personalized["action"],
        "personalized_score": personalized["score"],
        "personalized_confidence": personalized["confidence"],
        "personalized_rationale": personalized["rationale"],
        "personalized_changed": personalized["action"] != standard["action"],
    }
    result["decision_snapshot"] = _decision_snapshot(
        result["fundamentals"], _number(result.get("weekly_return")), standard
    )
    result["daily_summary"] = (
        f"{ticker} ({result['company_name']}) closed at {result['current_price']:.2f} "
        f"{result['currency'] or ''} on {result['price_date']}. The fundamentals-led model is "
        f"{standard['action']} with {standard['confidence']} confidence. {standard['rationale']}."
    )
    return result
