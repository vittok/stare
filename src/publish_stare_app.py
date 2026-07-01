from __future__ import annotations

import csv
import json
import math
import re
from datetime import datetime, timezone
from pathlib import Path
from shutil import copy2
from typing import Any

from stare_signals import enrich_stock_signals


ROOT = Path(__file__).resolve().parents[1]
APP_HTML = ROOT / "stare_app.html"
REPORT_JSON = ROOT / "reports" / "sector_dashboard.json"
REGION_REPORT_JSON = ROOT / "reports" / "region_dashboard.json"
REPORT_CSV = ROOT / "reports" / "sector_dashboard_top10.csv"
REGION_REPORT_CSV = ROOT / "reports" / "region_dashboard_top_active.csv"
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


def _json_safe(value: Any) -> Any:
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    return value


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


def _north_america_region_from_sectors(sectors: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not sectors:
        return None

    raw_scores = [_num(s.get("raw_score")) for s in sectors]
    raw_scores = [s for s in raw_scores if s is not None]
    raw_score = sum(raw_scores) / len(raw_scores) if raw_scores else 0.0
    if abs(raw_score) < 0.05:
        direction = "Neutral"
    elif raw_score > 0:
        direction = "Bullish"
    else:
        direction = "Bearish"

    stocks = []
    for sector in sectors:
        for stock in sector.get("top10_active", []):
            item = json.loads(json.dumps(stock))
            item["region"] = "NA"
            item["market"] = "S&P 500"
            item["country"] = "United States"
            item["source_sector"] = sector.get("sector")
            stocks.append(item)

    stocks.sort(key=lambda s: _num(s.get("dollar_vol_latest")) or 0.0, reverse=True)
    top10 = []
    for rank, stock in enumerate(stocks[:10], start=1):
        stock["rank"] = rank
        top10.append(stock)

    weeks = sorted({str(s.get("week_ending")) for s in sectors if s.get("week_ending")})
    latest_price_dates = sorted(
        {
            str(stock.get("priceDate"))
            for stock in stocks
            if stock.get("priceDate")
        }
    )
    return {
        "region": "NA",
        "sector": "NA",
        "week_ending": weeks[-1] if weeks else None,
        "direction": direction,
        "strength": int(min(100, abs(raw_score) * 100)),
        "raw_score": raw_score,
        "diagnostics": {
            "n_stocks": len(stocks),
            "top_markets": ["S&P 500"],
            "latest_price_date": latest_price_dates[-1] if latest_price_dates else None,
            "source": "S&P 500 sector dashboard folded into North America",
        },
        "markets": [
            {
                "market": "S&P 500",
                "country": "United States",
                "total_dollar_vol_latest": sum(_num(s.get("dollar_vol_latest")) or 0.0 for s in stocks),
                "top10_active": top10,
            }
        ],
        "top10_active": top10,
    }


def enrich_dashboard_data(data: dict[str, Any]) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    data["last_refresh"] = {
        "iso_utc": now.isoformat(timespec="seconds"),
        "display": now.strftime("%Y-%m-%d %H:%M:%S GMT"),
        "timezone": "GMT",
    }

    peg_fallbacks = _load_peg_fallbacks()
    for sector in [*data.get("sectors", []), *data.get("regions", [])]:
        for stock in sector.get("top10_active", []):
            fundamentals = stock.setdefault("fundamentals", {})
            ticker = stock.get("ticker")
            if ticker and fundamentals.get("pegRatio") is None:
                fundamentals["pegRatio"] = peg_fallbacks.get(ticker)
            enrich_stock_signals(sector, stock)

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
    if REGION_REPORT_JSON.exists():
        region_data = json.loads(REGION_REPORT_JSON.read_text(encoding="utf-8"))
        data["regions"] = region_data.get("regions", [])
        sector_date = (data.get("market_data") or {}).get("latest_price_date")
        region_date = (region_data.get("market_data") or {}).get("latest_price_date")
        if region_date and (not sector_date or region_date > sector_date):
            data.setdefault("market_data", {})["latest_price_date"] = region_date
    na_region = _north_america_region_from_sectors(data.get("sectors", []))
    if na_region:
        regions = [r for r in data.get("regions", []) if r.get("region") != "NA"]
        data["regions"] = [na_region, *regions]
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
    if REGION_REPORT_JSON.exists():
        copy2(REGION_REPORT_JSON, DOCS / "region_dashboard.json")
    if REGION_REPORT_CSV.exists():
        copy2(REGION_REPORT_CSV, DOCS / "region_dashboard_top_active.csv")
    if LOGO.exists():
        copy2(LOGO, DOCS / LOGO.name)
    (DOCS / ".nojekyll").write_text("", encoding="utf-8")

    print("Published STARE app to:")
    print(f"- {APP_HTML.relative_to(ROOT)}")
    print(f"- {(DOCS / 'index.html').relative_to(ROOT)}")


if __name__ == "__main__":
    publish()
