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
    ticker = stock.get("ticker") or "Ticker"
    name = _text(fundamentals.get("shortName")) or _text(fundamentals.get("industry")) or ticker
    pb = _num(fundamentals.get("priceToBook"))
    pe = _num(fundamentals.get("trailingPE") or fundamentals.get("forwardPE"))
    peg = _num(fundamentals.get("pegRatio"))
    dividend_yield = _num(fundamentals.get("dividendYield"))

    return (
        f"{ticker} ({name}) is a top active {sector.get('sector')} stock for this run. "
        f"Fundamentals snapshot: P/B {_fmt_num(pb)}, P/E {_fmt_num(pe)}, "
        f"PEG {_fmt_num(peg)}, dividend yield {_fmt_yield(dividend_yield)}. "
        f"Read-through: {_valuation_note(pb, pe, peg)} with {_yield_note(dividend_yield)}."
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
        selected = []
        seen = set()
        for stock in sector.get("top10_active", []):
            fundamentals = stock.setdefault("fundamentals", {})
            ticker = stock.get("ticker")
            if ticker and fundamentals.get("pegRatio") is None:
                fundamentals["pegRatio"] = peg_fallbacks.get(ticker)
            if ticker in seen:
                continue
            seen.add(ticker)
            selected.append(stock)
            if len(selected) == 3:
                break

        sector["top3_explanations"] = []
        for stock in selected:
            summary = _summary_for_stock(sector, stock)
            stock["daily_summary"] = summary
            sector["top3_explanations"].append(
                {
                    "rank": stock.get("rank"),
                    "ticker": stock.get("ticker"),
                    "summary": summary,
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
    (DOCS / ".nojekyll").write_text("", encoding="utf-8")

    print("Published STARE app to:")
    print(f"- {APP_HTML.relative_to(ROOT)}")
    print(f"- {(DOCS / 'index.html').relative_to(ROOT)}")


if __name__ == "__main__":
    publish()
