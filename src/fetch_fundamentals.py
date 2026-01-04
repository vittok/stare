from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import yfinance as yf
from sqlalchemy import create_engine, text


FUND_SCHEMA = """
CREATE TABLE IF NOT EXISTS fundamentals_snapshot (
  ticker TEXT NOT NULL,
  asof_utc TEXT NOT NULL,
  info_json TEXT,
  normalized_json TEXT,
  PRIMARY KEY (ticker, asof_utc)
);

CREATE TABLE IF NOT EXISTS fundamentals_latest (
  ticker TEXT PRIMARY KEY,
  asof_utc TEXT,
  normalized_json TEXT
);
"""


@dataclass
class FundamentalsConfig:
    db_path: Path = Path("data/stocks.db")
    universe_csv: Path = Path("data/universe_sp500.csv")
    sleep_seconds: float = 0.4
    max_retries: int = 4
    write_report_csv: bool = True
    report_path: Path = Path("reports/fundamentals_sp500_latest.csv")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def init_schema(engine) -> None:
    with engine.begin() as conn:
        for stmt in FUND_SCHEMA.strip().split(";"):
            s = stmt.strip()
            if s:
                conn.execute(text(s))


def load_sp500_tickers(universe_csv: Path) -> List[str]:
    df = pd.read_csv(universe_csv)
    if "ticker_yahoo" not in df.columns:
        raise RuntimeError("Universe CSV missing 'ticker_yahoo'. Run src/universe_sp500.py first.")
    tickers = df["ticker_yahoo"].dropna().astype(str).str.strip().tolist()

    # Deduplicate while preserving order
    seen = set()
    out = []
    for t in tickers:
        if t and t not in seen:
            out.append(t)
            seen.add(t)
    return out


def _num(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, bool):
            return float(x)
        return float(x)
    except Exception:
        return None


def _str(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    return s if s else None


def normalize_info(info: Dict[str, Any]) -> Dict[str, Any]:
    return {
        # identity
        "shortName": _str(info.get("shortName")),
        "longName": _str(info.get("longName")),
        "symbol": _str(info.get("symbol")),
        "exchange": _str(info.get("exchange")),
        "currency": _str(info.get("currency")),
        "sector": _str(info.get("sector")),
        "industry": _str(info.get("industry")),
        "country": _str(info.get("country")),

        # valuation/size
        "marketCap": _num(info.get("marketCap")),
        "enterpriseValue": _num(info.get("enterpriseValue")),
        "trailingPE": _num(info.get("trailingPE")),
        "forwardPE": _num(info.get("forwardPE")),
        "priceToBook": _num(info.get("priceToBook")),
        "pegRatio": _num(info.get("pegRatio")),

        # margins/returns
        "profitMargins": _num(info.get("profitMargins")),
        "operatingMargins": _num(info.get("operatingMargins")),
        "grossMargins": _num(info.get("grossMargins")),
        "returnOnEquity": _num(info.get("returnOnEquity")),
        "returnOnAssets": _num(info.get("returnOnAssets")),

        # growth
        "revenueGrowth": _num(info.get("revenueGrowth")),
        "earningsGrowth": _num(info.get("earningsGrowth")),

        # leverage/liquidity
        "totalDebt": _num(info.get("totalDebt")),
        "debtToEquity": _num(info.get("debtToEquity")),
        "currentRatio": _num(info.get("currentRatio")),
        "quickRatio": _num(info.get("quickRatio")),

        # dividends
        "dividendYield": _num(info.get("dividendYield")),
        "payoutRatio": _num(info.get("payoutRatio")),
        "fiveYearAvgDividendYield": _num(info.get("fiveYearAvgDividendYield")),

        # risk/range
        "beta": _num(info.get("beta")),
        "fiftyTwoWeekLow": _num(info.get("fiftyTwoWeekLow")),
        "fiftyTwoWeekHigh": _num(info.get("fiftyTwoWeekHigh")),
    }


def fetch_one_info(ticker: str) -> Dict[str, Any]:
    t = yf.Ticker(ticker)
    info = t.info or {}
    return info if isinstance(info, dict) else {}


def upsert_snapshot(engine, ticker: str, asof_utc: str, info: Dict[str, Any], norm: Dict[str, Any]) -> None:
    info_json = json.dumps(info, ensure_ascii=False)
    norm_json = json.dumps(norm, ensure_ascii=False)

    with engine.begin() as conn:
        conn.execute(
            text("""
            INSERT OR REPLACE INTO fundamentals_snapshot
              (ticker, asof_utc, info_json, normalized_json)
            VALUES
              (:ticker, :asof_utc, :info_json, :normalized_json)
            """),
            dict(ticker=ticker, asof_utc=asof_utc, info_json=info_json, normalized_json=norm_json),
        )
        conn.execute(
            text("""
            INSERT OR REPLACE INTO fundamentals_latest
              (ticker, asof_utc, normalized_json)
            VALUES
              (:ticker, :asof_utc, :normalized_json)
            """),
            dict(ticker=ticker, asof_utc=asof_utc, normalized_json=norm_json),
        )


def main():
    cfg = FundamentalsConfig()
    print("Using DB:", cfg.db_path.resolve())
    print("Using universe:", cfg.universe_csv.resolve())

    if not cfg.universe_csv.exists():
        raise RuntimeError(f"Missing universe file: {cfg.universe_csv}. Run src/universe_sp500.py first.")

    engine = create_engine(f"sqlite:///{cfg.db_path.as_posix()}", future=True)
    init_schema(engine)

    tickers = load_sp500_tickers(cfg.universe_csv)
    asof = utc_now_iso()
    print(f"Fetching fundamentals for {len(tickers)} tickers...")

    rows = []
    failures = []

    for i, ticker in enumerate(tickers, start=1):
        attempt = 0
        while True:
            attempt += 1
            try:
                info = fetch_one_info(ticker)
                norm = normalize_info(info)
                upsert_snapshot(engine, ticker, asof, info, norm)
                rows.append({"ticker": ticker, "asof_utc": asof, **norm})
                break
            except Exception as e:
                if attempt >= cfg.max_retries:
                    failures.append({"ticker": ticker, "error": str(e)})
                    print(f"FAIL {ticker}: {e}")
                    break
                backoff = cfg.sleep_seconds * (2 ** (attempt - 1))
                print(f"retry {ticker} (attempt {attempt}) -> {e}; sleeping {backoff:.1f}s")
                time.sleep(backoff)

        if i % 25 == 0 or i == len(tickers):
            print(f"  {i}/{len(tickers)} done")
        time.sleep(cfg.sleep_seconds)

    if failures:
        Path("reports").mkdir(parents=True, exist_ok=True)
        Path("reports/fundamentals_failures.json").write_text(
            json.dumps(failures, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        print(f"Wrote failures: reports/fundamentals_failures.json (count={len(failures)})")

    df = pd.DataFrame(rows)
    if cfg.write_report_csv and not df.empty:
        cfg.report_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(cfg.report_path, index=False)
        print("Wrote report:", cfg.report_path)

    print("Done.")


if __name__ == "__main__":
    main()