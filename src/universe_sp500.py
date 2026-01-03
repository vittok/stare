from __future__ import annotations

from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
import requests

WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
REQUIRED_COLS = {"Symbol", "Security", "GICS Sector", "GICS Sub-Industry"}


def normalize_yahoo_ticker(symbol: str) -> str:
    # Yahoo Finance uses '-' instead of '.' for class shares
    return symbol.replace(".", "-").strip().upper()


def fetch_html(url: str) -> str:
    headers = {
        "User-Agent": "stare-sp500-universe/1.0 (+https://github.com/; contact: none)",
        "Accept-Language": "en-US,en;q=0.9",
    }
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return r.text


def find_constituents_table(tables: list[pd.DataFrame]) -> pd.DataFrame:
    for t in tables:
        cols = set(map(str, t.columns))
        if REQUIRED_COLS.issubset(cols):
            return t[list(REQUIRED_COLS)].copy()
    raise RuntimeError(
        f"Could not find constituents table. Expected columns: {sorted(REQUIRED_COLS)}"
    )


def fetch_sp500_table() -> pd.DataFrame:
    html = fetch_html(WIKI_URL)
    tables = pd.read_html(html)
    if not tables:
        raise RuntimeError("No tables found on the Wikipedia page.")
    return find_constituents_table(tables)


def build_universe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(
        columns={
            "Symbol": "ticker_original",
            "Security": "security",
            "GICS Sector": "sector",
            "GICS Sub-Industry": "sub_industry",
        }
    )
    df["ticker_yahoo"] = df["ticker_original"].apply(normalize_yahoo_ticker)

    df = df[
        ["ticker_yahoo", "ticker_original", "security", "sector", "sub_industry"]
    ].sort_values("ticker_yahoo")

    return df.reset_index(drop=True)


def main():
    print("Fetching S&P 500 universe...")
    raw_df = fetch_sp500_table()
    universe_df = build_universe(raw_df)

    out_path = Path("data/universe_sp500.csv")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    universe_df.to_csv(out_path, index=False)

    ts = datetime.now(timezone.utc).isoformat()
    print(f"Universe written: {out_path}")
    print(f"Tickers: {len(universe_df)}")
    print(f"Updated at: {ts}")


if __name__ == "__main__":
    main()

