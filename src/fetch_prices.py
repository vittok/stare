from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import List, Tuple

import pandas as pd
import yfinance as yf

from store_sqlite import SQLiteStore


@dataclass
class FetchPricesConfig:
    universe_csv: Path = Path("data/universe_sp500.csv")
    db_path: Path = Path("data/stocks.db")
    period: str = "3mo"
    interval: str = "1d"
    chunk_size: int = 80          # 50-120 is usually fine
    max_retries: int = 4
    sleep_seconds: float = 1.0    # base sleep between chunks


def load_universe_tickers(universe_csv: Path) -> List[str]:
    df = pd.read_csv(universe_csv)
    if "ticker_yahoo" not in df.columns:
        raise RuntimeError("Universe CSV missing 'ticker_yahoo' column.")
    tickers = df["ticker_yahoo"].dropna().astype(str).str.strip().tolist()
    # Deduplicate while preserving order
    seen = set()
    out = []
    for t in tickers:
        if t and t not in seen:
            out.append(t)
            seen.add(t)
    return out


def chunk_list(items: List[str], n: int) -> List[List[str]]:
    return [items[i:i+n] for i in range(0, len(items), n)]


def _normalize_download_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize yfinance.download output into rows:
      ticker, date, open, high, low, close, adj_close, volume

    yfinance.download returns either:
      - Single ticker: columns are OHLCV, index is Date
      - Multiple tickers: columns are MultiIndex (Field, Ticker)
    """
    if df is None or df.empty:
        return pd.DataFrame()

    if isinstance(df.columns, pd.MultiIndex):
        # Multi-ticker: columns like ('Close', 'AAPL'), ...
        df2 = df.copy()
        df2.index = pd.to_datetime(df2.index)
        stacked = df2.stack(level=1)  # index: Date, Ticker
        stacked.index.names = ["Date", "Ticker"]
        stacked = stacked.reset_index()

        # Now columns contain fields: Open High Low Close Adj Close Volume
        stacked.rename(columns={"Ticker": "ticker"}, inplace=True)
        stacked["date"] = pd.to_datetime(stacked["Date"]).dt.date.astype(str)

        rename = {
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Adj Close": "adj_close",
            "Volume": "volume",
        }
        for k, v in rename.items():
            if k in stacked.columns:
                stacked.rename(columns={k: v}, inplace=True)

        out_cols = ["ticker", "date", "open", "high", "low", "close", "adj_close", "volume"]
        for c in out_cols:
            if c not in stacked.columns:
                stacked[c] = None
        return stacked[out_cols].dropna(subset=["close"], how="all")

    else:
        # Single ticker case (rare here because we chunk, but handle anyway)
        df2 = df.copy()
        df2.index = pd.to_datetime(df2.index)
        df2 = df2.reset_index().rename(columns={"Date": "date"})
        df2["date"] = pd.to_datetime(df2["date"]).dt.date.astype(str)
        rename = {
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Adj Close": "adj_close",
            "Volume": "volume",
        }
        df2.rename(columns=rename, inplace=True)
        df2["ticker"] = "UNKNOWN"
        out_cols = ["ticker", "date", "open", "high", "low", "close", "adj_close", "volume"]
        for c in out_cols:
            if c not in df2.columns:
                df2[c] = None
        return df2[out_cols]


def download_chunk(tickers: List[str], period: str, interval: str) -> pd.DataFrame:
    # Use threads=True for speed; group_by='column' is default.
    return yf.download(
        tickers=tickers,
        period=period,
        interval=interval,
        auto_adjust=False,
        threads=True,
        progress=False,
    )


def fetch_and_store_prices(cfg: FetchPricesConfig) -> Tuple[int, int]:
    """
    Returns: (n_chunks, n_rows_upserted)
    """
    if not cfg.universe_csv.exists():
        raise RuntimeError(f"Universe file not found: {cfg.universe_csv}. Run universe_sp500.py first.")

    tickers = load_universe_tickers(cfg.universe_csv)
    chunks = chunk_list(tickers, cfg.chunk_size)

    store = SQLiteStore(cfg.db_path)
    store.init_schema()

    total_rows = 0
    print(f"Fetching prices for {len(tickers)} tickers in {len(chunks)} chunks...")

    for i, chunk in enumerate(chunks, start=1):
        attempt = 0
        while True:
            attempt += 1
            try:
                print(f"[{i}/{len(chunks)}] Downloading {len(chunk)} tickers (attempt {attempt})...")
                raw = download_chunk(chunk, cfg.period, cfg.interval)
                norm = _normalize_download_df(raw)

                # yfinance multiindex normalization uses 'ticker' already
                # Ensure only our chunk tickers
                if not norm.empty:
                    norm["ticker"] = norm["ticker"].astype(str).str.strip()
                    norm = norm[norm["ticker"].isin(chunk)]

                n = store.upsert_prices(norm)
                total_rows += n
                print(f"  upserted rows: {n}  (total: {total_rows})")
                break
            except Exception as e:
                if attempt >= cfg.max_retries:
                    print(f"  FAILED chunk {i} after {attempt} attempts: {e}")
                    break
                backoff = cfg.sleep_seconds * (2 ** (attempt - 1))
                print(f"  error: {e}  -> retrying in {backoff:.1f}s")
                time.sleep(backoff)

        time.sleep(cfg.sleep_seconds)

    return len(chunks), total_rows


def main():
    cfg = FetchPricesConfig()
    fetch_and_store_prices(cfg)
    print("Done.")


if __name__ == "__main__":
    main()

