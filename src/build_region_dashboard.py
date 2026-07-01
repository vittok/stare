from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import create_engine, text

from build_sector_dashboard import (
    compute_7d_returns,
    compute_latest_prices,
    expand_fundamentals_json,
    load_fundamentals_latest,
)


@dataclass
class RegionDashboardConfig:
    db_path: Path = Path("data/stocks.db")
    universe_csv: Path = Path("data/universe_global.csv")
    out_json: Path = Path("reports/region_dashboard.json")
    out_csv: Path = Path("reports/region_dashboard_top_active.csv")


def _safe_float(value: Any) -> float | None:
    return None if pd.isna(value) else float(value)


def load_region_top_active(engine) -> pd.DataFrame:
    df = pd.read_sql(
        text("""
            SELECT region, market, country, week_ending, rank, ticker, volume_date,
                   dollar_vol_latest, latest_volume, dollar_vol_week, weekly_return, vol_ratio
            FROM region_top_active
            ORDER BY region, market, rank
        """),
        engine,
    )
    if df.empty:
        raise RuntimeError("No region_top_active data found. Run rank_region_top_active.py first.")
    return df.copy()


def compute_region_sentiment(top_active: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for region, g in top_active.groupby("region"):
        n = len(g)
        breadth = (pd.to_numeric(g["weekly_return"], errors="coerce") > 0).mean()
        breadth_signal = (breadth - 0.5) * 2.0
        median_ret = pd.to_numeric(g["weekly_return"], errors="coerce").median()
        return_signal = max(-1.0, min(1.0, median_ret / 0.03)) if pd.notna(median_ret) else 0.0
        median_vol_ratio = pd.to_numeric(g["vol_ratio"], errors="coerce").median()
        volume_signal = (
            max(-1.0, min(1.0, (median_vol_ratio - 1.0) / 0.5))
            if pd.notna(median_vol_ratio)
            else 0.0
        )
        raw = 0.50 * breadth_signal + 0.35 * return_signal + 0.15 * volume_signal
        direction = "Neutral" if abs(raw) < 0.05 else "Bullish" if raw > 0 else "Bearish"
        strength = int(min(100, abs(raw) * 100))
        markets = (
            g.groupby("market")["dollar_vol_latest"]
            .sum(min_count=1)
            .sort_values(ascending=False)
            .index.tolist()
        )
        rows.append(
            {
                "region": region,
                "sector": region,
                "week_ending": g["week_ending"].max(),
                "raw_score": raw,
                "direction": direction,
                "strength": strength,
                "diagnostics": {
                    "n_stocks": n,
                    "top_markets": markets,
                    "breadth": breadth,
                    "median_return": median_ret,
                    "median_vol_ratio": median_vol_ratio,
                    "signals": {
                        "breadth_signal": breadth_signal,
                        "return_signal": return_signal,
                        "volume_signal": volume_signal,
                    },
                },
            }
        )
    return pd.DataFrame(rows)


def build_flat_region_dashboard(
    top_active: pd.DataFrame,
    sentiment: pd.DataFrame,
    fundamentals_expanded: pd.DataFrame,
    universe: pd.DataFrame,
    r7: pd.DataFrame,
    latest_prices: pd.DataFrame,
) -> pd.DataFrame:
    df = top_active.merge(
        sentiment[["region", "raw_score", "direction", "strength"]],
        on="region",
        how="left",
    )
    df = df.merge(fundamentals_expanded, on="ticker", how="left", suffixes=("", "_fund"))
    df = df.merge(
        universe[["ticker_yahoo", "security", "sector"]],
        left_on="ticker",
        right_on="ticker_yahoo",
        how="left",
        suffixes=("", "_universe"),
    )
    universe_sector_col = "sector_universe" if "sector_universe" in df.columns else "sector"
    df["shortName"] = df["shortName"].fillna(df["security"])
    df["industry"] = df["industry"].fillna(df[universe_sector_col])
    df = df.merge(r7, on="ticker", how="left")
    df = df.merge(latest_prices, on="ticker", how="left")

    cols = [
        "region", "sector", "market", "country", "week_ending", "rank", "ticker",
        "direction", "strength", "raw_score", "volume_date", "dollar_vol_latest",
        "latest_volume", "dollar_vol_week", "weekly_return", "vol_ratio",
        "currentPrice", "priceDate", "previousClose", "previousCloseDate",
        "closeChange", "closeChangePct", "closeDirection", "marketCap",
        "trailingPE", "forwardPE", "priceToBook", "pegRatio", "profitMargins",
        "operatingMargins", "grossMargins", "returnOnEquity", "returnOnAssets",
        "revenueGrowth", "earningsGrowth", "totalDebt", "debtToEquity",
        "currentRatio", "quickRatio", "dividendYield", "payoutRatio",
        "fiveYearAvgDividendYield", "beta", "fiftyTwoWeekLow", "fiftyTwoWeekHigh",
        "shortName", "industry", "currency", "exchange", "longBusinessSummary",
        "asof_utc", "return_7d",
    ]
    df["sector"] = df["region"]
    for c in cols:
        if c not in df.columns:
            df[c] = None
    return df[cols].copy()


def _stock_from_row(r: pd.Series) -> dict[str, Any]:
    return {
        "rank": int(r["rank"]),
        "ticker": r["ticker"],
        "region": r.get("region"),
        "market": r.get("market"),
        "country": r.get("country"),
        "volume_date": None if pd.isna(r.get("volume_date")) else r.get("volume_date"),
        "dollar_vol_latest": _safe_float(r.get("dollar_vol_latest")),
        "latest_volume": _safe_float(r.get("latest_volume")),
        "weekly_return": _safe_float(r.get("weekly_return")),
        "dollar_vol_week": _safe_float(r.get("dollar_vol_week")),
        "vol_ratio": _safe_float(r.get("vol_ratio")),
        "currentPrice": _safe_float(r.get("currentPrice")),
        "priceDate": None if pd.isna(r.get("priceDate")) else r.get("priceDate"),
        "previousClose": _safe_float(r.get("previousClose")),
        "previousCloseDate": None if pd.isna(r.get("previousCloseDate")) else r.get("previousCloseDate"),
        "closeChange": _safe_float(r.get("closeChange")),
        "closeChangePct": _safe_float(r.get("closeChangePct")),
        "closeDirection": None if pd.isna(r.get("closeDirection")) else r.get("closeDirection"),
        "fundamentals": {
            "shortName": None if pd.isna(r.get("shortName")) else r.get("shortName"),
            "industry": None if pd.isna(r.get("industry")) else r.get("industry"),
            "exchange": None if pd.isna(r.get("exchange")) else r.get("exchange"),
            "currency": None if pd.isna(r.get("currency")) else r.get("currency"),
            "longBusinessSummary": None if pd.isna(r.get("longBusinessSummary")) else r.get("longBusinessSummary"),
            "marketCap": _safe_float(r.get("marketCap")),
            "trailingPE": _safe_float(r.get("trailingPE")),
            "forwardPE": _safe_float(r.get("forwardPE")),
            "priceToBook": _safe_float(r.get("priceToBook")),
            "pegRatio": _safe_float(r.get("pegRatio")),
            "profitMargins": _safe_float(r.get("profitMargins")),
            "operatingMargins": _safe_float(r.get("operatingMargins")),
            "grossMargins": _safe_float(r.get("grossMargins")),
            "returnOnEquity": _safe_float(r.get("returnOnEquity")),
            "returnOnAssets": _safe_float(r.get("returnOnAssets")),
            "revenueGrowth": _safe_float(r.get("revenueGrowth")),
            "earningsGrowth": _safe_float(r.get("earningsGrowth")),
            "totalDebt": _safe_float(r.get("totalDebt")),
            "debtToEquity": _safe_float(r.get("debtToEquity")),
            "currentRatio": _safe_float(r.get("currentRatio")),
            "quickRatio": _safe_float(r.get("quickRatio")),
            "dividendYield": _safe_float(r.get("dividendYield")),
            "payoutRatio": _safe_float(r.get("payoutRatio")),
            "fiveYearAvgDividendYield": _safe_float(r.get("fiveYearAvgDividendYield")),
            "beta": _safe_float(r.get("beta")),
            "fiftyTwoWeekLow": _safe_float(r.get("fiftyTwoWeekLow")),
            "fiftyTwoWeekHigh": _safe_float(r.get("fiftyTwoWeekHigh")),
        },
    }


def build_nested_json(sentiment: pd.DataFrame, flat: pd.DataFrame) -> dict[str, Any]:
    regions = []
    for _, srow in sentiment.sort_values(["region"]).iterrows():
        region = srow["region"]
        sub = flat[flat["region"] == region].copy()
        market_blocks = []
        for market, mg in sub.groupby("market"):
            market_blocks.append(
                {
                    "market": market,
                    "country": mg["country"].dropna().iloc[0] if not mg["country"].dropna().empty else None,
                    "total_dollar_vol_latest": _safe_float(mg["dollar_vol_latest"].sum()),
                    "top10_active": [_stock_from_row(r) for _, r in mg.sort_values("rank").iterrows()],
                }
            )
        top_region = sub.sort_values("dollar_vol_latest", ascending=False).head(10)
        regions.append(
            {
                "region": region,
                "sector": region,
                "week_ending": srow["week_ending"],
                "direction": srow["direction"],
                "strength": int(srow["strength"]) if pd.notna(srow["strength"]) else None,
                "raw_score": _safe_float(srow["raw_score"]),
                "diagnostics": srow["diagnostics"],
                "markets": market_blocks,
                "top10_active": [_stock_from_row(r) for _, r in top_region.iterrows()],
            }
        )

    price_dates = flat["priceDate"].dropna().astype(str).unique().tolist()
    return {
        "generated_from": "market update",
        "market_data": {"latest_price_date": max(price_dates) if price_dates else None},
        "regions": regions,
    }


def main() -> None:
    cfg = RegionDashboardConfig()
    engine = create_engine(f"sqlite:///{cfg.db_path.as_posix()}", future=True)
    top_active = load_region_top_active(engine)
    sentiment = compute_region_sentiment(top_active)
    tickers = top_active["ticker"].dropna().unique().tolist()
    week_ending = top_active["week_ending"].max()
    fundamentals = expand_fundamentals_json(load_fundamentals_latest(engine))
    universe = pd.read_csv(cfg.universe_csv)
    flat = build_flat_region_dashboard(
        top_active,
        sentiment,
        fundamentals,
        universe,
        compute_7d_returns(engine, tickers, week_ending),
        compute_latest_prices(engine, tickers, week_ending),
    )
    nested = build_nested_json(sentiment, flat)
    cfg.out_json.parent.mkdir(parents=True, exist_ok=True)
    cfg.out_csv.parent.mkdir(parents=True, exist_ok=True)
    cfg.out_json.write_text(json.dumps(nested, indent=2, ensure_ascii=False), encoding="utf-8")
    flat.to_csv(cfg.out_csv, index=False)
    print("Wrote:", cfg.out_json)
    print("Wrote:", cfg.out_csv)
    print("Rows:", len(flat))


if __name__ == "__main__":
    main()
