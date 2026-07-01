from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text


REGION_TOP_ACTIVE_SCHEMA = """
CREATE TABLE IF NOT EXISTS region_top_active (
  region TEXT NOT NULL,
  market TEXT NOT NULL,
  country TEXT,
  week_ending TEXT NOT NULL,
  rank INTEGER,
  ticker TEXT,
  volume_date TEXT,
  dollar_vol_latest REAL,
  latest_volume REAL,
  dollar_vol_week REAL,
  weekly_return REAL,
  vol_ratio REAL,
  PRIMARY KEY (region, market, week_ending, rank)
);
"""


@dataclass
class RegionTopActiveConfig:
    db_path: Path = Path("data/stocks.db")
    universe_csv: Path = Path("data/universe_global.csv")
    top_n_per_market: int = 10
    top_markets_per_region: int = 3


def init_schema(engine) -> None:
    with engine.begin() as conn:
        for stmt in REGION_TOP_ACTIVE_SCHEMA.strip().split(";"):
            s = stmt.strip()
            if s:
                conn.execute(text(s))


def load_inputs(engine, universe_csv: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    weekly = pd.read_sql("SELECT * FROM weekly_stats", engine)
    if weekly.empty:
        raise RuntimeError("No weekly_stats data found. Run compute_weekly_stats.py first.")
    weekly = (
        weekly.sort_values(["ticker", "week_ending"])
        .groupby("ticker", as_index=False, sort=False)
        .tail(1)
        .copy()
    )

    latest_volume = pd.read_sql(
        text("""
            SELECT p.ticker, p.date AS volume_date, p.close, p.volume
            FROM prices p
            JOIN (
                SELECT ticker, MAX(date) AS max_date
                FROM prices
                WHERE close IS NOT NULL
                  AND volume IS NOT NULL
                GROUP BY ticker
            ) latest
              ON p.ticker = latest.ticker
             AND p.date = latest.max_date
            WHERE p.close IS NOT NULL
              AND p.volume IS NOT NULL
        """),
        engine,
    )
    if latest_volume.empty:
        raise RuntimeError("No latest price/volume rows found. Run fetch_prices.py first.")
    latest_volume["close"] = pd.to_numeric(latest_volume["close"], errors="coerce")
    latest_volume["volume"] = pd.to_numeric(latest_volume["volume"], errors="coerce").fillna(0)
    latest_volume["dollar_vol_latest"] = latest_volume["close"] * latest_volume["volume"]
    latest_volume.rename(columns={"volume": "latest_volume"}, inplace=True)

    universe = pd.read_csv(universe_csv)
    required = {"ticker_yahoo", "region", "market", "country"}
    missing = required - set(universe.columns)
    if missing:
        raise RuntimeError(f"Global universe missing columns: {', '.join(sorted(missing))}")
    return weekly, latest_volume[["ticker", "volume_date", "dollar_vol_latest", "latest_volume"]], universe


def rank_region_top_active(
    weekly: pd.DataFrame,
    latest_volume: pd.DataFrame,
    universe: pd.DataFrame,
    top_n_per_market: int,
    top_markets_per_region: int,
) -> pd.DataFrame:
    df = weekly.merge(
        universe[["ticker_yahoo", "region", "market", "country"]],
        left_on="ticker",
        right_on="ticker_yahoo",
        how="inner",
    ).merge(latest_volume, on="ticker", how="left")

    if df.empty:
        return pd.DataFrame()

    market_volume = (
        df.groupby(["region", "market"], dropna=False)["dollar_vol_latest"]
        .sum(min_count=1)
        .reset_index()
        .sort_values(["region", "dollar_vol_latest"], ascending=[True, False])
    )
    selected_markets = set()
    for region, g in market_volume.groupby("region"):
        for market in g.head(top_markets_per_region)["market"].tolist():
            selected_markets.add((region, market))

    rows = []
    df = df[df.apply(lambda r: (r["region"], r["market"]) in selected_markets, axis=1)]
    for (region, market), g in df.groupby(["region", "market"], sort=True):
        g = g.sort_values("dollar_vol_latest", ascending=False).head(top_n_per_market)
        if g.empty:
            continue
        week_ending = g["week_ending"].iloc[0]
        country = g["country"].dropna().iloc[0] if not g["country"].dropna().empty else None
        for i, r in enumerate(g.itertuples(index=False), start=1):
            rows.append(
                {
                    "region": region,
                    "market": market,
                    "country": country,
                    "week_ending": week_ending,
                    "rank": i,
                    "ticker": r.ticker,
                    "volume_date": r.volume_date,
                    "dollar_vol_latest": r.dollar_vol_latest,
                    "latest_volume": r.latest_volume,
                    "dollar_vol_week": r.dollar_vol_week,
                    "weekly_return": r.weekly_return,
                    "vol_ratio": r.vol_ratio,
                }
            )
    return pd.DataFrame(rows)


def save_region_top_active(engine, df: pd.DataFrame) -> None:
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM region_top_active"))
        if not df.empty:
            conn.execute(
                text("""
                INSERT OR REPLACE INTO region_top_active
                  (region, market, country, week_ending, rank, ticker, volume_date,
                   dollar_vol_latest, latest_volume, dollar_vol_week, weekly_return, vol_ratio)
                VALUES
                  (:region, :market, :country, :week_ending, :rank, :ticker, :volume_date,
                   :dollar_vol_latest, :latest_volume, :dollar_vol_week, :weekly_return, :vol_ratio)
                """),
                df.to_dict(orient="records"),
            )


def main() -> None:
    cfg = RegionTopActiveConfig()
    engine = create_engine(f"sqlite:///{cfg.db_path.as_posix()}", future=True)
    init_schema(engine)
    weekly, latest_volume, universe = load_inputs(engine, cfg.universe_csv)
    top_active = rank_region_top_active(
        weekly,
        latest_volume,
        universe,
        cfg.top_n_per_market,
        cfg.top_markets_per_region,
    )
    save_region_top_active(engine, top_active)
    print("Region top active stocks (sample):")
    print(top_active.head(15))


if __name__ == "__main__":
    main()
