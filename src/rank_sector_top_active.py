from __future__ import annotations

from pathlib import Path
from dataclasses import dataclass
import pandas as pd
from sqlalchemy import create_engine, text


TOP_ACTIVE_SCHEMA = """
CREATE TABLE IF NOT EXISTS sector_top_active (
  sector TEXT NOT NULL,
  week_ending TEXT NOT NULL,
  rank INTEGER,
  ticker TEXT,
  volume_date TEXT,
  dollar_vol_latest REAL,
  latest_volume REAL,
  dollar_vol_week REAL,
  weekly_return REAL,
  vol_ratio REAL,
  PRIMARY KEY (sector, week_ending, rank)
);
"""


@dataclass
class TopActiveConfig:
    db_path: Path = Path("data/stocks.db")
    universe_csv: Path = Path("data/universe_sp500.csv")
    top_n: int = 10


def init_schema(engine):
    with engine.begin() as conn:
        for stmt in TOP_ACTIVE_SCHEMA.strip().split(";"):
            s = stmt.strip()
            if s:
                conn.execute(text(s))
        cols = {row[1] for row in conn.execute(text("PRAGMA table_info(sector_top_active)"))}
        migrations = {
            "volume_date": "ALTER TABLE sector_top_active ADD COLUMN volume_date TEXT",
            "dollar_vol_latest": "ALTER TABLE sector_top_active ADD COLUMN dollar_vol_latest REAL",
            "latest_volume": "ALTER TABLE sector_top_active ADD COLUMN latest_volume REAL",
        }
        for col, stmt in migrations.items():
            if col not in cols:
                conn.execute(text(stmt))


def load_inputs(engine, universe_csv: Path):
    weekly = pd.read_sql("SELECT * FROM weekly_stats", engine)
    if weekly.empty:
        raise RuntimeError("No weekly_stats data found. Run compute_weekly_stats.py first.")
    latest_week = weekly["week_ending"].max()
    weekly = weekly[weekly["week_ending"] == latest_week].copy()
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
    return weekly, latest_volume[["ticker", "volume_date", "dollar_vol_latest", "latest_volume"]], universe


def rank_top_active(weekly: pd.DataFrame, latest_volume: pd.DataFrame, universe: pd.DataFrame, top_n: int):
    df = weekly.merge(
        universe[["ticker_yahoo", "sector"]],
        left_on="ticker",
        right_on="ticker_yahoo",
        how="inner",
    )
    df = df.merge(latest_volume, on="ticker", how="left")

    rows = []
    for sector, g in df.groupby("sector"):
        g = g.sort_values("dollar_vol_latest", ascending=False).head(top_n)
        week_ending = g["week_ending"].iloc[0]

        for i, r in enumerate(g.itertuples(index=False), start=1):
            rows.append(
                {
                    "sector": sector,
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


def save_top_active(engine, df: pd.DataFrame):
    records = df.to_dict(orient="records")
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM sector_top_active"))
        conn.execute(
            text("""
            INSERT OR REPLACE INTO sector_top_active
              (sector, week_ending, rank, ticker, volume_date, dollar_vol_latest, latest_volume,
               dollar_vol_week, weekly_return, vol_ratio)
            VALUES
              (:sector, :week_ending, :rank, :ticker, :volume_date, :dollar_vol_latest, :latest_volume,
               :dollar_vol_week, :weekly_return, :vol_ratio)
            """),
            records,
        )


def main():
    cfg = TopActiveConfig()
    engine = create_engine(f"sqlite:///{cfg.db_path.as_posix()}", future=True)

    init_schema(engine)
    weekly, latest_volume, universe = load_inputs(engine, cfg.universe_csv)
    top_active = rank_top_active(weekly, latest_volume, universe, cfg.top_n)
    save_top_active(engine, top_active)

    print("Top active stocks (sample):")
    print(top_active.head(15))


if __name__ == "__main__":
    main()
