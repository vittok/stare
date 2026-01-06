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


def load_inputs(engine, universe_csv: Path):
    weekly = pd.read_sql("SELECT * FROM weekly_stats", engine)
    universe = pd.read_csv(universe_csv)
    return weekly, universe


def rank_top_active(weekly: pd.DataFrame, universe: pd.DataFrame, top_n: int):
    df = weekly.merge(
        universe[["ticker_yahoo", "sector"]],
        left_on="ticker",
        right_on="ticker_yahoo",
        how="inner",
    )

    rows = []
    for sector, g in df.groupby("sector"):
        g = g.sort_values("dollar_vol_week", ascending=False).head(top_n)
        week_ending = g["week_ending"].iloc[0]

        for i, r in enumerate(g.itertuples(index=False), start=1):
            rows.append(
                {
                    "sector": sector,
                    "week_ending": week_ending,
                    "rank": i,
                    "ticker": r.ticker,
                    "dollar_vol_week": r.dollar_vol_week,
                    "weekly_return": r.weekly_return,
                    "vol_ratio": r.vol_ratio,
                }
            )

    return pd.DataFrame(rows)


def save_top_active(engine, df: pd.DataFrame):
    records = df.to_dict(orient="records")
    with engine.begin() as conn:
        conn.execute(
            text("""
            INSERT OR REPLACE INTO sector_top_active
              (sector, week_ending, rank, ticker, dollar_vol_week, weekly_return, vol_ratio)
            VALUES
              (:sector, :week_ending, :rank, :ticker, :dollar_vol_week, :weekly_return, :vol_ratio)
            """),
            records,
        )


def main():
    cfg = TopActiveConfig()
    engine = create_engine(f"sqlite:///{cfg.db_path.as_posix()}", future=True)

    init_schema(engine)
    weekly, universe = load_inputs(engine, cfg.universe_csv)
    top_active = rank_top_active(weekly, universe, cfg.top_n)
    save_top_active(engine, top_active)

    print("Top active stocks (sample):")
    print(top_active.head(15))


if __name__ == "__main__":
    main()
