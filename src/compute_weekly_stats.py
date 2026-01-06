from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text


WEEKLY_SCHEMA = """
CREATE TABLE IF NOT EXISTS weekly_stats (
  ticker TEXT NOT NULL,
  week_ending TEXT NOT NULL,      -- ISO date (last session in the 5-session window)
  weekly_return REAL,
  dollar_vol_week REAL,
  week_volume REAL,
  vol_ratio REAL,
  PRIMARY KEY (ticker, week_ending)
);
CREATE INDEX IF NOT EXISTS idx_weekly_stats_ticker ON weekly_stats(ticker);
"""


@dataclass
class WeeklyStatsConfig:
    db_path: Path = Path("data/stocks.db")
    week_sessions: int = 5
    baseline_weeks: int = 8  # baseline volume = prior 8 weeks


def init_schema(engine):
    with engine.begin() as conn:
        for stmt in WEEKLY_SCHEMA.strip().split(";"):
            s = stmt.strip()
            if s:
                conn.execute(text(s))


def load_prices(engine) -> pd.DataFrame:
    with engine.begin() as conn:
        df = pd.read_sql(
            text("""
                SELECT ticker, date, close, volume
                FROM prices
                WHERE close IS NOT NULL
                ORDER BY ticker, date
            """),
            conn,
        )
    if df.empty:
        raise RuntimeError("No prices in DB. Run fetch_prices.py first.")
    df["date"] = pd.to_datetime(df["date"])
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0)
    return df


def compute_last_week_stats(prices: pd.DataFrame, cfg: WeeklyStatsConfig) -> pd.DataFrame:
    n_week = cfg.week_sessions
    n_base_sessions = cfg.baseline_weeks * cfg.week_sessions

    rows = []
    for ticker, g in prices.groupby("ticker", sort=False):
        g = g.dropna(subset=["close"]).sort_values("date")
        if len(g) < n_week:
            continue

        week = g.iloc[-n_week:]
        week_ending = week["date"].iloc[-1].date().isoformat()

        first_close = float(week["close"].iloc[0])
        last_close = float(week["close"].iloc[-1])
        weekly_return = (last_close / first_close - 1.0) if first_close else None

        dollar_vol_week = float((week["close"] * week["volume"]).sum())
        week_volume = float(week["volume"].sum())

        vol_ratio = None
        if len(g) >= n_week + n_base_sessions:
            base = g.iloc[-(n_week + n_base_sessions):-n_week]
            base_total_vol = float(base["volume"].sum())
            avg_week_vol = base_total_vol / cfg.baseline_weeks if cfg.baseline_weeks else None
            vol_ratio = (week_volume / avg_week_vol) if avg_week_vol else None

        rows.append(
            {
                "ticker": ticker,
                "week_ending": week_ending,
                "weekly_return": weekly_return,
                "dollar_vol_week": dollar_vol_week,
                "week_volume": week_volume,
                "vol_ratio": vol_ratio,
            }
        )

    return pd.DataFrame(rows)


def save_weekly_stats(engine, df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0
    records = df.to_dict(orient="records")
    with engine.begin() as conn:
        conn.execute(
            text("""
            INSERT OR REPLACE INTO weekly_stats
              (ticker, week_ending, weekly_return, dollar_vol_week, week_volume, vol_ratio)
            VALUES
              (:ticker, :week_ending, :weekly_return, :dollar_vol_week, :week_volume, :vol_ratio)
            """),
            records,
        )
    return len(df)


def main():
    cfg = WeeklyStatsConfig()
    engine = create_engine(f"sqlite:///{cfg.db_path.as_posix()}", future=True)

    init_schema(engine)
    prices = load_prices(engine)
    stats = compute_last_week_stats(prices, cfg)
    n = save_weekly_stats(engine, stats)

    print(f"weekly_stats rows written: {n}")
    if n:
        print(stats.sort_values("dollar_vol_week", ascending=False).head(10)[
            ["ticker", "week_ending", "weekly_return", "dollar_vol_week", "vol_ratio"]
        ])


if __name__ == "__main__":
    main()
