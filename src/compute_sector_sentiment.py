from __future__ import annotations

import json
from pathlib import Path
from dataclasses import dataclass
import pandas as pd
from sqlalchemy import create_engine, text


SENTIMENT_SCHEMA = """
CREATE TABLE IF NOT EXISTS sector_sentiment (
  sector TEXT NOT NULL,
  week_ending TEXT NOT NULL,
  raw_score REAL,
  direction TEXT,
  strength INTEGER,
  diagnostics_json TEXT,
  PRIMARY KEY (sector, week_ending)
);
"""


@dataclass
class SectorSentimentConfig:
    db_path: Path = Path("data/stocks.db")
    universe_csv: Path = Path("data/universe_sp500.csv")


def init_schema(engine):
    with engine.begin() as conn:
        for stmt in SENTIMENT_SCHEMA.strip().split(";"):
            s = stmt.strip()
            if s:
                conn.execute(text(s))


def load_inputs(engine, universe_csv: Path):
    weekly = pd.read_sql(
        "SELECT * FROM weekly_stats",
        engine,
        parse_dates=["week_ending"],
    )
    universe = pd.read_csv(universe_csv)
    return weekly, universe


def compute_sector_sentiment(weekly: pd.DataFrame, universe: pd.DataFrame) -> pd.DataFrame:
    df = weekly.merge(
        universe[["ticker_yahoo", "sector"]],
        left_on="ticker",
        right_on="ticker_yahoo",
        how="inner",
    )

    rows = []
    for sector, g in df.groupby("sector"):
        n = len(g)
        if n < 5:
            continue

        breadth = (g["weekly_return"] > 0).mean()
        breadth_signal = (breadth - 0.5) * 2.0

        median_ret = g["weekly_return"].median()
        return_signal = max(-1.0, min(1.0, median_ret / 0.03)) if pd.notna(median_ret) else 0.0

        median_vol_ratio = g["vol_ratio"].median()
        volume_signal = (
            max(-1.0, min(1.0, (median_vol_ratio - 1.0) / 0.5))
            if pd.notna(median_vol_ratio)
            else 0.0
        )

        raw = (
            0.50 * breadth_signal
            + 0.35 * return_signal
            + 0.15 * volume_signal
        )

        if abs(raw) < 0.05:
            direction = "Neutral"
        elif raw > 0:
            direction = "Bullish"
        else:
            direction = "Bearish"

        strength = int(min(100, abs(raw) * 100))

        rows.append(
            {
                "sector": sector,
                "week_ending": g["week_ending"].iloc[0].date().isoformat(),
                "raw_score": raw,
                "direction": direction,
                "strength": strength,
                "diagnostics_json": json.dumps(
                    {
                        "n_stocks": n,
                        "breadth": breadth,
                        "median_return": median_ret,
                        "median_vol_ratio": median_vol_ratio,
                        "signals": {
                            "breadth_signal": breadth_signal,
                            "return_signal": return_signal,
                            "volume_signal": volume_signal,
                        },
                    },
                    indent=2,
                ),
            }
        )

    return pd.DataFrame(rows)


def save_sector_sentiment(engine, df: pd.DataFrame):
    records = df.to_dict(orient="records")
    with engine.begin() as conn:
        conn.execute(
            text("""
            INSERT OR REPLACE INTO sector_sentiment
              (sector, week_ending, raw_score, direction, strength, diagnostics_json)
            VALUES
              (:sector, :week_ending, :raw_score, :direction, :strength, :diagnostics_json)
            """),
            records,
        )


def main():
    cfg = SectorSentimentConfig()
    engine = create_engine(f"sqlite:///{cfg.db_path.as_posix()}", future=True)

    init_schema(engine)
    weekly, universe = load_inputs(engine, cfg.universe_csv)
    sentiment = compute_sector_sentiment(weekly, universe)
    save_sector_sentiment(engine, sentiment)

    print("Sector sentiment:")
    print(sentiment.sort_values("strength", ascending=False)[
        ["sector", "direction", "strength", "raw_score"]
    ])


if __name__ == "__main__":
    main()
