from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from sqlalchemy import create_engine, text
import pandas as pd


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS prices (
  ticker TEXT NOT NULL,
  date TEXT NOT NULL,           -- ISO date YYYY-MM-DD
  open REAL,
  high REAL,
  low REAL,
  close REAL,
  adj_close REAL,
  volume REAL,
  PRIMARY KEY (ticker, date)
);

CREATE INDEX IF NOT EXISTS idx_prices_ticker_date ON prices(ticker, date);
"""


@dataclass
class SQLiteStore:
    db_path: Path

    def __post_init__(self):
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.engine = create_engine(f"sqlite:///{self.db_path.as_posix()}", future=True)

    def init_schema(self) -> None:
        with self.engine.begin() as conn:
            for stmt in SCHEMA_SQL.strip().split(";"):
                s = stmt.strip()
                if s:
                    conn.execute(text(s))

    def upsert_prices(self, df: pd.DataFrame) -> int:
        """
        Works on older SQLite versions using INSERT OR REPLACE.
        df columns required:
          ticker, date, open, high, low, close, adj_close, volume
        """
        if df is None or df.empty:
            return 0

        cols = ["ticker", "date", "open", "high", "low", "close", "adj_close", "volume"]
        for c in cols:
            if c not in df.columns:
                df[c] = None
        df = df[cols].copy()

        records = df.to_dict(orient="records")

        with self.engine.begin() as conn:
            conn.execute(
                text("""
                INSERT OR REPLACE INTO prices
                  (ticker, date, open, high, low, close, adj_close, volume)
                VALUES
                  (:ticker, :date, :open, :high, :low, :close, :adj_close, :volume)
                """),
                records,
            )

        return len(df)

