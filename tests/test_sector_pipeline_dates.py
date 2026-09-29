from __future__ import annotations

import sys
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

import pandas as pd
from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import compute_sector_sentiment as sentiment
import rank_sector_top_active as top_active


class SectorPipelineDateTests(TestCase):
    def setUp(self) -> None:
        self.engine = create_engine("sqlite:///:memory:", future=True)
        self.weekly = pd.DataFrame(
            [
                {
                    "ticker": ticker,
                    "week_ending": "2026-09-28",
                    "weekly_return": 0.01,
                    "dollar_vol_week": 1_000_000,
                    "week_volume": 10_000,
                    "vol_ratio": 1.1,
                }
                for ticker in ("A", "B", "C", "D", "E")
            ]
            + [
                {
                    "ticker": "7203.T",
                    "week_ending": "2026-09-29",
                    "weekly_return": 0.02,
                    "dollar_vol_week": 2_000_000,
                    "week_volume": 20_000,
                    "vol_ratio": 1.2,
                }
            ]
        )
        self.weekly.to_sql("weekly_stats", self.engine, index=False)
        pd.DataFrame(
            [
                {"ticker": ticker, "date": date, "close": 100.0, "volume": 10_000}
                for ticker, date in [
                    ("A", "2026-09-28"),
                    ("B", "2026-09-28"),
                    ("C", "2026-09-28"),
                    ("D", "2026-09-28"),
                    ("E", "2026-09-28"),
                    ("7203.T", "2026-09-29"),
                ]
            ]
        ).to_sql("prices", self.engine, index=False)

    def test_sector_loaders_choose_latest_date_after_scoping_to_sp500(self) -> None:
        universe = pd.DataFrame(
            {
                "ticker_yahoo": ["A", "B", "C", "D", "E"],
                "sector": ["Industrials"] * 5,
            }
        )
        with TemporaryDirectory() as directory:
            universe_csv = Path(directory) / "universe.csv"
            universe.to_csv(universe_csv, index=False)

            sentiment_weekly, _ = sentiment.load_inputs(self.engine, universe_csv)
            top_active_weekly, _, _ = top_active.load_inputs(self.engine, universe_csv)

        self.assertEqual(set(sentiment_weekly["ticker"]), {"A", "B", "C", "D", "E"})
        self.assertEqual(
            {value.date().isoformat() for value in sentiment_weekly["week_ending"]},
            {"2026-09-28"},
        )
        self.assertEqual(set(top_active_weekly["ticker"]), {"A", "B", "C", "D", "E"})
        self.assertEqual(set(top_active_weekly["week_ending"]), {"2026-09-28"})

    def test_empty_sentiment_result_fails_with_clear_error(self) -> None:
        sentiment.init_schema(self.engine)

        with self.assertRaisesRegex(RuntimeError, "No sector sentiment rows"):
            sentiment.save_sector_sentiment(self.engine, pd.DataFrame())

    def test_empty_top_active_result_preserves_existing_rows(self) -> None:
        top_active.init_schema(self.engine)
        with self.engine.begin() as connection:
            connection.execute(
                text(
                    """
                    INSERT INTO sector_top_active (sector, week_ending, rank, ticker)
                    VALUES ('Industrials', '2026-09-28', 1, 'A')
                    """
                )
            )

        with self.assertRaisesRegex(RuntimeError, "No sector top-active rows"):
            top_active.save_top_active(self.engine, pd.DataFrame())

        with self.engine.begin() as connection:
            count = connection.execute(text("SELECT COUNT(*) FROM sector_top_active")).scalar_one()
        self.assertEqual(count, 1)
