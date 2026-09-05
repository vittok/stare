from __future__ import annotations

import sys
from pathlib import Path
from unittest import TestCase

from fastapi import HTTPException

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "apps/api"))

from app.routes.history import _selection, _series, router  # noqa: E402


class HistoryRouteTests(TestCase):
    def test_history_routes_are_registered(self) -> None:
        paths = {route.path for route in router.routes}

        self.assertIn("/api/history/groups", paths)
        self.assertIn("/api/history/tickers", paths)

    def test_selection_removes_blanks_and_duplicates(self) -> None:
        self.assertEqual(_selection(" AAPL,MSFT,AAPL, ", limit=5), ["AAPL", "MSFT"])

    def test_selection_enforces_comparison_limit(self) -> None:
        with self.assertRaises(HTTPException):
            _selection("A,B,C", limit=2)

    def test_series_groups_rows_without_losing_point_fields(self) -> None:
        rows = [
            {"ticker": "AAPL", "observed_at": "2026-09-01", "current_price": 1},
            {"ticker": "AAPL", "observed_at": "2026-09-02", "current_price": 2},
            {"ticker": "MSFT", "observed_at": "2026-09-01", "current_price": 3},
        ]

        grouped = _series(rows, "ticker")

        self.assertEqual([item["name"] for item in grouped], ["AAPL", "MSFT"])
        self.assertEqual(len(grouped[0]["points"]), 2)
        self.assertNotIn("ticker", grouped[0]["points"][0])
