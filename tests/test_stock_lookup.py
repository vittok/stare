from __future__ import annotations

import sys
from pathlib import Path
from unittest import TestCase

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "apps/api"))

from app.market_lookup import (  # noqa: E402
    _decision_snapshot,
    _latest_timeseries_values,
    _market_location,
    normalize_symbol,
)


class StockLookupTests(TestCase):
    def test_symbol_normalization_allows_global_yahoo_symbols(self) -> None:
        self.assertEqual(normalize_symbol(" 9984.t "), "9984.T")
        self.assertEqual(normalize_symbol("brk-b"), "BRK-B")

    def test_symbol_normalization_rejects_path_content(self) -> None:
        with self.assertRaises(ValueError):
            normalize_symbol("../../secret")

    def test_latest_timeseries_value_uses_newest_observation(self) -> None:
        payload = {
            "timeseries": {
                "result": [{
                    "meta": {"type": ["trailingPeRatio"]},
                    "trailingPeRatio": [
                        {"reportedValue": {"raw": 12.5}},
                        {"reportedValue": {"raw": 14.25}},
                    ],
                }]
            }
        }
        self.assertEqual(_latest_timeseries_values(payload), {"trailingPeRatio": 14.25})

    def test_exchange_suffix_maps_global_listing_to_region(self) -> None:
        self.assertEqual(_market_location("9984.T", "JPX"), ("APAC", "Japan"))
        self.assertEqual(_market_location("AAPL", "NMS"), ("NA", "United States"))

    def test_decision_snapshot_explains_limited_on_demand_inputs(self) -> None:
        snapshot = _decision_snapshot(
            {"trailingPE": 12, "priceToBook": 1.5, "pegRatio": 0.8, "dividendYield": 2.2},
            0.04,
            {"action": "Buy", "confidence": 75},
        )
        self.assertEqual(snapshot["valuation"]["label"], "Attractive")
        self.assertEqual(snapshot["momentum"]["label"], "Positive")
        self.assertEqual(snapshot["quality"]["label"], "Limited")
