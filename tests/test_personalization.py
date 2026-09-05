from __future__ import annotations

import sys
from pathlib import Path
from unittest import TestCase

from pydantic import ValidationError

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "apps/api"))
sys.path.insert(0, str(ROOT / "src"))

from app.scoring import personalized_recommendation  # noqa: E402
from app.routes.personalization import (  # noqa: E402
    ScoringWeightsPayload,
    WatchlistCreate,
    WatchlistUpdate,
)
from stare_signals import recommendation_for_stock  # noqa: E402


class WatchlistPayloadTests(TestCase):
    def test_watchlist_names_and_tickers_are_normalized(self) -> None:
        payload = WatchlistCreate(
            name="  Income ideas  ",
            tickers=[" aapl ", "AAPL", "9984.t", ""],
        )

        self.assertEqual(payload.name, "Income ideas")
        self.assertEqual(payload.tickers, ["AAPL", "9984.T"])

    def test_blank_watchlist_name_is_rejected(self) -> None:
        with self.assertRaises(ValidationError):
            WatchlistUpdate(name="   ")


class ScoringWeightPayloadTests(TestCase):
    def test_model_defaults_are_neutral_multipliers(self) -> None:
        payload = ScoringWeightsPayload()

        self.assertEqual(set(payload.model_dump().values()), {1.0})

    def test_weight_outside_supported_range_is_rejected(self) -> None:
        with self.assertRaises(ValidationError):
            ScoringWeightsPayload(pe_weight=2.1)

    def test_all_zero_weights_are_rejected(self) -> None:
        with self.assertRaises(ValidationError):
            ScoringWeightsPayload(
                group_sentiment_weight=0,
                pe_weight=0,
                pb_weight=0,
                peg_weight=0,
                dividend_weight=0,
                momentum_weight=0,
            )


class PersonalizedRecommendationTests(TestCase):
    def setUp(self) -> None:
        self.group = {
            "sector": "Technology",
            "direction": "Bullish",
            "strength": 72,
            "raw_score": 0.4,
        }
        self.stock = {
            "weekly_return": 0.06,
            "fundamentals": {
                "trailingPE": 13,
                "priceToBook": 1.6,
                "pegRatio": 0.8,
                "dividendYield": 2.4,
            },
        }

    def test_default_weights_exactly_match_standard_model(self) -> None:
        standard = recommendation_for_stock(self.group, self.stock)
        personalized = personalized_recommendation(self.group, self.stock)

        self.assertEqual(personalized["action"], standard["action"])
        self.assertEqual(personalized["score"], standard["score"])
        self.assertEqual(personalized["confidence"], standard["confidence"])
        self.assertEqual(personalized["rationale"], standard["rationale"])

    def test_custom_weights_change_score_without_changing_standard_input(self) -> None:
        standard = recommendation_for_stock(self.group, self.stock)
        personalized = personalized_recommendation(
            self.group,
            self.stock,
            {
                "group_sentiment_weight": 0,
                "pe_weight": 0,
                "pb_weight": 0,
                "peg_weight": 0,
                "dividend_weight": 0,
                "momentum_weight": 0.1,
            },
        )

        self.assertEqual(standard["action"], "Buy")
        self.assertEqual(personalized["action"], "Hold")
        self.assertEqual(personalized["score"], 0.025)
        self.assertEqual(personalized["factor_contributions"]["momentum_weight"], 0.025)


class PersonalizationMigrationTests(TestCase):
    def test_new_user_tables_enable_rls_and_limit_client_roles(self) -> None:
        migration = (
            ROOT
            / "supabase/migrations/20260905154101_named_watchlists_and_scoring_weights.sql"
        ).read_text(encoding="utf-8")

        for table in (
            "user_watchlists",
            "user_watchlist_items",
            "user_scoring_weights",
        ):
            self.assertIn(f"alter table public.{table} enable row level security", migration)
            self.assertIn(f"revoke all on table public.{table} from anon, authenticated", migration)
        self.assertIn("(select auth.uid()) = user_id", migration)
