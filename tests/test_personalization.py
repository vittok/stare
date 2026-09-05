from __future__ import annotations

import sys
from pathlib import Path
from unittest import TestCase

from pydantic import ValidationError

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "apps/api"))

from app.routes.personalization import (  # noqa: E402
    ScoringWeightsPayload,
    WatchlistCreate,
    WatchlistUpdate,
)


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
