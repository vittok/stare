from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path
from unittest import TestCase, mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import run_render_scheduled_update


class RenderScheduleTests(TestCase):
    def test_market_open_selects_correct_daylight_saving_occurrence(self) -> None:
        summer_active = datetime(2026, 7, 6, 13, 35, tzinfo=UTC)
        summer_inactive = datetime(2026, 7, 6, 14, 35, tzinfo=UTC)
        winter_inactive = datetime(2026, 1, 5, 13, 35, tzinfo=UTC)
        winter_active = datetime(2026, 1, 5, 14, 35, tzinfo=UTC)

        self.assertTrue(run_render_scheduled_update.is_scheduled_window("market-open", summer_active))
        self.assertFalse(run_render_scheduled_update.is_scheduled_window("market-open", summer_inactive))
        self.assertFalse(run_render_scheduled_update.is_scheduled_window("market-open", winter_inactive))
        self.assertTrue(run_render_scheduled_update.is_scheduled_window("market-open", winter_active))

    def test_market_close_selects_correct_daylight_saving_occurrence(self) -> None:
        summer_active = datetime(2026, 7, 6, 20, 10, tzinfo=UTC)
        summer_inactive = datetime(2026, 7, 6, 21, 10, tzinfo=UTC)
        winter_inactive = datetime(2026, 1, 5, 20, 10, tzinfo=UTC)
        winter_active = datetime(2026, 1, 5, 21, 10, tzinfo=UTC)

        self.assertTrue(run_render_scheduled_update.is_scheduled_window("market-close", summer_active))
        self.assertFalse(run_render_scheduled_update.is_scheduled_window("market-close", summer_inactive))
        self.assertFalse(run_render_scheduled_update.is_scheduled_window("market-close", winter_inactive))
        self.assertTrue(run_render_scheduled_update.is_scheduled_window("market-close", winter_active))

    def test_inactive_occurrence_does_not_start_pipeline(self) -> None:
        inactive = datetime(2026, 7, 6, 14, 35, tzinfo=UTC)
        with mock.patch.object(run_render_scheduled_update.run_pipeline, "main") as pipeline:
            result = run_render_scheduled_update.run_update("market-open", inactive)

        self.assertEqual(result, 0)
        pipeline.assert_not_called()

    def test_monday_close_refreshes_fundamentals_and_sends_email(self) -> None:
        monday_close = datetime(2026, 7, 6, 20, 10, tzinfo=UTC)
        with (
            mock.patch.object(run_render_scheduled_update.run_pipeline, "main", return_value=0) as pipeline,
            mock.patch.object(run_render_scheduled_update, "smtp_is_configured", return_value=True),
            mock.patch.object(run_render_scheduled_update.email_stare_report, "send_email") as send_email,
        ):
            result = run_render_scheduled_update.run_update("market-close", monday_close)

        self.assertEqual(result, 0)
        pipeline.assert_called_once_with(
            [
                "--postgres-mode",
                "required",
                "--run-label",
                "market-close",
                "--with-fundamentals",
            ]
        )
        send_email.assert_called_once_with()

    def test_pipeline_failure_prevents_success_email(self) -> None:
        open_time = datetime(2026, 7, 6, 13, 35, tzinfo=UTC)
        with (
            mock.patch.object(run_render_scheduled_update.run_pipeline, "main", return_value=7),
            mock.patch.object(run_render_scheduled_update.email_stare_report, "send_email") as send_email,
        ):
            result = run_render_scheduled_update.run_update("market-open", open_time)

        self.assertEqual(result, 7)
        send_email.assert_not_called()
