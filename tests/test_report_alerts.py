from datetime import UTC, datetime
from pathlib import Path
import sys
from unittest import TestCase

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "apps" / "api"))
from app.routes.report import _expected_update_checkpoint, _report_alert


class ReportAlertTests(TestCase):
    def test_calendar_checkpoint_uses_latest_due_nyse_update(self):
        checkpoint = _expected_update_checkpoint(datetime(2026, 7, 6, 22, 0, tzinfo=UTC))
        self.assertEqual(checkpoint, datetime(2026, 7, 6, 20, 10, tzinfo=UTC))

    def test_newer_failure_takes_precedence_over_staleness(self):
        update = {
            "status": "success",
            "started_at": datetime(2026, 7, 6, 13, 30, tzinfo=UTC),
            "completed_at": datetime(2026, 7, 6, 13, 40, tzinfo=UTC),
        }
        attempt = {
            "status": "failed",
            "started_at": datetime(2026, 7, 6, 20, 10, tzinfo=UTC),
        }
        alert = _report_alert(update, attempt, datetime(2026, 7, 6, 22, 0, tzinfo=UTC))
        self.assertEqual(alert["kind"], "failed")

    def test_partial_and_stale_updates_are_reported(self):
        now = datetime(2026, 7, 6, 22, 0, tzinfo=UTC)
        partial = {
            "status": "partial",
            "started_at": datetime(2026, 7, 6, 20, 10, tzinfo=UTC),
            "completed_at": datetime(2026, 7, 6, 20, 20, tzinfo=UTC),
        }
        self.assertEqual(_report_alert(partial, partial, now)["kind"], "partial")

        stale = {**partial, "status": "success", "completed_at": datetime(2026, 7, 6, 14, 0, tzinfo=UTC)}
        self.assertEqual(_report_alert(stale, stale, now)["kind"], "stale")

        fresh = {**stale, "completed_at": datetime(2026, 7, 6, 20, 20, tzinfo=UTC)}
        self.assertIsNone(_report_alert(fresh, fresh, now))
