from datetime import UTC, datetime
from pathlib import Path
import sys
from unittest import TestCase, mock

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import update_alerts


class UpdateAlertTests(TestCase):
    def test_checkpoint_respects_open_close_grace_and_market_holidays(self):
        after_open_grace = update_alerts.expected_update_checkpoint(
            datetime(2026, 7, 6, 15, 0, tzinfo=UTC)
        )
        self.assertEqual(after_open_grace, datetime(2026, 7, 6, 13, 35, tzinfo=UTC))

        holiday = update_alerts.expected_update_checkpoint(
            datetime(2026, 7, 3, 23, 0, tzinfo=UTC)
        )
        self.assertEqual(holiday, datetime(2026, 7, 2, 20, 10, tzinfo=UTC))

    def test_stale_details_compare_latest_completion_to_checkpoint(self):
        now = datetime(2026, 7, 6, 22, 0, tzinfo=UTC)
        stale = update_alerts.stale_details(
            ("success", datetime(2026, 7, 6, 20, 0, tzinfo=UTC)), now
        )
        self.assertIn("2026-07-06T20:10:00+00:00", stale or "")
        self.assertIsNone(update_alerts.stale_details(
            ("success", datetime(2026, 7, 6, 20, 20, tzinfo=UTC)), now
        ))

    def test_freshness_check_sends_only_when_stale(self):
        with mock.patch.dict(update_alerts.os.environ, {"DATABASE_URL": "postgresql://test"}), \
             mock.patch.object(update_alerts, "latest_successful_update") as latest, \
             mock.patch.object(update_alerts, "stale_details") as details, \
             mock.patch.object(update_alerts, "send_alert") as send:
            latest.return_value = ("success", datetime.now(UTC))
            details.return_value = None
            self.assertFalse(update_alerts.check_freshness())
            send.assert_not_called()

            details.return_value = "Update is late."
            self.assertTrue(update_alerts.check_freshness())
            send.assert_called_once_with("Market data is stale", "Update is late.")

    def test_alert_recipient_defaults_to_sender(self):
        with mock.patch.dict(update_alerts.os.environ, {
            "SMTP_FROM": "owner@example.test",
            "STARE_ALERT_TO": "",
        }, clear=True):
            self.assertEqual(update_alerts._recipients(), ["owner@example.test"])

    def test_workflows_wire_failure_and_freshness_alerts(self):
        root = Path(__file__).resolve().parents[1]
        refresh = yaml.safe_load((root / ".github/workflows/pipeline_weekdays.yml").read_text())
        steps = refresh["jobs"]["run"]["steps"]
        failure = next(step for step in steps if step["name"] == "Send update failure alert")
        self.assertIn("failure()", failure["if"])
        self.assertEqual(failure["env"]["STARE_ALERT_TO"], "${{ secrets.STARE_ALERT_TO }}")

        freshness = yaml.safe_load((root / ".github/workflows/data_freshness.yml").read_text())
        check = freshness["jobs"]["check"]["steps"][-1]
        self.assertEqual(check["name"], "Check latest successful update")
        self.assertEqual(check["run"], "python src/update_alerts.py stale")
