from datetime import datetime
from pathlib import Path
import sys
from unittest import TestCase
from unittest import mock
import json

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from github_market_schedule import select_update
from verify_published_update import check_api
import verify_published_update


class GitHubScheduleTests(TestCase):
    def select(self, cron, instant, event="schedule"):
        return select_update(event, cron, datetime.fromisoformat(instant))

    def test_summer_and_winter_pairs(self):
        for day, open_hour, close_hour in (("2026-07-06", 13, 20), ("2026-01-05", 14, 21)):
            for minute, hour, label in ((35, open_hour, "market-open"), (10, close_hour, "market-close")):
                result = self.select(f"{minute} {hour} * * 1-5", f"{day}T23:00:00+00:00")
                self.assertEqual(result["should_run"], "true")
                self.assertEqual(result["label"], label)
                other_hour = hour + 1 if day.endswith("07-06") else hour - 1
                self.assertEqual(self.select(f"{minute} {other_hour} * * 1-5",
                                            f"{day}T23:00:00+00:00")["should_run"], "false")

    def test_holidays_and_weekend_skip(self):
        for day in ("2026-04-03", "2026-07-03", "2026-11-26", "2026-12-25", "2026-09-27"):
            self.assertEqual(self.select("10 20 * * 1-5", f"{day}T23:00:00+00:00")["label"],
                             "market-holiday")

    def test_early_close_replaces_normal_close(self):
        now = "2026-11-27T23:00:00+00:00"
        self.assertEqual(self.select("10 18 * * 1-5", now)["label"], "market-close")
        self.assertEqual(self.select("10 21 * * 1-5", now)["should_run"], "false")
        self.assertEqual(self.select("10 18 * * 1-5", "2026-11-30T23:00:00+00:00")["should_run"], "false")

    def test_first_session_after_monday_holiday_refreshes_fundamentals(self):
        self.assertEqual(self.select("10 20 * * 1-5", "2026-09-08T23:00:00+00:00")["with_fundamentals"], "true")
        self.assertEqual(self.select("10 20 * * 1-5", "2026-09-09T23:00:00+00:00")["with_fundamentals"], "false")

    def test_dst_transition_weeks(self):
        for day, hour in (("2026-03-06", 14), ("2026-03-09", 13), ("2026-10-30", 13), ("2026-11-02", 14)):
            self.assertEqual(self.select(f"35 {hour} * * 1-5", f"{day}T23:00:00+00:00")["should_run"], "true")

    def test_manual_bypasses_holiday_guard(self):
        self.assertEqual(self.select("", "2026-12-25T12:00:00+00:00", "workflow_dispatch")["label"], "manual")

    def test_not_before_target_or_for_unknown_events(self):
        self.assertEqual(self.select("35 13 * * 1-5", "2026-07-06T12:00:00+00:00")["should_run"], "false")
        self.assertEqual(self.select("", "2026-07-06T23:00:00+00:00", "push")["should_run"], "false")


class PublicationTests(TestCase):
    def test_rejects_old_snapshot_even_if_market_date_matches(self):
        report = {"market_data": {"latest_price_date": "2026-09-25"}, "sectors": [{}]}
        payload = {"update": {"id": "new", "status": "success", "latest_price_date": "2026-09-25"},
                   "sectors": [{}], "top_stocks": [{"ticker": "TEST"}]}
        check_api(payload, "new", report, report)
        with self.assertRaises(ValueError):
            check_api(payload, "old", report, report)
        payload["top_stocks"] = []
        with self.assertRaises(ValueError):
            check_api(payload, "new", report, report)

    def test_verifies_portal_and_pages_and_rejects_stale_html(self):
        root = verify_published_update.ROOT
        sector = json.loads((root / "docs/sector_dashboard.json").read_text())
        region = json.loads((root / "docs/region_dashboard.json").read_text())
        payload = {"update": {"id": "new-update-id", "status": "success",
                              "latest_price_date": max(r["market_data"]["latest_price_date"]
                                                       for r in (sector, region))},
                   "sectors": sector["sectors"], "top_stocks": [{"ticker": "TEST"}]}

        def response(data=None, text=""):
            result = mock.Mock()
            result.json.return_value = data
            result.text = text
            return result

        env = {"STARE_API_URL": "https://api.test", "STARE_PORTAL_URL": "https://portal.test",
               "STARE_PAGES_URL": "https://pages.test"}
        for html, expected_success in (((root / "docs/index.html").read_text(), True), ("stale", False)):
            responses = [response(payload), response(text="report: new-update-id"),
                         response(sector), response(region), response(text=html)]
            with mock.patch.dict(verify_published_update.os.environ, env), mock.patch.object(
                verify_published_update.requests, "get", side_effect=responses
            ):
                if expected_success:
                    verify_published_update.verify("new-update-id", attempts=1)
                else:
                    with self.assertRaisesRegex(ValueError, "Pages HTML"):
                        verify_published_update.verify("new-update-id", attempts=1)
