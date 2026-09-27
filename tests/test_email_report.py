import copy
from pathlib import Path
import sys
from unittest import TestCase, mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import email_stare_report as email


def report():
    stock = {"ticker": "TEST", "rank": 1, "currentPrice": 100,
             "dollar_vol_latest": 1000, "recommendation": {"action": "Hold"},
             "fundamentals": {"currency": "USD"}}
    return {"last_refresh": {"display": "2026-09-25 20:10 UTC"},
            "market_data": {"latest_price_date": "2026-09-25"},
            "sectors": [{"sector": "Technology", "strength": 40, "direction": "Neutral",
                         "top10_active": [stock]}], "regions": []}


class EmailReportTests(TestCase):
    def test_comparison_includes_signals_strength_picks_and_price(self):
        old = report()
        new = copy.deepcopy(old)
        group = new["sectors"][0]
        group.update(strength=60, direction="Bullish")
        group["top10_active"][0].update(currentPrice=110, recommendation={"action": "Buy"})
        group["top10_active"].append({"ticker": "NEW", "rank": 2})
        changes = "\n".join(email._top_changes(new, old))
        for expected in ("Hold to Buy", "40 to 60", "+20 points", "added NEW", "+10.00%", "USD"):
            self.assertIn(expected, changes)

    def test_first_update_and_identical_report(self):
        self.assertIn("No previous report", email._top_changes(report(), None)[0])
        self.assertIn("No material changes", email._top_changes(report(), report())[0])

    def test_missing_zero_nonfinite_and_changed_currency_not_price_moves(self):
        for value in (None, 0, float("nan"), float("inf")):
            old, new = report(), report()
            old["sectors"][0]["top10_active"][0]["currentPrice"] = value
            self.assertFalse(any(line.startswith("Price:") for line in email._top_changes(new, old)))
        old, new = report(), report()
        new["sectors"][0]["top10_active"][0].update(currentPrice=200, fundamentals={"currency": "EUR"})
        self.assertFalse(any(line.startswith("Price:") for line in email._top_changes(new, old)))

    def test_na_duplicates_and_international_stocks(self):
        old, new = report(), report()
        for data, action in ((old, "Hold"), (new, "Buy")):
            stock = data["sectors"][0]["top10_active"][0]
            stock["recommendation"]["action"] = action
            data["regions"] = [
                {"region": "NA", "top10_active": [{**stock, "market": "S&P 500"}]},
                {"region": "APAC", "markets": [{"market": "Japan", "top10_active": [
                    {**stock, "ticker": "JAPAN", "fundamentals": {"currency": "JPY"}}
                ]}]},
            ]
        changes = email._top_changes(new, old)
        self.assertEqual(sum("Signal: TEST" in line for line in changes), 1)
        self.assertTrue(any("JAPAN (APAC)" in line for line in changes))

    def test_status_text_and_html_escape(self):
        old, new = report(), report()
        new["sectors"][0]["top10_active"][0]["recommendation"]["action"] = "<script>"
        with mock.patch.dict(email.os.environ, {"STARE_UPDATE_STATUS": "success"}):
            html = email._build_html_email(new, old)
            text = email._build_text_email(new, old)
        for body in (html, text):
            self.assertIn("Update status: Successful", body)
            self.assertIn("2026-09-25 20:10 UTC", body)
            self.assertIn("not previous-day closes", body)
        self.assertIn("&lt;script&gt;", html)
        self.assertNotIn("<script>", html)

    def test_unsupplied_status_is_not_success(self):
        with mock.patch.dict(email.os.environ, {}, clear=True):
            self.assertIn("Not supplied", email._update_summary(report(), None)[0])

    def test_smtp_message_uses_baseline_and_tolerates_missing_history(self):
        env = {"SMTP_HOST": "smtp.test", "SMTP_FROM": "sender@example.test", "SMTP_AUTH": "false",
               "STARE_EMAIL_TO": "reader@example.test", "STARE_UPDATE_STATUS": "success",
               "STARE_PREVIOUS_APP_HTML": "/tmp/previous.html"}
        old, new = report(), report()
        new["sectors"][0]["top10_active"][0]["currentPrice"] = 110
        for baseline, expected in ((old, "+10.00%"), (FileNotFoundError(), "No previous report")):
            with mock.patch.dict(email.os.environ, env, clear=True), mock.patch.object(
                email, "_load_app_data", side_effect=[new, baseline]
            ) as load, mock.patch.object(email.smtplib, "SMTP") as smtp:
                email.send_email()
                self.assertEqual(load.call_args_list[1].args, (Path("/tmp/previous.html"),))
                message = smtp.return_value.__enter__.return_value.send_message.call_args.args[0]
                for content_type in ("plain", "html"):
                    body = message.get_body(preferencelist=(content_type,)).get_content()
                    self.assertIn("Update status: Successful", body)
                    self.assertIn(expected, body)

    def test_workflow_captures_baseline_before_calculation_and_verifies_before_email(self):
        import yaml
        root = Path(__file__).resolve().parents[1]
        workflow = yaml.safe_load((root / ".github/workflows/pipeline_weekdays.yml").read_text())
        steps = workflow["jobs"]["run"]["steps"]
        names = [step["name"] for step in steps]
        self.assertLess(names.index("Preserve previous report for email comparison"),
                        names.index("Calculate and publish market update"))
        self.assertLess(names.index("Verify published portal and Pages data"), names.index("Send app update email"))
        env = steps[names.index("Send app update email")]["env"]
        self.assertEqual(env["STARE_UPDATE_STATUS"], "success")
        self.assertIn("runner.temp", env["STARE_PREVIOUS_APP_HTML"])
