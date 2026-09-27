from pathlib import Path
import sys
from unittest import TestCase

from pydantic import ValidationError


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "apps" / "api"))

from app.routes.preferences import PreferencesPayload


class PreferenceValidationTests(TestCase):
    def test_email_report_defaults_are_opt_in(self):
        settings = PreferencesPayload().notification_settings.email_reports
        self.assertFalse(settings.enabled)
        self.assertEqual(settings.frequency, "market_close")
        self.assertEqual(settings.scope, "full")

    def test_email_report_options_are_validated(self):
        with self.assertRaises(ValidationError):
            PreferencesPayload(
                notification_settings={
                    "email_reports": {
                        "enabled": True,
                        "frequency": "hourly",
                        "scope": "full",
                    }
                }
            )
