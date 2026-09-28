import json
from pathlib import Path
import sys
from unittest import TestCase

from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "apps" / "api"))
from app.main import app


class RequestLoggingTests(TestCase):
    def test_logs_structured_request_without_sensitive_values(self):
        request_id = "test-request-123"
        with self.assertLogs("stare.requests", level="INFO") as captured:
            response = TestClient(app).get(
                "/?token=do-not-log",
                headers={"authorization": "Bearer do-not-log", "x-request-id": request_id},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["x-request-id"], request_id)
        payload = json.loads(captured.records[-1].getMessage())
        self.assertEqual(payload["event"], "http_request")
        self.assertEqual(payload["request_id"], request_id)
        self.assertEqual(payload["method"], "GET")
        self.assertEqual(payload["path"], "/")
        self.assertEqual(payload["status"], 200)
        self.assertGreaterEqual(payload["duration_ms"], 0)
        self.assertNotIn("do-not-log", captured.output[0])

    def test_replaces_unsafe_request_id(self):
        with self.assertLogs("stare.requests", level="INFO") as captured:
            response = TestClient(app).get("/missing", headers={"x-request-id": "unsafe value"})

        request_id = response.headers["x-request-id"]
        self.assertRegex(request_id, r"^[a-f0-9]{32}$")
        payload = json.loads(captured.records[-1].getMessage())
        self.assertEqual(payload["request_id"], request_id)
        self.assertEqual(payload["status"], 404)
