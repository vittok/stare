from __future__ import annotations

import argparse
import os
import sys
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

import email_stare_report
import run_pipeline


NEW_YORK = ZoneInfo("America/New_York")
WINDOWS = {
    "market-open": (9, 35),
    "market-close": (16, 10),
}


def is_scheduled_window(window: str, now: datetime | None = None) -> bool:
    current = (now or datetime.now(UTC)).astimezone(NEW_YORK)
    hour, minute = WINDOWS[window]
    return current.weekday() < 5 and current.hour == hour and current.minute >= minute


def should_refresh_fundamentals(window: str, now: datetime | None = None) -> bool:
    current = (now or datetime.now(UTC)).astimezone(NEW_YORK)
    return window == "market-close" and current.weekday() == 0


def smtp_is_configured() -> bool:
    required = ("SMTP_HOST", "SMTP_USERNAME", "SMTP_PASSWORD", "SMTP_FROM")
    return all(os.getenv(name, "").strip() for name in required)


def run_update(window: str, now: datetime | None = None, force: bool = False) -> int:
    current = now or datetime.now(UTC)
    local_now = current.astimezone(NEW_YORK)
    print(f"Render schedule check: {local_now.isoformat(timespec='seconds')}")
    print(f"Requested update window: {window}")

    if not force and not is_scheduled_window(window, current):
        print("Inactive daylight-saving schedule occurrence; no update is required.")
        return 0

    os.environ["STARE_REFRESH_LABEL"] = window
    pipeline_args = [
        "--postgres-mode",
        "required",
        "--run-label",
        window,
    ]
    if should_refresh_fundamentals(window, current):
        pipeline_args.append("--with-fundamentals")

    result = run_pipeline.main(pipeline_args)
    if result != 0:
        return result

    if smtp_is_configured():
        email_stare_report.send_email()
    else:
        print("SMTP notification skipped because its Render secrets are not configured.")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run a Render-scheduled market update.")
    parser.add_argument("--window", choices=tuple(WINDOWS), required=True)
    parser.add_argument(
        "--force",
        action="store_true",
        help="Bypass the New York schedule guard for a controlled manual test.",
    )
    args = parser.parse_args(argv)
    return run_update(args.window, force=args.force)


if __name__ == "__main__":
    sys.exit(main())
