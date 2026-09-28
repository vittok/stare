"""Send operational alerts for failed or stale S.T.A.R.E market updates."""

from __future__ import annotations

import argparse
import html
import os
import smtplib
import ssl
from datetime import UTC, datetime, timedelta
from email.message import EmailMessage
from email.utils import parseaddr

import pandas_market_calendars as mcal


ALERT_GRACE = timedelta(minutes=75)


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def expected_update_checkpoint(now: datetime | None = None) -> datetime | None:
    current = _utc(now or datetime.now(UTC))
    schedule = mcal.get_calendar("NYSE").schedule(
        start_date=(current - timedelta(days=14)).date(),
        end_date=current.date(),
    )
    checkpoints: list[datetime] = []
    for _, session in schedule.iterrows():
        for column, delay in (("market_open", 5), ("market_close", 10)):
            checkpoint = _utc(session[column].to_pydatetime()) + timedelta(minutes=delay)
            if current >= checkpoint + ALERT_GRACE:
                checkpoints.append(checkpoint)
    return max(checkpoints) if checkpoints else None


def latest_successful_update(database_url: str) -> tuple[str, datetime] | None:
    import psycopg

    if database_url.startswith("postgresql+psycopg://"):
        database_url = database_url.replace("postgresql+psycopg://", "postgresql://", 1)
    query = """
        select status, completed_at
        from public.update_runs
        where status in ('success', 'partial') and completed_at is not null
        order by completed_at desc
        limit 1
    """
    with psycopg.connect(database_url) as connection:
        row = connection.execute(query).fetchone()
    return (str(row[0]), _utc(row[1])) if row else None


def stale_details(latest: tuple[str, datetime] | None, now: datetime | None = None) -> str | None:
    checkpoint = expected_update_checkpoint(now)
    if checkpoint is None:
        return None
    if latest and latest[1] >= checkpoint:
        return None
    completed = latest[1].isoformat() if latest else "no successful update"
    return (
        f"Latest successful completion: {completed}. "
        f"Expected an update at or after {checkpoint.isoformat()}."
    )


def _recipients() -> list[str]:
    configured = _env("STARE_ALERT_TO") or _env("SMTP_FROM")
    candidates = [item.strip() for item in configured.replace(";", ",").split(",")]
    recipients = []
    for candidate in candidates:
        _, address = parseaddr(candidate)
        if address == candidate and "@" in address:
            recipients.append(candidate)
    return recipients


def send_alert(title: str, details: str) -> None:
    host = _env("SMTP_HOST")
    username = _env("SMTP_USERNAME")
    password = _env("SMTP_PASSWORD")
    sender = _env("SMTP_FROM", username)
    recipients = _recipients()
    missing = [name for name, value in {
        "SMTP_HOST": host,
        "SMTP_USERNAME": username,
        "SMTP_PASSWORD": password,
        "SMTP_FROM": sender,
        "STARE_ALERT_TO or SMTP_FROM": recipients,
    }.items() if not value]
    if missing:
        raise RuntimeError(f"Missing alert configuration: {', '.join(missing)}")

    try:
        port = int(_env("SMTP_PORT", "587"))
    except ValueError as exc:
        raise RuntimeError("SMTP_PORT must be a valid integer") from exc

    app_url = _env("STARE_APP_URL", "https://stare-portal.onrender.com")
    message = EmailMessage()
    message["Subject"] = f"S.T.A.R.E alert: {title}"
    message["From"] = sender
    message["To"] = ", ".join(recipients)
    message.set_content(f"S.T.A.R.E operational alert\n\n{title}\n{details}\n\nPortal: {app_url}\n")
    message.add_alternative(
        "<h1>S.T.A.R.E operational alert</h1>"
        f"<h2>{html.escape(title)}</h2>"
        f"<p>{html.escape(details)}</p>"
        f'<p><a href="{html.escape(app_url)}">Open the portal</a></p>',
        subtype="html",
    )

    context = ssl.create_default_context()
    with smtplib.SMTP(host, port) as server:
        server.starttls(context=context)
        server.login(username, password)
        server.send_message(message)
    print(f"Sent S.T.A.R.E operational alert to {len(recipients)} recipient(s).")


def check_freshness() -> bool:
    database_url = _env("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is required for the freshness check")
    latest = latest_successful_update(database_url)
    details = stale_details(latest)
    if details is None:
        print("Latest S.T.A.R.E update is fresh for the current NYSE checkpoint.")
        return False
    send_alert("Market data is stale", details)
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("kind", choices=("failure", "stale"))
    args = parser.parse_args()
    if args.kind == "failure":
        details = _env("STARE_ALERT_DETAILS", "The market refresh workflow did not complete successfully.")
        send_alert("Market update failed", details)
        return 0

    try:
        check_freshness()
        return 0
    except Exception as exc:
        send_alert("Freshness monitor failed", f"The freshness check could not complete: {exc}")
        raise


if __name__ == "__main__":
    raise SystemExit(main())
