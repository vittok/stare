from __future__ import annotations

import html
import json
import os
import re
import smtplib
import ssl
from email.message import EmailMessage
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
APP_HTML = ROOT / "stare_app.html"
DATA_RE = re.compile(
    r'<script id="stare-data" type="application/json">(.*?)</script>',
    re.DOTALL,
)


def _env(name: str, default: str = "") -> str:
    value = os.environ.get(name)
    if value is None or not value.strip():
        return default
    return value.strip()


def _env_bool(name: str, default: bool) -> bool:
    value = _env(name)
    if not value:
        return default
    return value.lower() in {"1", "true", "yes", "on"}


def _recipients(value: str) -> list[str]:
    recipients = [item.strip() for item in value.replace(";", ",").split(",")]
    return [item for item in recipients if item]


def _num(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        n = float(value)
        return n if n == n and n not in {float("inf"), float("-inf")} else None
    except (TypeError, ValueError):
        return None


def _fmt_price(value: Any) -> str:
    n = _num(value)
    return "n/a" if n is None else f"${n:,.2f}"


def _fmt_pct(value: Any) -> str:
    n = _num(value)
    if n is None:
        return "n/a"
    return f"{n * 100:.2f}%"


def _fmt_big(value: Any) -> str:
    n = _num(value)
    if n is None:
        return "n/a"
    for suffix, divisor in (("T", 1e12), ("B", 1e9), ("M", 1e6), ("K", 1e3)):
        if abs(n) >= divisor:
            return f"{n / divisor:.2f}{suffix}"
    return f"{n:,.0f}"


def _load_app_data() -> dict[str, Any]:
    raw_html = APP_HTML.read_text(encoding="utf-8")
    match = DATA_RE.search(raw_html)
    if not match:
        raise RuntimeError(f"Could not find embedded STARE data block in {APP_HTML}")
    return json.loads(match.group(1))


def _flatten_rows(data: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for sector in data.get("sectors", []):
        for stock in sector.get("top10_active", []):
            fundamentals = stock.get("fundamentals") or {}
            recommendation = stock.get("recommendation") or {}
            rows.append(
                {
                    "sector": sector.get("sector"),
                    "direction": sector.get("direction"),
                    "strength": sector.get("strength"),
                    "rank": stock.get("rank"),
                    "ticker": stock.get("ticker"),
                    "price": stock.get("currentPrice"),
                    "price_date": stock.get("priceDate"),
                    "previous_close": stock.get("previousClose"),
                    "previous_close_date": stock.get("previousCloseDate"),
                    "close_change": stock.get("closeChange"),
                    "close_change_pct": stock.get("closeChangePct"),
                    "close_direction": stock.get("closeDirection"),
                    "signal": recommendation.get("action", "Hold"),
                    "confidence": recommendation.get("confidence"),
                    "volume_date": stock.get("volume_date"),
                    "dollar_vol_latest": stock.get("dollar_vol_latest"),
                    "latest_volume": stock.get("latest_volume"),
                    "weekly_return": stock.get("weekly_return"),
                    "dollar_vol_week": stock.get("dollar_vol_week"),
                    "name": fundamentals.get("shortName") or fundamentals.get("industry") or "",
                    "summary": stock.get("daily_summary") or "",
                }
            )
    return rows


def _sector_table(data: dict[str, Any]) -> str:
    rows = sorted(
        data.get("sectors", []),
        key=lambda s: (_num(s.get("strength")) or 0),
        reverse=True,
    )
    body = "\n".join(
        "<tr>"
        f"<td>{html.escape(str(s.get('sector') or ''))}</td>"
        f"<td>{html.escape(str(s.get('direction') or ''))}</td>"
        f"<td style=\"text-align:right\">{html.escape(str(s.get('strength') or ''))}</td>"
        f"<td style=\"text-align:right\">{_fmt_pct((s.get('diagnostics') or {}).get('median_return'))}</td>"
        "</tr>"
        for s in rows
    )
    return (
        "<table>"
        "<thead><tr><th>Sector</th><th>Direction</th><th>Strength</th><th>Median Return</th></tr></thead>"
        f"<tbody>{body}</tbody></table>"
    )


def _stock_table(rows: list[dict[str, Any]]) -> str:
    sorted_rows = sorted(
        rows,
        key=lambda r: (_num(r.get("dollar_vol_latest")) or 0),
        reverse=True,
    )
    body = "\n".join(
        "<tr>"
        f"<td>{html.escape(str(r.get('sector') or ''))}</td>"
        f"<td>{html.escape(str(r.get('ticker') or ''))}</td>"
        f"<td>{html.escape(str(r.get('name') or ''))}</td>"
        f"<td style=\"text-align:right\">{_fmt_price(r.get('price'))}</td>"
        f"<td style=\"text-align:right\">{_fmt_price(r.get('previous_close'))}</td>"
        f"<td>{html.escape(str(r.get('close_direction') or 'unknown'))}</td>"
        f"<td>{html.escape(str(r.get('signal') or ''))}</td>"
        f"<td style=\"text-align:right\">{html.escape(str(r.get('confidence') or 'n/a'))}</td>"
        f"<td style=\"text-align:right\">{_fmt_pct(r.get('weekly_return'))}</td>"
        f"<td style=\"text-align:right\">{_fmt_big(r.get('dollar_vol_latest'))}</td>"
        "</tr>"
        for r in sorted_rows
    )
    return (
        "<table>"
        "<thead><tr><th>Sector</th><th>Ticker</th><th>Name</th><th>Price</th>"
        "<th>Previous Close</th><th>Close Direction</th><th>Signal</th><th>Confidence</th>"
        "<th>Weekly</th><th>Latest Day Dollar Volume</th></tr></thead>"
        f"<tbody>{body}</tbody></table>"
    )


def _sector_pick_summaries(data: dict[str, Any]) -> str:
    sectors = sorted(
        data.get("sectors", []),
        key=lambda s: (_num(s.get("strength")) or 0),
        reverse=True,
    )
    sections = []
    for sector in sectors:
        picks = sector.get("top3_explanations") or []
        if not picks:
            picks = [
                {
                    "rank": stock.get("rank"),
                    "ticker": stock.get("ticker"),
                    "summary": stock.get("daily_summary"),
                }
                for stock in (sector.get("top10_active") or [])[:3]
            ]
        if not picks:
            continue
        items = "\n".join(
            f"<li><strong>{html.escape(str(pick.get('ticker') or ''))}</strong>: "
            f"{html.escape(str(pick.get('summary') or 'No generated summary.'))}</li>"
            for pick in picks[:3]
        )
        sections.append(
            f"<h3>{html.escape(str(sector.get('sector') or 'Sector'))} "
            f"({html.escape(str(sector.get('direction') or 'Neutral'))}, "
            f"strength {html.escape(str(sector.get('strength') or 'n/a'))})</h3>"
            f"<ol>{items}</ol>"
        )
    return "\n".join(sections) or "<p>No generated sector pick summaries are available.</p>"


def _build_html_email(data: dict[str, Any]) -> str:
    rows = _flatten_rows(data)
    refresh = data.get("last_refresh") or {}
    market_data = data.get("market_data") or {}
    app_url = _env("STARE_APP_URL", "https://vittok.github.io/stare/")
    refresh_display = html.escape(str(refresh.get("display") or refresh.get("iso_utc") or "n/a"))
    market_date = html.escape(str(market_data.get("latest_price_date") or "n/a"))
    return f"""\
<!doctype html>
<html>
  <head>
    <meta charset="utf-8">
    <style>
      body {{ font-family: Arial, sans-serif; color: #1f2933; line-height: 1.45; }}
      h1, h2 {{ color: #111827; }}
      table {{ border-collapse: collapse; width: 100%; margin: 12px 0 22px; font-size: 13px; }}
      th, td {{ border: 1px solid #d9dee3; padding: 7px 8px; vertical-align: top; }}
      th {{ background: #f2f5f7; text-align: left; }}
      .meta {{ color: #52616b; }}
    </style>
  </head>
  <body>
    <h1>S.T.A.R.E Daily Report</h1>
    <p class="meta">Last refresh: {refresh_display}</p>
    <p class="meta">Market data date: {market_date}</p>
    <p><a href="{html.escape(app_url)}">Open the published S.T.A.R.E dashboard</a></p>

    <h2>Sector Overview</h2>
    {_sector_table(data)}

    <h2>Sector Pick Summaries</h2>
    {_sector_pick_summaries(data)}

    <h2>Updated Stock Report</h2>
    {_stock_table(rows)}

    <p class="meta">Signals are deterministic model outputs for research and monitoring, not personalized financial advice.</p>
  </body>
</html>
"""


def _build_text_email(data: dict[str, Any]) -> str:
    rows = _flatten_rows(data)
    refresh = data.get("last_refresh") or {}
    market_data = data.get("market_data") or {}
    app_url = _env("STARE_APP_URL", "https://vittok.github.io/stare/")
    lines = [
        "S.T.A.R.E Daily Report",
        f"Last refresh: {refresh.get('display') or refresh.get('iso_utc') or 'n/a'}",
        f"Market data date: {market_data.get('latest_price_date') or 'n/a'}",
        f"Dashboard: {app_url}",
        "",
        "Sector overview:",
    ]
    for sector in sorted(data.get("sectors", []), key=lambda s: (_num(s.get("strength")) or 0), reverse=True):
        lines.append(f"- {sector.get('sector')}: {sector.get('direction')} strength {sector.get('strength')}")
    lines.extend(["", "Sector pick summaries:"])
    for sector in sorted(data.get("sectors", []), key=lambda s: (_num(s.get("strength")) or 0), reverse=True):
        picks = sector.get("top3_explanations") or []
        if not picks:
            picks = [
                {
                    "ticker": stock.get("ticker"),
                    "summary": stock.get("daily_summary"),
                }
                for stock in (sector.get("top10_active") or [])[:3]
            ]
        if not picks:
            continue
        lines.append(f"- {sector.get('sector')}: {sector.get('direction')} strength {sector.get('strength')}")
        for pick in picks[:3]:
            lines.append(f"  - {pick.get('ticker')}: {pick.get('summary') or 'No generated summary.'}")

    lines.extend(["", "Global latest-day liquidity table preview:"])
    for row in sorted(rows, key=lambda r: (_num(r.get("dollar_vol_latest")) or 0), reverse=True)[:20]:
        lines.append(
            f"- {row.get('ticker')} {row.get('signal')} "
            f"{_fmt_price(row.get('price'))}, previous close {_fmt_price(row.get('previous_close'))} "
            f"({row.get('close_direction') or 'unknown'}), weekly {_fmt_pct(row.get('weekly_return'))}: "
            f"{row.get('summary')}"
        )
    lines.append("")
    lines.append("Signals are deterministic model outputs for research, not personalized financial advice.")
    return "\n".join(lines)


def send_email() -> None:
    smtp_host = _env("SMTP_HOST")
    try:
        smtp_port = int(_env("SMTP_PORT", "587"))
    except ValueError as exc:
        raise RuntimeError("SMTP_PORT must be a valid integer") from exc
    smtp_username = _env("SMTP_USERNAME")
    smtp_password = _env("SMTP_PASSWORD")
    smtp_from = _env("SMTP_FROM", smtp_username)
    recipients = _recipients(_env("STARE_EMAIL_TO", "vittok@hotmail.com"))
    require_auth = _env_bool("SMTP_AUTH", True)

    required = {"SMTP_HOST": smtp_host, "SMTP_FROM": smtp_from}
    if require_auth:
        required.update(
            {
                "SMTP_USERNAME": smtp_username,
                "SMTP_PASSWORD": smtp_password,
            }
        )
    missing = [name for name, value in required.items() if not value]
    if missing:
        raise RuntimeError(f"Missing required email environment variables: {', '.join(missing)}")
    if not recipients:
        raise RuntimeError("STARE_EMAIL_TO must contain at least one recipient")

    data = _load_app_data()
    refresh = data.get("last_refresh") or {}
    subject_date = refresh.get("display") or refresh.get("iso_utc") or "latest refresh"
    refresh_label = _env("STARE_REFRESH_LABEL", "app update")

    msg = EmailMessage()
    msg["Subject"] = f"S.T.A.R.E Update ({refresh_label}) - {subject_date}"
    msg["From"] = smtp_from
    msg["To"] = ", ".join(recipients)
    msg.set_content(_build_text_email(data))
    msg.add_alternative(_build_html_email(data), subtype="html")

    use_ssl = _env_bool("SMTP_SSL", False)
    use_starttls = _env_bool("SMTP_STARTTLS", True)
    context = ssl.create_default_context()

    if use_ssl:
        with smtplib.SMTP_SSL(smtp_host, smtp_port, context=context) as server:
            if require_auth:
                server.login(smtp_username, smtp_password)
            server.send_message(msg)
    else:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            if use_starttls:
                server.starttls(context=context)
            if require_auth:
                server.login(smtp_username, smtp_password)
            server.send_message(msg)

    print(f"Sent STARE report email to {', '.join(recipients)}")


if __name__ == "__main__":
    send_email()
