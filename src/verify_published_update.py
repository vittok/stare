"""Check that the API and Pages serve this update, not an older successful one."""

import json
import os
from pathlib import Path
import time

import requests

ROOT = Path(__file__).resolve().parents[1]


def check_api(payload: dict, update_id: str, sector: dict, region: dict) -> None:
    update = payload.get("update") or {}
    if update.get("id") != update_id or update.get("status") != "success":
        raise ValueError("API has not returned this successful update")
    expected_date = max(report["market_data"]["latest_price_date"]
                        for report in (sector, region))
    if update.get("latest_price_date") != expected_date:
        raise ValueError("API market date differs from generated reports")
    if len(payload.get("sectors", [])) != len(sector["sectors"]):
        raise ValueError("API sector coverage differs from generated reports")
    if not payload.get("top_stocks"):
        raise ValueError("API returned no stocks")


def verify(update_id: str, attempts: int = 12) -> None:
    if not update_id:
        raise ValueError("Missing expected Postgres update ID")
    reports = {name: json.loads((ROOT / "docs" / f"{name}_dashboard.json").read_text())
               for name in ("sector", "region")}
    expected_html = (ROOT / "docs/index.html").read_text(encoding="utf-8")
    api = os.environ["STARE_API_URL"].rstrip("/")
    pages = os.environ["STARE_PAGES_URL"].rstrip("/")
    for attempt in range(attempts):
        try:
            params = {"update": update_id, "attempt": attempt}
            response = requests.get(f"{api}/api/latest-report", params=params, timeout=90)
            response.raise_for_status()
            check_api(response.json(), update_id, reports["sector"], reports["region"])
            for name, expected in reports.items():
                response = requests.get(f"{pages}/{name}_dashboard.json", params=params, timeout=30)
                response.raise_for_status()
                if response.json() != expected:
                    raise ValueError(f"Pages {name} report is not this update")
            response = requests.get(f"{pages}/", params=params, timeout=30)
            response.raise_for_status()
            if response.text != expected_html:
                raise ValueError("Pages HTML is not this update")
            print(f"Verified API snapshot {update_id} and both Pages reports plus HTML.")
            return
        except (requests.RequestException, ValueError) as exc:
            print(f"Publication check {attempt + 1}/{attempts}: {exc}")
            if attempt + 1 == attempts:
                raise
            time.sleep(20)


if __name__ == "__main__":
    verify(os.getenv("EXPECTED_UPDATE_ID", ""))
