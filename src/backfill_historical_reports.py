"""Backfill missing market dates from Git's complete JSON report pairs."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
import json
from pathlib import Path
import subprocess
from tempfile import TemporaryDirectory

from sqlalchemy import create_engine, text

from export_reports_to_postgres import (
    _db_url, export_reports, resolve_database_url, validate_reports,
)

ROOT = Path(__file__).resolve().parents[1]
REPORTS = ("reports/sector_dashboard.json", "reports/region_dashboard.json")


@dataclass
class Snapshot:
    commit: str
    observed_at: datetime
    market_date: date
    sector: dict
    region: dict


def git(*args: str) -> str:
    return subprocess.check_output(["git", *args], cwd=ROOT, text=True)


def select_daily(snapshots: list[Snapshot], now: datetime, days: int) -> list[Snapshot]:
    if not 1 <= days <= 30:
        raise ValueError("Backfill is limited to the 30-day retention window")
    earliest_date = now.date() - timedelta(days=days - 1)
    earliest_time = now - timedelta(days=days)
    selected = {}
    for snapshot in sorted(snapshots, key=lambda item: (item.observed_at, item.commit)):
        if (earliest_time <= snapshot.observed_at <= now
                and earliest_date <= snapshot.market_date <= now.date()):
            selected[snapshot.market_date] = snapshot
    return [selected[day] for day in sorted(selected)]


def discover(now: datetime, days: int) -> list[Snapshot]:
    history = git("log", "--first-parent", f"--since={(now - timedelta(days=days)).isoformat()}",
                  "--format=%H %cI", "HEAD", "--", *REPORTS)
    snapshots = []
    for line in history.splitlines():
        commit, timestamp = line.split()
        sector, region = (json.loads(git("show", f"{commit}:{path}")) for path in REPORTS)
        market_date = max(date.fromisoformat(report["market_data"]["latest_price_date"])
                          for report in (sector, region))
        snapshots.append(Snapshot(commit, datetime.fromisoformat(timestamp), market_date, sector, region))
    selected = select_daily(snapshots, now, days)
    # Validate every candidate before allowing any writes; JSON supersedes the CSV projections.
    for snapshot in selected:
        validate_reports(snapshot.sector, snapshot.region, max_data_age_days=days)
    return selected


def existing_dates(conn) -> set[date]:
    return set(conn.execute(text("""
        select distinct latest_price_date from public.update_runs
        where status in ('success', 'partial') and latest_price_date is not null
    """)).scalars())


def backfill(database_url: str, snapshots: list[Snapshot], apply: bool = False) -> dict:
    engine = create_engine(_db_url(database_url), pool_pre_ping=True,
                           connect_args={"prepare_threshold": None, "connect_timeout": 15})
    result = {"imported": [], "skipped": [], "planned": []}
    try:
        for snapshot in snapshots:
            with engine.begin() as conn:
                if apply:
                    # Transaction locks are compatible with Supabase's transaction pooler.
                    conn.execute(text("select pg_advisory_xact_lock(hashtext(:key))"),
                                 {"key": f"stare-history:{snapshot.market_date}"})
                if snapshot.market_date in existing_dates(conn):
                    result["skipped"].append(str(snapshot.market_date))
                    continue
                if not apply:
                    result["planned"].append(str(snapshot.market_date))
                    continue
                with TemporaryDirectory(prefix="stare-history-") as directory:
                    paths = [Path(directory) / Path(name).name for name in REPORTS]
                    for path, data in zip(paths, (snapshot.sector, snapshot.region)):
                        path.write_text(json.dumps(data), encoding="utf-8")
                    update_id = export_reports(
                        database_url, *paths, run_label=f"Historical daily snapshot {snapshot.market_date}",
                        triggered_by="historical_backfill", max_data_age_days=30,
                        historical_at=snapshot.observed_at, source_commit=snapshot.commit,
                    )
                result["imported"].append({"date": str(snapshot.market_date), "id": update_id,
                                           "source_commit": snapshot.commit})
                print(f"Imported {snapshot.market_date}: {update_id}", flush=True)
    finally:
        engine.dispose()
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, choices=range(1, 31), default=30)
    parser.add_argument("--apply", action="store_true", help="Write missing dates; default is a read-only plan")
    args = parser.parse_args()
    url = resolve_database_url()
    if not url:
        raise SystemExit("DATABASE_URL is required in the environment or ignored .env file")
    snapshots = discover(datetime.now(UTC), args.days)
    print(f"Validated {len(snapshots)} daily historical report pairs.", flush=True)
    print(json.dumps(backfill(url, snapshots, args.apply), indent=2), flush=True)


if __name__ == "__main__":
    main()
