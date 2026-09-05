from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

CALCULATION_STEPS = [
    ("S&P 500 universe", ["python", "src/universe_sp500.py"]),
    ("Global universe", ["python", "src/universe_global.py"]),
    ("S&P 500 prices", ["python", "src/fetch_prices.py"]),
    ("Global prices", ["python", "src/fetch_prices.py"], {"STARE_UNIVERSE_CSV": "data/universe_global.csv"}),
    ("Weekly stats", ["python", "src/compute_weekly_stats.py"]),
    ("Sector sentiment", ["python", "src/compute_sector_sentiment.py"]),
    ("Top active", ["python", "src/rank_sector_top_active.py"]),
    ("Region top active", ["python", "src/rank_region_top_active.py"]),
]

OUTPUT_STEPS = [
    ("Dashboard (JSON/CSV)", ["python", "src/build_sector_dashboard.py"]),
    ("Region dashboard (JSON/CSV)", ["python", "src/build_region_dashboard.py"]),
    ("Dashboard (HTML)", ["python", "src/build_sector_dashboard_html.py"]),
    ("Publish app", ["python", "src/publish_stare_app.py"]),
]

FUNDAMENTALS = [
    ("S&P 500 fundamentals", ["python", "src/fetch_fundamentals.py"], {}),
    (
        "Global fundamentals",
        ["python", "src/fetch_fundamentals.py"],
        {
            "STARE_UNIVERSE_CSV": "data/universe_global.csv",
            "STARE_FUNDAMENTALS_REPORT": "reports/fundamentals_global_latest.csv",
        },
    ),
]


def progress_bar(i: int, total: int, width: int = 30) -> str:
    filled = int(width * i / total)
    bar = "█" * filled + "░" * (width - filled)
    return f"[{bar}] {i}/{total}"


def run_step(name: str, cmd: list[str], idx: int, total: int, env: dict[str, str] | None = None):
    print("\n" + progress_bar(idx, total))
    print(f"▶ Step {idx}/{total}: {name}")
    print("  CMD:", " ".join(cmd))

    start = time.time()
    step_env = None
    if env:
        step_env = {**os.environ, **env}
    subprocess.check_call(cmd, cwd=str(ROOT), env=step_env)
    elapsed = time.time() - start

    print(f"✓ Completed {name} in {elapsed:.1f}s")


def build_steps(with_fundamentals: bool) -> list[tuple]:
    fundamentals = FUNDAMENTALS if with_fundamentals else []
    return [*CALCULATION_STEPS, *fundamentals, *OUTPUT_STEPS]


def persist_to_postgres(database_url: str, run_label: str) -> str:
    from export_reports_to_postgres import export_reports

    return export_reports(
        database_url=database_url,
        sector_report_path=ROOT / "reports/sector_dashboard.json",
        region_report_path=ROOT / "reports/region_dashboard.json",
        run_label=run_label,
        triggered_by="market_pipeline",
    )


def run_postgres_step(database_url: str, run_label: str, idx: int, total: int) -> str:
    name = "Postgres portal snapshot"
    print("\n" + progress_bar(idx, total))
    print(f"▶ Step {idx}/{total}: {name}")

    start = time.time()
    update_run_id = persist_to_postgres(database_url, run_label)
    elapsed = time.time() - start

    print(f"✓ Completed {name} in {elapsed:.1f}s (update_run_id={update_run_id})")
    return update_run_id


def _database_url(explicit_url: str | None) -> str | None:
    from export_reports_to_postgres import resolve_database_url

    return resolve_database_url(explicit_url)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Run the STARE market update.")
    p.add_argument("--with-fundamentals", action="store_true")
    p.add_argument("--fundamentals-only", action="store_true")
    p.add_argument("--continue-on-error", action="store_true")
    p.add_argument(
        "--postgres-mode",
        choices=("disabled", "auto", "required"),
        default=os.getenv("STARE_POSTGRES_MODE", "disabled"),
        help="Control whether the final report is persisted to Postgres.",
    )
    p.add_argument("--database-url", default=None, help=argparse.SUPPRESS)
    p.add_argument("--run-label", default=None)
    args = p.parse_args(argv)

    if args.fundamentals_only:
        for idx, (name, cmd, env) in enumerate(FUNDAMENTALS, start=1):
            run_step(name, cmd, idx, len(FUNDAMENTALS), env)
        return 0

    database_url = _database_url(args.database_url) if args.postgres_mode != "disabled" else None
    if args.postgres_mode == "required" and not database_url:
        print("DATABASE_URL is required when --postgres-mode=required.", file=sys.stderr)
        return 2

    write_postgres = bool(database_url) and args.postgres_mode in {"auto", "required"}
    steps = build_steps(args.with_fundamentals)
    total_steps = len(steps) + (1 if write_postgres else 0)
    failures = 0
    step_idx = 0

    for step in steps:
        name, cmd, env = step if len(step) == 3 else (step[0], step[1], None)
        step_idx += 1
        try:
            run_step(name, cmd, step_idx, total_steps, env)
        except subprocess.CalledProcessError as e:
            failures += 1
            print(f"✗ FAILED ({e.returncode}): {name}")
            if not args.continue_on_error:
                return e.returncode

    if write_postgres and failures == 0:
        step_idx += 1
        refresh_label = args.run_label or os.getenv("STARE_REFRESH_LABEL", "manual")
        run_label = f"{refresh_label.strip() or 'manual'} portal update"
        try:
            run_postgres_step(database_url, run_label, step_idx, total_steps)
        except Exception as exc:
            failures += 1
            print(f"✗ FAILED: Postgres portal snapshot: {exc}", file=sys.stderr)
            if not args.continue_on_error:
                return 1
    elif args.postgres_mode == "auto" and not database_url:
        print("\nPostgres output skipped because DATABASE_URL is not configured.")
    elif write_postgres and failures:
        print("\nPostgres output skipped because an earlier step failed.")

    print("\n" + progress_bar(total_steps, total_steps))
    print("🎉 Market update finished.")
    return 0 if failures == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
