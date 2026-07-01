from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

STEPS = [
    ("S&P 500 universe", ["python", "src/universe_sp500.py"]),
    ("Global universe", ["python", "src/universe_global.py"]),
    ("S&P 500 prices", ["python", "src/fetch_prices.py"]),
    ("Global prices", ["python", "src/fetch_prices.py"], {"STARE_UNIVERSE_CSV": "data/universe_global.csv"}),
    ("Weekly stats", ["python", "src/compute_weekly_stats.py"]),
    ("Sector sentiment", ["python", "src/compute_sector_sentiment.py"]),
    ("Top active", ["python", "src/rank_sector_top_active.py"]),
    ("Region top active", ["python", "src/rank_region_top_active.py"]),
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
        import os

        step_env = {**os.environ, **env}
    subprocess.check_call(cmd, cwd=str(ROOT), env=step_env)
    elapsed = time.time() - start

    print(f"✓ Completed {name} in {elapsed:.1f}s")

def main() -> int:
    p = argparse.ArgumentParser(description="Run the STARE pipeline.")
    p.add_argument("--with-fundamentals", action="store_true")
    p.add_argument("--fundamentals-only", action="store_true")
    p.add_argument("--continue-on-error", action="store_true")
    args = p.parse_args()

    if args.fundamentals_only:
        for idx, (name, cmd, env) in enumerate(FUNDAMENTALS, start=1):
            run_step(name, cmd, idx, len(FUNDAMENTALS), env)
        return 0

    total_steps = len(STEPS) + (len(FUNDAMENTALS) if args.with_fundamentals else 0)
    failures = 0
    step_idx = 0

    for step in STEPS:
        name, cmd, env = step if len(step) == 3 else (step[0], step[1], None)
        step_idx += 1
        try:
            run_step(name, cmd, step_idx, total_steps, env)
        except subprocess.CalledProcessError as e:
            failures += 1
            print(f"✗ FAILED ({e.returncode}): {name}")
            if not args.continue_on_error:
                return e.returncode

    if args.with_fundamentals:
        for name, cmd, env in FUNDAMENTALS:
            step_idx += 1
            try:
                run_step(name, cmd, step_idx, total_steps, env)
            except subprocess.CalledProcessError as e:
                failures += 1
                print(f"✗ FAILED ({e.returncode}): {name}")
                if not args.continue_on_error:
                    return e.returncode

    print("\n" + progress_bar(total_steps, total_steps))
    print("🎉 Pipeline finished.")
    return 0 if failures == 0 else 2

if __name__ == "__main__":
    sys.exit(main())
