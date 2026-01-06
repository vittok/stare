from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path
from shutil import copy2

ROOT = Path(__file__).resolve().parents[1]

STEPS = [
    ("Universe", ["python", "src/universe_sp500.py"]),
    ("Prices", ["python", "src/fetch_prices.py"]),
    ("Weekly stats", ["python", "src/compute_weekly_stats.py"]),
    ("Sector sentiment", ["python", "src/compute_sector_sentiment.py"]),
    ("Top active", ["python", "src/rank_sector_top_active.py"]),
    ("Dashboard (JSON/CSV)", ["python", "src/build_sector_dashboard.py"]),
    ("Dashboard (HTML)", ["python", "src/build_sector_dashboard_html.py"]),
]

FUNDAMENTALS = ("Fundamentals", ["python", "src/fetch_fundamentals.py"])


def progress_bar(i: int, total: int, width: int = 30) -> str:
    filled = int(width * i / total)
    bar = "█" * filled + "░" * (width - filled)
    return f"[{bar}] {i}/{total}"


def run_step(name: str, cmd: list[str], idx: int, total: int):
    print("\n" + progress_bar(idx, total))
    print(f"▶ Step {idx}/{total}: {name}")
    print("  CMD:", " ".join(cmd))

    start = time.time()
    subprocess.check_call(cmd, cwd=str(ROOT))
    elapsed = time.time() - start

    print(f"✓ Completed {name} in {elapsed:.1f}s")

def publish_to_docs():
    docs = ROOT / "docs"
    reports = ROOT / "reports"
    docs.mkdir(parents=True, exist_ok=True)

    # HTML entry point for GitHub Pages
    copy2(reports / "sector_dashboard.html", docs / "index.html")

    # Optional: publish raw data artifacts too
    copy2(reports / "sector_dashboard.json", docs / "sector_dashboard.json")
    copy2(reports / "sector_dashboard_top10.csv", docs / "sector_dashboard_top10.csv")

    # Ensure no Jekyll processing
    (docs / ".nojekyll").write_text("", encoding="utf-8")

    print("✓ Published GitHub Pages artifacts to docs/")

def main() -> int:
    p = argparse.ArgumentParser(description="Run the STARE pipeline.")
    p.add_argument("--with-fundamentals", action="store_true")
    p.add_argument("--fundamentals-only", action="store_true")
    p.add_argument("--continue-on-error", action="store_true")
    args = p.parse_args()

    if args.fundamentals_only:
        run_step(FUNDAMENTALS[0], FUNDAMENTALS[1], 1, 1)
        return 0

    total_steps = len(STEPS) + (1 if args.with_fundamentals else 0)
    failures = 0
    step_idx = 0

    for name, cmd in STEPS:
        step_idx += 1
        try:
            run_step(name, cmd, step_idx, total_steps)
        except subprocess.CalledProcessError as e:
            failures += 1
            print(f"✗ FAILED ({e.returncode}): {name}")
            if not args.continue_on_error:
                return e.returncode

    if args.with_fundamentals:
        step_idx += 1
        try:
            run_step(FUNDAMENTALS[0], FUNDAMENTALS[1], step_idx, total_steps)
        except subprocess.CalledProcessError as e:
            failures += 1
            print(f"✗ FAILED ({e.returncode}): Fundamentals")
            if not args.continue_on_error:
                return e.returncode

    print("\n" + progress_bar(total_steps, total_steps))
    print("🎉 Pipeline finished.")
    return 0 if failures == 0 else 2

publish_to_docs()

if __name__ == "__main__":
    sys.exit(main())
