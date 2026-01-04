import subprocess
import sys

STEPS = [
    ["python", "src/universe_sp500.py"],
    ["python", "src/fetch_prices.py"],
    ["python", "src/compute_weekly_stats.py"],
    ["python", "src/compute_sector_sentiment.py"],
    ["python", "src/rank_sector_top_active.py"],
    ["python", "src/build_sector_dashboard.py"],
    ["python", "src/build_sector_dashboard_html.py"],
]

# Fundamentals is heavy; keep it as an optional step.
FUNDAMENTALS = ["python", "src/fetch_fundamentals.py"]


def run(cmd):
    print("\n==>", " ".join(cmd))
    subprocess.check_call(cmd)


def main():
    for cmd in STEPS:
        run(cmd)

    # Optional: run fundamentals only on Mondays to reduce rate-limit / runtime.
    # You can flip this to always run fundamentals by uncommenting the next line
    # and removing the weekday condition in the GitHub workflow.
    #
    # run(FUNDAMENTALS)

    print("\nPipeline completed.")


if __name__ == "__main__":
    sys.exit(main())
