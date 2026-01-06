# STARE — Developer Guide

## Running the Pipeline

python src/run_pipeline.py

---

## Script Responsibilities

- universe_sp500.py: universe ingestion
- fetch_prices.py: price data
- compute_weekly_stats.py: weekly metrics
- compute_sector_sentiment.py: sector scoring
- rank_sector_top_active.py: liquidity ranking
- fetch_fundamentals.py: fundamentals
- build_sector_dashboard.py: data joins
- build_sector_dashboard_html.py: HTML rendering

---

## Database

SQLite database: data/stocks.db

---

## Extending the System

Add new metrics by:
1. Computing them
2. Storing them
3. Exposing them in reports
