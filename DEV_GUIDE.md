# STARE — Developer Guide

## Architecture Tracks

S.T.A.R.E currently has two tracks:

- **GitHub Actions static publishing:** SQLite-backed pipeline builds JSON/CSV artifacts and embeds them into `stare_app.html` and `docs/index.html`.
- **Standalone portal:** Next.js frontend, FastAPI backend, and Supabase/Postgres history tables for authenticated users and future personalization.

The static app remains the GitHub Pages demo/fallback while the standalone portal is being built.

## GitHub Actions / Static App Pipeline

```bash
python src/run_pipeline.py
```

Refresh only the already-generated static app data:

```bash
PYTHONPATH=src python src/publish_stare_app.py
```

## Script Responsibilities

- universe_sp500.py: universe ingestion
- universe_global.py: APAC, EMEA, LAC regional universe
- fetch_prices.py: price data
- compute_weekly_stats.py: weekly metrics
- compute_sector_sentiment.py: sector scoring
- rank_sector_top_active.py: liquidity ranking
- rank_region_top_active.py: regional market and stock ranking
- fetch_fundamentals.py: fundamentals
- build_sector_dashboard.py: data joins
- build_region_dashboard.py: regional dashboard data joins
- build_sector_dashboard_html.py: HTML rendering
- stare_signals.py: shared Buy/Hold/Sell, confidence, rationale, decision snapshot, and summary logic
- publish_stare_app.py: embeds dashboard data and shared signals into the static HTML app
- export_reports_to_postgres.py: imports current artifacts into Supabase/Postgres for the standalone portal

## Standalone Portal

Frontend:

```bash
cd apps/web
npm install
npm run dev
```

API:

```bash
cd apps/api
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```

Import current report artifacts into Supabase/Postgres:

```bash
PYTHONPATH=.python_deps:src python src/export_reports_to_postgres.py --run-label "portal seed with shared recommendations"
```

The API expects `DATABASE_URL` to point to the Supabase Session Pooler connection string. Store it only in `.env` or deployment secrets.

Production DNS, OAuth callback, and secret-rotation notes are in `PORTAL_DEPLOYMENT.md`.

## Databases

Static pipeline database:

- SQLite: `data/stocks.db`

Standalone portal database:

- Supabase/Postgres
- Schema: `supabase/migrations/001_initial_portal_schema.sql`

## Extending the System

Add new metrics by:
1. Computing them
2. Storing them
3. Exposing them in reports
4. Persisting them into Supabase if the standalone portal needs history
