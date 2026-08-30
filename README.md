# Sector & Stock Trend Analysis Engine (S.T.A.R.E)

Sector & Stock Trend Analysis Engine (S.T.A.R.E) is an automated analytics system for monitoring S&P 500 sector momentum, regional market activity, active stocks, and basic company fundamentals.

The repository now supports two delivery paths:

- **GitHub Pages demo:** the original single-file static HTML dashboard refreshed by GitHub Actions.
- **Standalone portal foundation:** a Next.js + FastAPI + Supabase/Postgres path for Google authentication, user preferences, historical snapshots, and future personalization.

Both paths use the same market data pipeline and the same shared Buy/Hold/Sell signal logic.

Author: Viktor Kvapil

## What S.T.A.R.E Does

S.T.A.R.E answers a practical market-monitoring question:

Which S&P 500 sectors and major global regions are showing the strongest short-term trend, and which stocks are driving that activity?

The project turns raw market data into a publishable dashboard with:

- Sector sentiment direction: Bullish, Bearish, or Neutral
- Sector sentiment strength from 0 to 100
- Top active stocks per sector by latest trading-day dollar volume
- NA, APAC, EMEA, and LAC regional views across selected large local markets
- Top active stocks per region by latest trading-day traded value
- Latest available close price for every displayed stock
- Weekly stock returns
- Volume activity versus recent baseline
- Company fundamentals such as market cap, P/E, margins, dividend yield, beta, industry, exchange, and currency
- Deterministic Buy, Hold, or Sell model signals for every displayed stock
- A static HTML app that works without a backend

The output is intended for screening, monitoring, and research. It is not financial advice.

## Published App

The GitHub Pages version remains the public demo and fallback.

The publishable app is generated as:

- `stare_app.html` - standalone root-level HTML app
- `docs/index.html` - GitHub Pages entry point

When deployed through GitHub Pages, the expected public URL is:

`https://vittok.github.io/stare/`

The app embeds the latest dashboard JSON directly into the HTML file, so it can be served as a single static page. No server, database, API, or JavaScript package runtime is needed by the published page.

During publishing, the embedded data is sanitized into strict browser-safe JSON. Missing or non-finite values such as `NaN`, `Infinity`, and `-Infinity` are converted to `null` so the app can reliably parse and render the data in any browser.

## Standalone Portal

The standalone portal runs alongside the static app and now reproduces its
complete current dashboard experience. GitHub Pages remains published as the
public demo and fallback during UAT.

Current standalone stack:

- Frontend: Next.js in `apps/web`
- API: FastAPI in `apps/api`
- Auth and database: Supabase project `STARE`
- Database: Supabase Postgres
- Historical import bridge: `src/export_reports_to_postgres.py`

The Supabase schema is defined in `supabase/migrations/001_initial_portal_schema.sql`.

Core standalone tables:

- `update_runs` - one row per market update/import
- `sector_snapshots` - sector-level signal history
- `region_snapshots` - APAC, EMEA, LAC, and NA region history
- `stock_snapshots` - per-ticker prices, volume, fundamentals, and activity data
- `stock_recommendations` - Buy/Hold/Sell action, score, confidence, rationale, decision snapshot, and daily summary
- `user_profiles` - authenticated user profile data
- `user_preferences` - saved theme, default filters, visible columns, watchlist, and notification preferences

The first Supabase imports have been verified with populated historical data and recommendation rows. The importer reads the current JSON artifacts and writes them to Postgres:

```bash
PYTHONPATH=.python_deps:src python src/export_reports_to_postgres.py --run-label "portal seed with shared recommendations"
```

The local backend uses Supabase's Session Pooler connection string through `DATABASE_URL`. Keep that value in `.env` or deployment secrets only; do not commit it.

The portal includes All Regions, NA, NA/Sectors, LAC, EMEA, and APAC views;
country/market and sector navigation; direction and text filters; KPIs;
heatmap and strength comparisons; top active picks; sortable price, activity,
fundamental, recommendation, and decision data; company explanations; theme;
print; watchlists; visible-column preferences; and exact update/source context.

Local portal setup is documented in `apps/README.md`. Production auth, DNS, deployment secrets, and secret rotation notes are tracked in `PORTAL_DEPLOYMENT.md`. The project task tracker is `STANDALONE_PORTAL_TASKS.md`.

## Data Sources

STARE uses public data sources:

- S&P 500 universe: Wikipedia list of S&P 500 companies
- Regional universe: S&P 500 folded into NA, plus curated APAC, EMEA, and LAC market lists covering selected large exchanges and liquid local symbols
- Historical prices and volumes: Yahoo Finance through `yfinance`
- Fundamentals: Yahoo Finance through `yfinance`

Universe files are written to `data/universe_sp500.csv` and `data/universe_global.csv`. Market data and computed results are stored in `data/stocks.db`.

## Shared Pipeline Overview

The main pipeline is run with:

```bash
python src/run_pipeline.py
```

Pipeline steps:

1. `src/universe_sp500.py` and `src/universe_global.py`
   Fetch the current S&P 500 company list and generate the curated APAC, EMEA, and LAC market universe. S&P 500 sectors are folded into the parent NA region during app publishing. Symbols are normalized for Yahoo Finance.

2. `src/fetch_prices.py`
   Downloads recent daily OHLCV price data in chunks and upserts it into SQLite for both the S&P 500 and global regional universes.

3. `src/compute_weekly_stats.py`
   Calculates latest 5-session stock-level metrics.

4. `src/compute_sector_sentiment.py`
   Converts the latest available weekly stock behavior into sector-level sentiment.

5. `src/rank_sector_top_active.py`
   Ranks the most active stocks inside each sector by latest available trading-day dollar volume for the current `week_ending`.

6. `src/rank_region_top_active.py`
   Selects the top regional markets by latest traded value and ranks active stocks inside APAC, EMEA, and LAC. It uses each ticker's latest available weekly row so different local market calendars do not exclude valid data.

7. `src/build_sector_dashboard.py` and `src/build_region_dashboard.py`
   Joins sector sentiment, top active stocks, recent return data, latest available close prices, and fundamentals into dashboard JSON/CSV outputs.

8. `src/build_sector_dashboard_html.py`
   Builds the legacy HTML report.

9. `src/stare_signals.py`
   Provides the shared deterministic Buy/Hold/Sell signal, confidence score, rationale, decision snapshot, and daily stock summary logic.

10. `src/publish_stare_app.py`
    Embeds fresh sector and regional dashboard data, refresh metadata, generated top-3 stock summaries, and shared Buy/Hold/Sell model signals into the standalone HTML app and publishes it to `docs/index.html`.

11. `src/export_reports_to_postgres.py`
    Imports the current JSON artifacts into Supabase/Postgres for the standalone portal, including stock recommendation rows generated from the same shared signal module.

## Application Workflow

```mermaid
flowchart TD
    A["GitHub Actions daily schedule"] --> B["Checkout main"]
    B --> C["Install Python dependencies"]
    C --> D["Run src/run_pipeline.py"]

    D --> E["Fetch S&P 500 universe from Wikipedia"]
    D --> F["Fetch price and volume data from Yahoo Finance"]
    D --> G["Fetch fundamentals from Yahoo Finance"]

    E --> H[("SQLite database: data/stocks.db")]
    F --> H
    G --> H

    H --> I["Compute weekly stock metrics"]
    I --> I1["weekly_return"]
    I --> I2["dollar_vol_week"]
    I --> I3["vol_ratio"]

    I --> J["Compute sector sentiment"]
    J --> J1["Breadth signal"]
    J --> J2["Return signal"]
    J --> J3["Volume signal"]
    J1 --> K["Sector raw_score"]
    J2 --> K
    J3 --> K
    K --> L["Direction and strength"]

    I --> M["Rank top active stocks per sector"]
    M --> N["Top 10 by latest-day dollar volume"]

    H --> O["Build dashboard dataset"]
    L --> O
    N --> O
    O --> P["Add latest close price"]
    P --> Q["Shared signal module: src/stare_signals.py"]
    Q --> Q1["Generate Buy, Hold, or Sell signal"]
    Q --> Q2["Generate decision snapshot"]
    Q --> Q3["Generate stock summary"]

    Q1 --> R["Write report artifacts"]
    Q2 --> R
    Q3 --> R
    S --> S1["reports/sector_dashboard.json"]
    S --> S2["reports/sector_dashboard_top10.csv"]
    S --> S3["reports/sector_dashboard.html"]

    R --> T["Publish single-file HTML app"]
    T --> T1["stare_app.html"]
    T --> T2["docs/index.html"]

    T2 --> U["Commit generated artifacts"]
    U --> V["Push to main"]
    V --> W["Deploy GitHub Pages"]
    W --> X["Public S.T.A.R.E web app"]
    W --> Y["Build scheduled email report"]
    Y --> Z["Send email to Viktor"]

    R --> AA["Import artifacts to Supabase"]
    AA --> AB[("Supabase Postgres")]
    AB --> AC["FastAPI latest-report endpoint"]
    AC --> AD["Next.js standalone portal"]
```

## What Gets Calculated

### Weekly Stock Metrics

For each ticker, STARE looks at the latest 5 available trading sessions.

`weekly_return`

The percentage return from the first close in the 5-session window to the last close:

```text
weekly_return = last_close / first_close - 1
```

`dollar_vol_week`

The total traded dollar volume over the 5-session window:

```text
dollar_vol_week = sum(close * volume)
```

`week_volume`

The total share volume over the same 5-session window:

```text
week_volume = sum(volume)
```

`vol_ratio`

The current 5-session share volume divided by the average weekly volume across the prior 8 weeks:

```text
vol_ratio = current_week_volume / average_prior_8_week_volume
```

This highlights whether a stock is trading with unusually high or low activity.

### Sector Sentiment

Sector sentiment combines three signals:

`breadth_signal`

How many stocks in the sector had a positive weekly return:

```text
breadth = positive_return_stock_count / sector_stock_count
breadth_signal = (breadth - 0.5) * 2
```

A sector where most stocks are rising receives a positive breadth signal. A sector where most stocks are falling receives a negative signal.

`return_signal`

The sector's median weekly stock return, scaled around a 3% weekly move:

```text
return_signal = median_weekly_return / 0.03
```

The result is capped between `-1.0` and `1.0`.

`volume_signal`

The sector's median volume ratio, scaled around a 50% increase over baseline:

```text
volume_signal = (median_vol_ratio - 1.0) / 0.5
```

The result is capped between `-1.0` and `1.0`.

`raw_score`

The final sector score is a weighted blend:

```text
raw_score = 0.50 * breadth_signal
          + 0.35 * return_signal
          + 0.15 * volume_signal
```

This means breadth matters most, median return matters second, and abnormal volume acts as confirmation.

`direction`

The raw score is converted into a readable direction:

```text
abs(raw_score) < 0.05 -> Neutral
raw_score > 0         -> Bullish
raw_score < 0         -> Bearish
```

`strength`

The strength value converts the absolute raw score into a 0-100 scale:

```text
strength = min(100, abs(raw_score) * 100)
```

### Top Active Stocks

For each sector, stocks are ranked by `dollar_vol_latest`, which is calculated from the most recent available trading session:

```text
dollar_vol_latest = latest_close * latest_day_volume
```

The source date is stored as `volume_date`, and the share volume is stored as `latest_volume`. This identifies the names carrying the most current market activity instead of letting older high-volume days dominate the picks. Weekly return, weekly dollar volume, and volume ratio remain available as trend and confirmation context.

### Displayed Stock Prices

For each stock shown in the dashboard, S.T.A.R.E pulls the latest close price already stored in the `prices` table at or before the current dashboard `week_ending`.

The app displays this as `currentPrice` beside the ticker symbol and stores the source trading date as `priceDate`. This keeps the published app static while still showing the most recent price captured during the scheduled data refresh.

S.T.A.R.E also stores the prior trading-session close as `previousClose` with `previousCloseDate`. The app compares `currentPrice` against `previousClose` and displays the day-over-day close direction:

- Green up marker when the latest close is higher
- Black neutral marker when the latest close is unchanged
- Red down marker when the latest close is lower

The page header shows both the app refresh timestamp and the market data date, so users can distinguish when the static app was regenerated from the trading date behind the displayed prices.

### Fundamentals

STARE stores normalized fundamentals in `fundamentals_latest`, including:

- Market capitalization
- Enterprise value
- Trailing and forward P/E
- Price-to-book
- Profit, operating, and gross margins
- Return on equity and assets
- Revenue and earnings growth
- Debt and liquidity ratios
- Dividend yield and payout ratio
- Beta
- 52-week range
- Sector, industry, country, exchange, and currency

Fundamentals are used to add business context to the active stock rankings.

### Daily Stock Summaries

During each publish or portal import step, S.T.A.R.E generates short explanatory summaries from the fundamentals available in the dashboard data.

The summaries focus on:

- Price-to-book (P/B)
- Price-to-earnings (P/E)
- Price/earnings-to-growth (PEG), when available
- Dividend yield

These summaries are embedded into the static HTML and persisted into `stock_recommendations.daily_summary` for the standalone portal.

### Buy, Hold, or Sell Signals

S.T.A.R.E generates a deterministic model signal for every displayed stock:

- `Buy`
- `Hold`
- `Sell`

The signal combines sector market sentiment with stock-level weekly momentum and selected fundamentals:

- Price-to-book (P/B)
- Price-to-earnings (P/E)
- Price/earnings-to-growth (PEG), when available
- Dividend yield

Bullish sector sentiment, positive weekly return, lower valuation ratios, reasonable PEG, and higher dividend yield add support to the score. Bearish sector sentiment, negative weekly return, elevated valuation ratios, and weak or missing growth/value support reduce the score.

The output includes a recommendation action, numeric score, confidence score, short rationale, decision snapshot, and daily summary. The calculation lives in `src/stare_signals.py` so the static app and standalone portal persist the same signal.

These signals are research-oriented model outputs for screening and monitoring only; they are not personalized financial advice.

## Value Added

Raw stock tables are noisy. STARE adds value by organizing the data into a repeatable market overview:

- It compresses hundreds of S&P 500 tickers into sector-level signals.
- It shows whether a sector move is broad-based or concentrated.
- It combines price direction with volume confirmation.
- It surfaces the most liquid and active names in each sector.
- It shows the latest captured close price next to each displayed stock.
- It adds fundamentals so activity can be interpreted with company context.
- It turns sector sentiment and fundamentals into plain Buy/Hold/Sell screening signals.
- It produces static outputs that are easy to publish, archive, inspect, and share.

The dashboard is especially useful as a daily pre-market or morning scan: it points attention toward sectors with broad momentum and toward stocks where activity is highest.

## GitHub Actions Static Publishing

This section covers only the GitHub Pages demo/static publishing path.

The GitHub Actions workflow in `.github/workflows/pipeline_weekdays.yml` refreshes around the regular US market open and close:

```text
09:35 America/New_York - market open refresh
16:10 America/New_York - market close refresh
```

GitHub Actions schedules are defined in UTC/GMT, so the workflow includes paired UTC cron entries for daylight saving time and standard time. A guard step checks which cron expression triggered the run and only lets the active New York-time pair proceed, even if GitHub starts the scheduled job late.

On each scheduled market refresh, it:

1. Checks out `main`
2. Pulls the latest `origin/main` with `git pull --ff-only origin main`
3. Installs Python dependencies
4. Runs the market-data pipeline
5. Regenerates dashboard reports
6. Embeds the latest JSON data, displayed prices, stock summaries, and model signals into the HTML app
7. Pulls `origin/main` again before committing generated artifacts
8. Commits updated artifacts back to the repository
9. Deploys `docs/` to GitHub Pages
10. Sends the updated report by SMTP after every successful scheduled or manual refresh

Fundamentals are refreshed weekly after the Monday market close to reduce load.

### SMTP Update Notifications

The workflow sends an HTML email notification after GitHub Pages deployment for both scheduled and manually triggered refreshes. The email body is generated from the freshly embedded `stare_app.html` data and includes:

- Last refresh timestamp
- Link to the published S.T.A.R.E dashboard
- Sector overview table
- Top active stock summaries
- Updated stock report with ticker, price, signal, confidence, weekly return, and latest-day dollar volume

The production workflow is configured for Brevo's SMTP relay:

- Host: `smtp-relay.brevo.com`
- Port: `587`
- Encryption: STARTTLS
- Recipient: `vittok@hotmail.com`

Create a free Brevo account, verify the sender address or domain, and create an SMTP key under Brevo's transactional SMTP settings. Then add these GitHub repository secrets:

- `SMTP_USERNAME` - the SMTP login shown by Brevo
- `SMTP_PASSWORD` - the generated Brevo SMTP key, not the Brevo account password
- `SMTP_FROM` - a sender address verified in Brevo

They can be added from **GitHub → Settings → Secrets and variables → Actions**, or with:

```bash
gh secret set SMTP_USERNAME --repo vittok/stare
gh secret set SMTP_PASSWORD --repo vittok/stare
gh secret set SMTP_FROM --repo vittok/stare
```

Do not commit SMTP credentials to the repository. If any of these three secrets is missing, the workflow logs a warning and skips only the email step. Authentication or sender-verification failures fail the email step so a broken notification configuration is visible in GitHub Actions.

The workflow can also be triggered manually from the GitHub Actions tab.

## Standalone Portal Operations

The standalone portal path is separate from GitHub Actions static publishing.

Current status:

- Supabase initial schema has been applied.
- The local `DATABASE_URL` uses the Supabase Session Pooler because the direct DB host can require IPv6.
- Existing report artifacts have been imported into Supabase.
- A later import verified `stock_recommendations` has one recommendation row for each imported stock snapshot.
- The FastAPI latest-report path returns the complete current snapshot and
  derives NA region direction from S&P 500 sector snapshots using the same
  calculation as the static publisher.
- The Next.js portal has current static-app feature parity plus authenticated
  watchlist, theme, filter, and visible-column personalization.

Current standalone operations:

- Host `apps/web` as the Next.js frontend on Render.
- Host `apps/api` as the FastAPI backend on Render.
- Store `DATABASE_URL` and future service secrets in deployment secrets.
- Import each GitHub-scheduled market update into Supabase during UAT.
- Keep GitHub Pages available as the public demo/fallback while the portal matures.

The remaining operational migration is moving market-open and market-close
execution from the temporary GitHub bridge into a standalone scheduled job.

Important security notes:

- Do not expose `DATABASE_URL` or service-role keys to the browser.
- Rotate setup-time secrets before production launch.
- Keep Supabase Row Level Security enabled for user profile and preference tables.

## Repository Structure

```text
.
├── apps/
│   ├── api/
│   │   ├── app/
│   │   └── requirements.txt
│   └── web/
│       ├── app/
│       ├── components/
│       └── package.json
├── data/
│   ├── stocks.db
│   ├── universe_global.csv
│   └── universe_sp500.csv
├── docs/
│   ├── index.html
│   ├── region_dashboard.json
│   ├── region_dashboard_top_active.csv
│   ├── sector_dashboard.json
│   └── sector_dashboard_top10.csv
├── reports/
│   ├── fundamentals_sp500_latest.csv
│   ├── region_dashboard.json
│   ├── region_dashboard_top_active.csv
│   ├── sector_dashboard.html
│   ├── sector_dashboard.json
│   └── sector_dashboard_top10.csv
├── supabase/
│   └── migrations/
│       └── 001_initial_portal_schema.sql
├── src/
│   ├── build_region_dashboard.py
│   ├── build_sector_dashboard.py
│   ├── build_sector_dashboard_html.py
│   ├── compute_sector_sentiment.py
│   ├── compute_weekly_stats.py
│   ├── export_reports_to_postgres.py
│   ├── fetch_fundamentals.py
│   ├── fetch_prices.py
│   ├── publish_stare_app.py
│   ├── rank_region_top_active.py
│   ├── rank_sector_top_active.py
│   ├── run_pipeline.py
│   ├── stare_signals.py
│   ├── store_sqlite.py
│   ├── universe_global.py
│   └── universe_sp500.py
├── DATA_SCHEMA.md
├── DEV_GUIDE.md
├── STANDALONE_PORTAL_TASKS.md
├── stare_app.html
├── requirements.txt
└── README.md
```

## Database Tables

Core SQLite tables for the GitHub Actions/static app pipeline:

- `prices`
- `weekly_stats`
- `sector_sentiment`
- `sector_top_active`
- `region_top_active`
- `fundamentals_snapshot`
- `fundamentals_latest`
- `sector_dashboard_top10`

Core Supabase/Postgres tables for the standalone portal:

- `update_runs`
- `sector_snapshots`
- `region_snapshots`
- `stock_snapshots`
- `stock_recommendations`
- `user_profiles`
- `user_preferences`

See `DATA_SCHEMA.md` for a compact schema summary.

## Local Setup

Recommended Python version:

```text
3.11.7
```

Install dependencies:

```bash
python -m pip install --upgrade pip
pip install -r requirements.txt
```

Run the full pipeline:

```bash
python src/run_pipeline.py
```

Refresh only the publishable app from existing report data:

```bash
python src/publish_stare_app.py
```

Open the local app:

```text
stare_app.html
```

## Outputs

Primary generated outputs:

- `reports/sector_dashboard.json` - nested dashboard data
- `reports/sector_dashboard_top10.csv` - flat top-active table
- `reports/sector_dashboard.html` - legacy report HTML
- `stare_app.html` - standalone publishable app
- `docs/index.html` - GitHub Pages app

## Design Principles

- SQLite-first storage
- Deterministic calculations from stored data
- Static publishing
- No backend required for the published app
- CI-friendly automation
- Human-readable outputs for inspection and sharing

## License

MIT
