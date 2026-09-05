# STARE — Data Schema

S.T.A.R.E has two data layers:

- **SQLite** for the GitHub Actions/static HTML publishing pipeline.
- **Supabase/Postgres** for the standalone authenticated portal and historical tracking.

`src/run_pipeline.py` produces the final calculated report once and sends that
state to both layers when Postgres output is enabled. File artifacts remain
available as the static fallback; `src/export_reports_to_postgres.py` is the
reusable database writer and historical recovery command.

## SQLite Pipeline Tables

## prices
ticker | date | open | high | low | close | adj_close | volume

## weekly_stats
ticker | week_ending | weekly_return | dollar_vol_week | week_volume | vol_ratio

## sector_sentiment
sector | week_ending | raw_score | direction | strength | diagnostics_json

## sector_top_active
sector | week_ending | rank | ticker | volume_date | dollar_vol_latest | latest_volume | dollar_vol_week | weekly_return | vol_ratio

## region_top_active
region | market | country | week_ending | rank | ticker | volume_date | dollar_vol_latest | latest_volume | dollar_vol_week | weekly_return | vol_ratio

Rows are ranked by latest available trading-day traded value inside the selected regional markets. The regional ranker uses each ticker's latest available weekly row so markets with different local holidays or trading calendars are not dropped merely because their latest `week_ending` differs from the US market date.

The published app also derives an `NA` region from the S&P 500 sector dashboard. NA uses `market = S&P 500` and `country = United States`, preserving sector detail while allowing the app's region view to compare NA, APAC, EMEA, and LAC together.

## fundamentals_latest
ticker | asof_utc | normalized_json

## Published Artifacts
- `reports/sector_dashboard.json` and `docs/sector_dashboard.json`
- `reports/sector_dashboard_top10.csv` and `docs/sector_dashboard_top10.csv`
- `reports/region_dashboard.json` and `docs/region_dashboard.json`
- `reports/region_dashboard_top_active.csv` and `docs/region_dashboard_top_active.csv`

## Shared Signal Fields

`src/stare_signals.py` generates the same recommendation payload for the static app and the standalone portal:

- `action` - Buy, Hold, or Sell
- `score` - numeric model score
- `confidence` - 0-100 confidence value
- `rationale` - short explanation of the drivers
- `decision_snapshot` - valuation, quality, risk, momentum, and income labels
- `daily_summary` - concise stock summary for display

## Supabase/Postgres Portal Tables

Defined in `supabase/migrations/001_initial_portal_schema.sql`, hardened by
`supabase/migrations/002_portal_security_hardening.sql`, and maintained by the
30-day retention job in
`supabase/migrations/20260905150822_market_snapshot_retention.sql`. Named
watchlists and scoring profiles are added by
`supabase/migrations/20260905154101_named_watchlists_and_scoring_weights.sql`.

## update_runs
id | run_label | triggered_by | status | started_at | completed_at | market_data_date | latest_price_date | source_commit | diagnostics | created_at

One row per standalone portal import or future scheduled update.

### Update validation and failure audit

An update is marked `success` only after `src/export_reports_to_postgres.py`
confirms all 11 S&P 500 sectors and APAC, EMEA, and LAC are present, each group
has at least three stocks, price and latest-day activity coverage are at least
90%, group signals and generated stock recommendations are valid, and the
Postgres row counts match the validated report totals. Scheduled updates also
reject market data older than seven calendar days by default; override that
limit with `STARE_MAX_DATA_AGE_DAYS` when needed.

The `update_runs` audit row is committed before snapshot persistence. Snapshot
rows and the final status change are then written atomically. If artifact
loading, validation, or persistence fails, snapshot changes are rolled back and
the audit row is marked `failed`. Its `diagnostics.failure` object records the
stage, exception type, message, and validation errors where applicable.
Calculation-step failures are also recorded with completed and failed step
lists. Failed rows are not selected as the portal's latest report, so users
continue to see the newest successful snapshot.

### Snapshot retention

Supabase Cron runs `private.cleanup_market_snapshot_retention(30)` every day at
02:15 UTC. The function deletes `update_runs` older than 30 days, and the
foreign-key cascades remove the associated sector, region, stock, and
recommendation snapshots in the same transaction. The newest successful update
is always preserved so the portal retains a report during an extended update
outage. Function execution is restricted to the `postgres` job owner.

## sector_snapshots
id | update_run_id | sector | week_ending | direction | strength | raw_score | diagnostics | created_at

Stores sector-level history for charts and comparisons.

## region_snapshots
id | update_run_id | region | week_ending | direction | strength | raw_score | diagnostics | created_at

Stores NA, APAC, EMEA, and LAC region-level history.

## stock_snapshots
id | update_run_id | ticker | company_name | sector | region | market | country | rank | volume_date | price_date | current_price | previous_close | previous_close_date | close_change | close_change_pct | close_direction | weekly_return | dollar_vol_latest | latest_volume | dollar_vol_week | vol_ratio | daily_trading_percentile | market_cap | trailing_pe | forward_pe | price_to_book | peg_ratio | dividend_yield | currency | exchange | industry | fundamentals | created_at

Stores per-ticker market data, activity ranking inputs, price context, and fundamentals.

## stock_recommendations
id | update_run_id | ticker | action | score | confidence | rationale | decision_snapshot | daily_summary | created_at

Stores the shared Buy/Hold/Sell signal and explanation generated by `src/stare_signals.py`.

## user_profiles
user_id | display_name | avatar_url | created_at | updated_at

Linked to Supabase Auth users.

## user_preferences
user_id | theme | default_region | default_sector | default_market | visible_columns | watchlist | notification_settings | created_at | updated_at

Stores per-user display and navigation settings. The original `watchlist` array
is retained as a compatibility field; new watchlists use the normalized tables
below. Row Level Security allows users to read and update only their own rows.

## user_watchlists
id | user_id | name | is_default | created_at | updated_at

Stores multiple uniquely named lists per user. A partial unique index permits at
most one default list for each user.

## user_watchlist_items
watchlist_id | user_id | ticker | created_at

Stores normalized uppercase ticker membership. The composite foreign key binds
each item to a watchlist owned by the same user, and deleting a list cascades to
its items.

## user_scoring_weights
user_id | group_sentiment_weight | pe_weight | pb_weight | peg_weight | dividend_weight | momentum_weight | created_at | updated_at

Stores one optional custom scoring profile per user. Each factor is constrained
between `0.0` and `2.0`; at least one factor must remain above zero. Missing or
reset profiles use the standard model multiplier of `1.0` for every factor.
