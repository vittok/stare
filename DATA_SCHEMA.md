# STARE — Data Schema

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
