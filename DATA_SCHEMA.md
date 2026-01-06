# STARE — Data Schema

## prices
ticker | date | open | high | low | close | adj_close | volume

## weekly_stats
ticker | week_ending | weekly_return | dollar_vol_week | week_volume | vol_ratio

## sector_sentiment
sector | week_ending | raw_score | direction | strength | diagnostics_json

## sector_top_active
sector | week_ending | rank | ticker | dollar_vol_week | weekly_return | vol_ratio

## fundamentals_latest
ticker | asof_utc | normalized_json
