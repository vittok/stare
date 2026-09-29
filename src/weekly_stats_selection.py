from __future__ import annotations

import pandas as pd


def select_latest_universe_week(
    weekly: pd.DataFrame,
    universe: pd.DataFrame,
) -> pd.DataFrame:
    """Return rows from the latest weekly-stat date within the requested universe."""
    required_weekly = {"ticker", "week_ending"}
    missing_weekly = required_weekly - set(weekly.columns)
    if missing_weekly:
        raise RuntimeError(
            "weekly_stats data missing columns: " + ", ".join(sorted(missing_weekly))
        )
    if "ticker_yahoo" not in universe.columns:
        raise RuntimeError("Universe data missing column: ticker_yahoo")

    universe_tickers = set(universe["ticker_yahoo"].dropna())
    scoped = weekly[weekly["ticker"].isin(universe_tickers)].copy()
    if scoped.empty:
        raise RuntimeError("No weekly_stats data overlaps the requested universe.")

    latest_week = scoped["week_ending"].max()
    if pd.isna(latest_week):
        raise RuntimeError("The requested universe has no valid weekly_stats dates.")

    latest = scoped[scoped["week_ending"] == latest_week].copy()
    if latest.empty:
        raise RuntimeError("No weekly_stats data found for the universe's latest date.")
    return latest
