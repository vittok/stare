import logging
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pandas_market_calendars as mcal

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session

from ..db import get_db

router = APIRouter(prefix="/api", tags=["reports"])
logger = logging.getLogger(__name__)

ALERT_GRACE = timedelta(minutes=75)


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _expected_update_checkpoint(now: datetime | None = None) -> datetime | None:
    current = _utc(now or datetime.now(UTC))
    schedule = mcal.get_calendar("NYSE").schedule(
        start_date=(current - timedelta(days=14)).date(),
        end_date=current.date(),
    )
    checkpoints: list[datetime] = []
    for _, session in schedule.iterrows():
        for column, delay in (("market_open", 5), ("market_close", 10)):
            checkpoint = _utc(session[column].to_pydatetime()) + timedelta(minutes=delay)
            if current >= checkpoint + ALERT_GRACE:
                checkpoints.append(checkpoint)
    return max(checkpoints) if checkpoints else None


def _report_alert(
    update_run: dict | None,
    latest_attempt: dict | None,
    now: datetime | None = None,
) -> dict | None:
    detected_at = _utc(now or datetime.now(UTC))
    if latest_attempt and latest_attempt.get("status") == "failed":
        attempt_started = latest_attempt.get("started_at")
        update_started = update_run.get("started_at") if update_run else None
        if update_started is None or (attempt_started and _utc(attempt_started) > _utc(update_started)):
            return {
                "kind": "failed",
                "title": "Latest market update failed",
                "message": "The last successful snapshot remains available while the update is retried.",
                "detected_at": detected_at.isoformat(),
            }

    if update_run and update_run.get("status") == "partial":
        return {
            "kind": "partial",
            "title": "Latest market update is incomplete",
            "message": "Some market data could not be refreshed. Values shown may have reduced coverage.",
            "detected_at": detected_at.isoformat(),
        }

    try:
        checkpoint = _expected_update_checkpoint(detected_at)
    except Exception:
        logger.exception("Could not calculate the expected market update checkpoint.")
        checkpoint = None
    completed_at = update_run.get("completed_at") if update_run else None
    if checkpoint and (completed_at is None or _utc(completed_at) < checkpoint):
        return {
            "kind": "stale",
            "title": "Market data may be stale",
            "message": "No successful refresh has completed since the latest scheduled market checkpoint.",
            "detected_at": detected_at.isoformat(),
            "expected_after": checkpoint.isoformat(),
        }
    return None


def _north_america_region(sectors: list[dict]) -> dict | None:
    if not sectors:
        return None

    raw_scores = [
        float(row["raw_score"])
        for row in sectors
        if isinstance(row.get("raw_score"), (int, float, Decimal))
    ]
    raw_score = sum(raw_scores) / len(raw_scores) if raw_scores else 0.0
    direction = "Neutral" if abs(raw_score) < 0.05 else "Bullish" if raw_score > 0 else "Bearish"
    weeks = sorted(str(row["week_ending"]) for row in sectors if row.get("week_ending"))
    return {
        "region": "NA",
        "week_ending": weeks[-1] if weeks else None,
        "direction": direction,
        "strength": min(100, int(abs(raw_score) * 100)),
        "raw_score": raw_score,
        "diagnostics": {
            "source": "S&P 500 sector snapshots folded into North America",
            "n_sectors": len(sectors),
        },
    }


@router.get("/latest-report")
def latest_report(db: Session = Depends(get_db)) -> dict:
    try:
        latest_attempt = db.execute(
            text(
                """
                select id, status, started_at, completed_at
                from public.update_runs
                order by started_at desc
                limit 1
                """
            )
        ).mappings().first()
        update_run = db.execute(
            text(
                """
                select id, run_label, status, started_at, completed_at,
                       market_data_date, latest_price_date, diagnostics
                from public.update_runs
                where status in ('success', 'partial')
                order by completed_at desc nulls last, started_at desc
                limit 1
                """
            )
        ).mappings().first()
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Database is not reachable") from exc

    if update_run is None:
        return {
            "update": None,
            "alert": _report_alert(None, dict(latest_attempt) if latest_attempt else None),
            "regions": [],
            "sectors": [],
            "top_stocks": [],
        }

    run_id = update_run["id"]
    regions = db.execute(
        text(
            """
            select region, week_ending, direction, strength, raw_score, diagnostics
            from public.region_snapshots
            where update_run_id = :run_id
            order by strength desc nulls last, region
            """
        ),
        {"run_id": run_id},
    ).mappings().all()
    sectors = db.execute(
        text(
            """
            select sector, week_ending, direction, strength, raw_score, diagnostics
            from public.sector_snapshots
            where update_run_id = :run_id
            order by strength desc nulls last, sector
            """
        ),
        {"run_id": run_id},
    ).mappings().all()
    top_stocks = db.execute(
        text(
            """
            with latest_rows as (
              select
                s.*,
                row_number() over (
                  partition by s.region, s.market, s.sector, s.ticker
                  order by s.created_at, s.id
                ) as duplicate_rank
              from public.stock_snapshots s
              where s.update_run_id = :run_id and s.rank is not null
            )
            select s.ticker, s.company_name, s.region, s.market, s.country, s.sector,
                   s.rank, s.volume_date, s.price_date, s.current_price,
                   s.previous_close, s.previous_close_date, s.close_change,
                   s.close_change_pct, s.close_direction, s.weekly_return,
                   s.dollar_vol_latest, s.latest_volume, s.dollar_vol_week,
                   s.vol_ratio, s.daily_trading_percentile, s.market_cap,
                   s.trailing_pe, s.forward_pe, s.price_to_book, s.peg_ratio,
                   s.dividend_yield, s.currency, s.exchange, s.industry,
                   s.fundamentals, r.action, r.score, r.confidence, r.rationale,
                   r.daily_summary, r.decision_snapshot
            from latest_rows s
            left join public.stock_recommendations r
              on r.update_run_id = s.update_run_id and r.ticker = s.ticker
            where s.duplicate_rank = 1
            order by s.region, s.market, s.sector, s.rank, s.ticker
            """
        ),
        {"run_id": run_id},
    ).mappings().all()

    sector_rows = [dict(row) for row in sectors]
    region_rows = [dict(row) for row in regions]
    north_america = _north_america_region(sector_rows)
    if north_america:
        region_rows = [north_america, *[row for row in region_rows if row["region"] != "NA"]]

    return {
        "update": dict(update_run),
        "alert": _report_alert(
            dict(update_run),
            dict(latest_attempt) if latest_attempt else None,
        ),
        "regions": region_rows,
        "sectors": sector_rows,
        "top_stocks": [dict(row) for row in top_stocks],
    }
