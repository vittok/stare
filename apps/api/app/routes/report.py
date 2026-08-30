from decimal import Decimal

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session

from ..db import get_db

router = APIRouter(prefix="/api", tags=["reports"])


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
        return {"update": None, "regions": [], "sectors": [], "top_stocks": []}

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
        "regions": region_rows,
        "sectors": sector_rows,
        "top_stocks": [dict(row) for row in top_stocks],
    }
