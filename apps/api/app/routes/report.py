from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session

from ..db import get_db

router = APIRouter(prefix="/api", tags=["reports"])


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
            select region, week_ending, direction, strength, raw_score
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
            select sector, week_ending, direction, strength, raw_score
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
            select s.ticker, s.company_name, s.region, s.market, s.country, s.sector,
                   s.rank, s.current_price, s.previous_close, s.close_change_pct,
                   s.daily_trading_percentile, s.weekly_return,
                   r.action, r.score, r.confidence, r.rationale,
                   r.daily_summary, r.decision_snapshot
            from public.stock_snapshots s
            left join public.stock_recommendations r
              on r.update_run_id = s.update_run_id and r.ticker = s.ticker
            where s.update_run_id = :run_id and s.rank is not null
            order by s.rank asc, s.ticker
            limit 50
            """
        ),
        {"run_id": run_id},
    ).mappings().all()

    return {
        "update": dict(update_run),
        "regions": [dict(row) for row in regions],
        "sectors": [dict(row) for row in sectors],
        "top_stocks": [dict(row) for row in top_stocks],
    }
