import re
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from ..db import get_db

router = APIRouter(prefix="/api/history", tags=["history"])


def _selection(value: str | None, *, limit: int) -> list[str]:
    if not value:
        return []
    selected = []
    seen = set()
    for item in value.split(","):
        normalized = item.strip()
        if not normalized or normalized in seen:
            continue
        if len(normalized) > 80:
            raise HTTPException(status_code=422, detail="A selected value is too long")
        seen.add(normalized)
        selected.append(normalized)
    if len(selected) > limit:
        raise HTTPException(status_code=422, detail=f"Select no more than {limit} values")
    return selected


def _series(rows: list[dict], key: str) -> list[dict]:
    grouped: dict[str, list[dict]] = {}
    for row in rows:
        values = dict(row)
        name = values.pop(key)
        grouped.setdefault(name, []).append(values)
    return [{"name": name, "points": points} for name, points in grouped.items()]


@router.get("/groups")
def group_history(
    kind: Literal["sector", "region"],
    names: str | None = None,
    days: int = Query(default=30, ge=1, le=365),
    db: Session = Depends(get_db),
) -> dict:
    selected = _selection(names, limit=12)
    params = {"days": days, "names": selected}
    if kind == "sector":
        statement = text(
            """
            select s.sector as name, s.week_ending as market_date,
                   coalesce(u.completed_at, u.started_at) as observed_at,
                   s.direction, s.strength, s.raw_score
            from public.sector_snapshots s
            join public.update_runs u on u.id = s.update_run_id
            where u.status in ('success', 'partial')
              and coalesce(u.latest_price_date, u.market_data_date, s.week_ending)
                    >= current_date - (:days - 1)
              and (cardinality(cast(:names as text[])) = 0 or s.sector = any(cast(:names as text[])))
            order by s.sector, observed_at, s.id
            """
        )
    else:
        statement = text(
            """
            with group_rows as (
              select r.update_run_id, r.region as name, r.week_ending,
                     r.direction, r.strength, r.raw_score, r.id::text as row_order
              from public.region_snapshots r
              union all
              select s.update_run_id, 'NA' as name, max(s.week_ending),
                     case
                       when abs(avg(s.raw_score)) < 0.05 then 'Neutral'
                       when avg(s.raw_score) > 0 then 'Bullish'
                       else 'Bearish'
                     end as direction,
                     least(100, floor(abs(avg(s.raw_score)) * 100))::integer as strength,
                     avg(s.raw_score) as raw_score,
                     'NA' as row_order
              from public.sector_snapshots s
              group by s.update_run_id
            )
            select g.name, g.week_ending as market_date,
                   coalesce(u.completed_at, u.started_at) as observed_at,
                   g.direction, g.strength, g.raw_score
            from group_rows g
            join public.update_runs u on u.id = g.update_run_id
            where u.status in ('success', 'partial')
              and coalesce(u.latest_price_date, u.market_data_date, g.week_ending)
                    >= current_date - (:days - 1)
              and (cardinality(cast(:names as text[])) = 0 or g.name = any(cast(:names as text[])))
            order by g.name, observed_at, g.row_order
            """
        )

    try:
        rows = db.execute(statement, params).mappings().all()
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Historical group data is unavailable") from exc
    return {"kind": kind, "days": days, "series": _series(rows, "name")}


@router.get("/tickers")
def ticker_history(
    tickers: str = Query(min_length=1),
    days: int = Query(default=30, ge=1, le=365),
    db: Session = Depends(get_db),
) -> dict:
    selected = list(dict.fromkeys(ticker.upper() for ticker in _selection(tickers, limit=5)))
    if any(not re.fullmatch(r"[A-Z0-9.-]+", ticker) for ticker in selected):
        raise HTTPException(status_code=422, detail="A ticker contains unsupported characters")

    try:
        rows = db.execute(
            text(
                """
                with ranked as (
                  select s.*,
                         row_number() over (
                           partition by s.update_run_id, s.ticker
                           order by s.rank nulls last, s.created_at, s.id
                         ) as duplicate_rank
                  from public.stock_snapshots s
                  join public.update_runs u on u.id = s.update_run_id
                  where s.ticker = any(cast(:tickers as text[]))
                    and u.status in ('success', 'partial')
                    and coalesce(u.latest_price_date, u.market_data_date, s.price_date)
                          >= current_date - (:days - 1)
                )
                select s.ticker, s.company_name, s.price_date as market_date,
                       coalesce(u.completed_at, u.started_at) as observed_at,
                       s.current_price, s.previous_close, s.weekly_return,
                       s.daily_trading_percentile, s.latest_volume,
                       s.dollar_vol_latest, s.currency, s.region, s.market,
                       r.action, r.score, r.confidence, r.rationale
                from ranked s
                join public.update_runs u on u.id = s.update_run_id
                left join public.stock_recommendations r
                  on r.update_run_id = s.update_run_id and r.ticker = s.ticker
                where s.duplicate_rank = 1
                order by s.ticker, observed_at, s.id
                """
            ),
            {"days": days, "tickers": selected},
        ).mappings().all()
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Historical ticker data is unavailable") from exc
    return {"days": days, "series": _series(rows, "ticker")}
