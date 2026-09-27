from uuid import UUID

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from ..auth import require_user_id
from ..db import get_db
from ..market_lookup import analyze_stock, search_stocks
from ..scoring import DEFAULT_WEIGHTS

router = APIRouter(prefix="/api/stocks", tags=["stocks"])


def _user_weights(db: Session, user_id: UUID) -> dict:
    row = db.execute(
        text(
            """
            select group_sentiment_weight, pe_weight, pb_weight, peg_weight,
                   dividend_weight, momentum_weight
            from public.user_scoring_weights
            where user_id = :user_id
            """
        ),
        {"user_id": user_id},
    ).mappings().first()
    return dict(row) if row else DEFAULT_WEIGHTS


@router.get("/search")
async def stock_search(
    q: str = Query(min_length=1, max_length=80),
    _: UUID = Depends(require_user_id),
) -> dict:
    try:
        return {"results": await search_stocks(q)}
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=503, detail="Market search is temporarily unavailable") from exc


@router.get("/{symbol}/analysis")
async def stock_analysis(
    symbol: str,
    user_id: UUID = Depends(require_user_id),
    db: Session = Depends(get_db),
) -> dict:
    try:
        return await analyze_stock(symbol, _user_weights(db, user_id))
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=503, detail="Market analysis is temporarily unavailable") from exc
