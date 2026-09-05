from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Response, status
from pydantic import BaseModel, Field, field_validator, model_validator
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from ..auth import require_user_id
from ..db import get_db

router = APIRouter(prefix="/api/me", tags=["personalization"])

DEFAULT_SCORING_WEIGHTS = {
    "group_sentiment_weight": 1.0,
    "pe_weight": 1.0,
    "pb_weight": 1.0,
    "peg_weight": 1.0,
    "dividend_weight": 1.0,
    "momentum_weight": 1.0,
}


def _normalize_tickers(tickers: list[str]) -> list[str]:
    normalized = []
    seen = set()
    for ticker in tickers:
        value = ticker.strip().upper()
        if not value or value in seen:
            continue
        if len(value) > 32:
            raise ValueError("Tickers must be 32 characters or fewer")
        seen.add(value)
        normalized.append(value)
    return normalized


class WatchlistCreate(BaseModel):
    name: str = Field(min_length=1, max_length=60)
    tickers: list[str] = Field(default_factory=list, max_length=500)
    is_default: bool = False

    @field_validator("name")
    @classmethod
    def normalize_name(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("Watchlist name cannot be empty")
        return normalized

    @field_validator("tickers")
    @classmethod
    def normalize_tickers(cls, value: list[str]) -> list[str]:
        return _normalize_tickers(value)


class WatchlistUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=60)
    tickers: list[str] | None = Field(default=None, max_length=500)
    is_default: bool | None = None

    @field_validator("name")
    @classmethod
    def normalize_name(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            raise ValueError("Watchlist name cannot be empty")
        return normalized

    @field_validator("tickers")
    @classmethod
    def normalize_tickers(cls, value: list[str] | None) -> list[str] | None:
        return None if value is None else _normalize_tickers(value)


class ScoringWeightsPayload(BaseModel):
    group_sentiment_weight: float = Field(default=1.0, ge=0, le=2)
    pe_weight: float = Field(default=1.0, ge=0, le=2)
    pb_weight: float = Field(default=1.0, ge=0, le=2)
    peg_weight: float = Field(default=1.0, ge=0, le=2)
    dividend_weight: float = Field(default=1.0, ge=0, le=2)
    momentum_weight: float = Field(default=1.0, ge=0, le=2)

    @model_validator(mode="after")
    def require_active_factor(self) -> "ScoringWeightsPayload":
        if sum(self.model_dump().values()) <= 0:
            raise ValueError("At least one scoring factor must be greater than zero")
        return self


def _watchlist_rows(db: Session, user_id: UUID) -> list[dict]:
    rows = db.execute(
        text(
            """
            select w.id, w.name, w.is_default, w.created_at, w.updated_at,
                   coalesce(
                     array_agg(i.ticker order by i.ticker)
                       filter (where i.ticker is not null),
                     array[]::text[]
                   ) as tickers
            from public.user_watchlists w
            left join public.user_watchlist_items i
              on i.watchlist_id = w.id and i.user_id = w.user_id
            where w.user_id = :user_id
            group by w.id
            order by w.is_default desc, lower(w.name), w.created_at
            """
        ),
        {"user_id": user_id},
    ).mappings().all()
    return [{**dict(row), "id": str(row["id"])} for row in rows]


def _replace_watchlist_items(
    db: Session,
    watchlist_id: UUID,
    user_id: UUID,
    tickers: list[str],
) -> None:
    db.execute(
        text(
            """
            delete from public.user_watchlist_items
            where watchlist_id = :watchlist_id and user_id = :user_id
            """
        ),
        {"watchlist_id": watchlist_id, "user_id": user_id},
    )
    if tickers:
        db.execute(
            text(
                """
                insert into public.user_watchlist_items (watchlist_id, user_id, ticker)
                select :watchlist_id, :user_id, ticker
                from unnest(cast(:tickers as text[])) as ticker
                """
            ),
            {
                "watchlist_id": watchlist_id,
                "user_id": user_id,
                "tickers": tickers,
            },
        )


@router.get("/watchlists")
def get_watchlists(
    user_id: UUID = Depends(require_user_id),
    db: Session = Depends(get_db),
) -> list[dict]:
    return _watchlist_rows(db, user_id)


@router.post("/watchlists", status_code=status.HTTP_201_CREATED)
def create_watchlist(
    payload: WatchlistCreate,
    user_id: UUID = Depends(require_user_id),
    db: Session = Depends(get_db),
) -> dict:
    has_watchlists = db.execute(
        text("select exists(select 1 from public.user_watchlists where user_id = :user_id)"),
        {"user_id": user_id},
    ).scalar_one()
    make_default = payload.is_default or not has_watchlists
    try:
        if make_default:
            db.execute(
                text("update public.user_watchlists set is_default = false where user_id = :user_id"),
                {"user_id": user_id},
            )
        watchlist_id = db.execute(
            text(
                """
                insert into public.user_watchlists (user_id, name, is_default)
                values (:user_id, :name, :is_default)
                returning id
                """
            ),
            {
                "user_id": user_id,
                "name": payload.name,
                "is_default": make_default,
            },
        ).scalar_one()
        _replace_watchlist_items(db, watchlist_id, user_id, payload.tickers)
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise HTTPException(status_code=409, detail="A watchlist with that name already exists") from exc

    return next(row for row in _watchlist_rows(db, user_id) if row["id"] == str(watchlist_id))


@router.put("/watchlists/{watchlist_id}")
def update_watchlist(
    watchlist_id: UUID,
    payload: WatchlistUpdate,
    user_id: UUID = Depends(require_user_id),
    db: Session = Depends(get_db),
) -> dict:
    exists = db.execute(
        text(
            """
            select exists(
              select 1 from public.user_watchlists
              where id = :watchlist_id and user_id = :user_id
            )
            """
        ),
        {"watchlist_id": watchlist_id, "user_id": user_id},
    ).scalar_one()
    if not exists:
        raise HTTPException(status_code=404, detail="Watchlist not found")

    try:
        if payload.is_default:
            db.execute(
                text("update public.user_watchlists set is_default = false where user_id = :user_id"),
                {"user_id": user_id},
            )
        db.execute(
            text(
                """
                update public.user_watchlists
                set name = case when :has_name then :name else name end,
                    is_default = case when :has_default then :is_default else is_default end
                where id = :watchlist_id and user_id = :user_id
                """
            ),
            {
                "has_name": payload.name is not None,
                "name": payload.name,
                "has_default": payload.is_default is not None,
                "is_default": payload.is_default,
                "watchlist_id": watchlist_id,
                "user_id": user_id,
            },
        )
        if payload.tickers is not None:
            _replace_watchlist_items(db, watchlist_id, user_id, payload.tickers)
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise HTTPException(status_code=409, detail="A watchlist with that name already exists") from exc

    return next(row for row in _watchlist_rows(db, user_id) if row["id"] == str(watchlist_id))


@router.delete("/watchlists/{watchlist_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_watchlist(
    watchlist_id: UUID,
    user_id: UUID = Depends(require_user_id),
    db: Session = Depends(get_db),
) -> Response:
    deleted = db.execute(
        text(
            """
            delete from public.user_watchlists
            where id = :watchlist_id and user_id = :user_id
            returning is_default
            """
        ),
        {"watchlist_id": watchlist_id, "user_id": user_id},
    ).scalar_one_or_none()
    if deleted is None:
        db.rollback()
        raise HTTPException(status_code=404, detail="Watchlist not found")
    if deleted:
        db.execute(
            text(
                """
                update public.user_watchlists
                set is_default = true
                where id = (
                  select id from public.user_watchlists
                  where user_id = :user_id
                  order by updated_at desc, created_at
                  limit 1
                )
                """
            ),
            {"user_id": user_id},
        )
    db.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


def _scoring_response(user_id: UUID, values: dict) -> dict:
    return {
        "user_id": str(user_id),
        **{key: float(values[key]) for key in DEFAULT_SCORING_WEIGHTS},
    }


@router.get("/scoring-weights")
def get_scoring_weights(
    user_id: UUID = Depends(require_user_id),
    db: Session = Depends(get_db),
) -> dict:
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
    return _scoring_response(user_id, dict(row) if row else DEFAULT_SCORING_WEIGHTS)


@router.put("/scoring-weights")
def update_scoring_weights(
    payload: ScoringWeightsPayload,
    user_id: UUID = Depends(require_user_id),
    db: Session = Depends(get_db),
) -> dict:
    values = payload.model_dump()
    row = db.execute(
        text(
            """
            insert into public.user_scoring_weights (
              user_id, group_sentiment_weight, pe_weight, pb_weight, peg_weight,
              dividend_weight, momentum_weight
            )
            values (
              :user_id, :group_sentiment_weight, :pe_weight, :pb_weight, :peg_weight,
              :dividend_weight, :momentum_weight
            )
            on conflict (user_id) do update set
              group_sentiment_weight = excluded.group_sentiment_weight,
              pe_weight = excluded.pe_weight,
              pb_weight = excluded.pb_weight,
              peg_weight = excluded.peg_weight,
              dividend_weight = excluded.dividend_weight,
              momentum_weight = excluded.momentum_weight,
              updated_at = now()
            returning group_sentiment_weight, pe_weight, pb_weight, peg_weight,
                      dividend_weight, momentum_weight
            """
        ),
        {"user_id": user_id, **values},
    ).mappings().one()
    db.commit()
    return _scoring_response(user_id, dict(row))


@router.delete("/scoring-weights")
def reset_scoring_weights(
    user_id: UUID = Depends(require_user_id),
    db: Session = Depends(get_db),
) -> dict:
    db.execute(
        text("delete from public.user_scoring_weights where user_id = :user_id"),
        {"user_id": user_id},
    )
    db.commit()
    return _scoring_response(user_id, DEFAULT_SCORING_WEIGHTS)
