from typing import Literal
from uuid import UUID

from fastapi import APIRouter, Depends
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import text
from sqlalchemy.orm import Session

from ..auth import require_user_id
from ..db import get_db

router = APIRouter(prefix="/api/me", tags=["preferences"])


class EmailReportSettings(BaseModel):
    enabled: bool = False
    frequency: Literal["every_update", "market_close"] = "market_close"
    scope: Literal["full", "watchlist"] = "full"


class NotificationSettings(BaseModel):
    model_config = ConfigDict(extra="allow")

    email_reports: EmailReportSettings = Field(default_factory=EmailReportSettings)


class PreferencesPayload(BaseModel):
    theme: str = Field(default="system", pattern="^(light|dark|system)$")
    default_region: str | None = None
    default_sector: str | None = None
    default_market: str | None = None
    visible_columns: list[str] = Field(default_factory=list)
    watchlist: list[str] = Field(default_factory=list)
    notification_settings: NotificationSettings = Field(default_factory=NotificationSettings)


@router.get("/preferences")
def get_preferences(
    user_id: UUID = Depends(require_user_id),
    db: Session = Depends(get_db),
) -> dict:
    row = db.execute(
        text(
            """
            select user_id, theme, default_region, default_sector, default_market,
                   visible_columns, watchlist, notification_settings
            from public.user_preferences
            where user_id = :user_id
            """
        ),
        {"user_id": user_id},
    ).mappings().first()

    if row is None:
        return {"user_id": str(user_id), **PreferencesPayload().model_dump()}

    result = dict(row)
    result["user_id"] = str(result["user_id"])
    result["notification_settings"] = NotificationSettings.model_validate(
        result["notification_settings"] or {}
    ).model_dump()
    return result


@router.put("/preferences")
def update_preferences(
    payload: PreferencesPayload,
    user_id: UUID = Depends(require_user_id),
    db: Session = Depends(get_db),
) -> dict:
    row = db.execute(
        text(
            """
            insert into public.user_preferences (
              user_id, theme, default_region, default_sector, default_market,
              visible_columns, watchlist, notification_settings
            )
            values (
              :user_id, :theme, :default_region, :default_sector, :default_market,
              :visible_columns, :watchlist, cast(:notification_settings as jsonb)
            )
            on conflict (user_id) do update set
              theme = excluded.theme,
              default_region = excluded.default_region,
              default_sector = excluded.default_sector,
              default_market = excluded.default_market,
              visible_columns = excluded.visible_columns,
              watchlist = excluded.watchlist,
              notification_settings = excluded.notification_settings,
              updated_at = now()
            returning user_id, theme, default_region, default_sector, default_market,
                      visible_columns, watchlist, notification_settings
            """
        ),
        {
            "user_id": user_id,
            "theme": payload.theme,
            "default_region": payload.default_region,
            "default_sector": payload.default_sector,
            "default_market": payload.default_market,
            "visible_columns": payload.visible_columns,
            "watchlist": payload.watchlist,
            "notification_settings": payload.notification_settings.model_dump_json(),
        },
    ).mappings().one()
    db.commit()

    result = dict(row)
    result["user_id"] = str(result["user_id"])
    result["notification_settings"] = NotificationSettings.model_validate(
        result["notification_settings"] or {}
    ).model_dump()
    return result
