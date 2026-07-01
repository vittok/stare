from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, text

from stare_signals import enrich_stock_signals


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing report artifact: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _env_value(name: str, env_path: Path = Path(".env")) -> str | None:
    if os.getenv(name):
        return os.getenv(name)
    if not env_path.exists():
        return None

    for line in env_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        if key.strip() == name:
            return value.strip().strip('"').strip("'")
    return None


def _db_url(value: str) -> str:
    if value.startswith("postgresql://"):
        return value.replace("postgresql://", "postgresql+psycopg://", 1)
    return value


def _source_commit() -> str | None:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except Exception:
        return None


def _num(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(out) else out


def _int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    value = str(value)
    return value if value and value.lower() != "nan" else None


def _json(value: Any) -> str:
    return json.dumps(_clean_json(value or {}), allow_nan=False)


def _clean_json(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _clean_json(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_clean_json(v) for v in value]
    if isinstance(value, float) and math.isnan(value):
        return None
    return value


def _date(value: Any) -> str | None:
    return _text(value)


def _latest_price_date(*reports: dict[str, Any]) -> str | None:
    dates = [
        report.get("market_data", {}).get("latest_price_date")
        for report in reports
        if report.get("market_data", {}).get("latest_price_date")
    ]
    return max(dates) if dates else None


def _iter_sector_stocks(sector: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for stock in sector.get("top10_active", []):
        rows.append(
            {
                "stock": stock,
                "sector": sector.get("sector"),
                "region": "NA",
                "market": "S&P 500",
                "country": "United States",
                "rank": stock.get("rank"),
            }
        )
    return rows


def _iter_region_stocks(region: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()

    def add(stock: dict[str, Any], market: str | None, country: str | None) -> None:
        key = (
            region.get("region"),
            market or stock.get("market"),
            stock.get("ticker"),
            stock.get("rank"),
        )
        if key in seen:
            return
        seen.add(key)
        rows.append(
            {
                "stock": stock,
                "sector": stock.get("sector") or region.get("sector") or region.get("region"),
                "region": stock.get("region") or region.get("region"),
                "market": stock.get("market") or market,
                "country": stock.get("country") or country,
                "rank": stock.get("rank"),
            }
        )

    for market in region.get("markets", []):
        for stock in market.get("top10_active", []):
            add(stock, market.get("market"), market.get("country"))

    for stock in region.get("top10_active", []):
        add(stock, stock.get("market"), stock.get("country"))

    return rows


def _stock_params(update_run_id: str, row: dict[str, Any]) -> dict[str, Any]:
    stock = row["stock"]
    fundamentals = stock.get("fundamentals") or {}
    return {
        "update_run_id": update_run_id,
        "ticker": stock.get("ticker"),
        "company_name": fundamentals.get("shortName"),
        "sector": row.get("sector"),
        "region": row.get("region"),
        "market": row.get("market"),
        "country": row.get("country"),
        "rank": _int(row.get("rank")),
        "volume_date": _date(stock.get("volume_date")),
        "price_date": _date(stock.get("priceDate")),
        "current_price": _num(stock.get("currentPrice")),
        "previous_close": _num(stock.get("previousClose")),
        "previous_close_date": _date(stock.get("previousCloseDate")),
        "close_change": _num(stock.get("closeChange")),
        "close_change_pct": _num(stock.get("closeChangePct")),
        "close_direction": _text(stock.get("closeDirection")),
        "weekly_return": _num(stock.get("weekly_return")),
        "dollar_vol_latest": _num(stock.get("dollar_vol_latest")),
        "latest_volume": _num(stock.get("latest_volume")),
        "dollar_vol_week": _num(stock.get("dollar_vol_week")),
        "vol_ratio": _num(stock.get("vol_ratio")),
        "daily_trading_percentile": _num(stock.get("daily_trading_percentile")),
        "market_cap": _num(fundamentals.get("marketCap")),
        "trailing_pe": _num(fundamentals.get("trailingPE")),
        "forward_pe": _num(fundamentals.get("forwardPE")),
        "price_to_book": _num(fundamentals.get("priceToBook")),
        "peg_ratio": _num(fundamentals.get("pegRatio")),
        "dividend_yield": _num(fundamentals.get("dividendYield")),
        "currency": _text(fundamentals.get("currency")),
        "exchange": _text(fundamentals.get("exchange")),
        "industry": _text(fundamentals.get("industry")),
        "fundamentals": _json(fundamentals),
    }


def _recommendation_params(update_run_id: str, stock: dict[str, Any]) -> dict[str, Any]:
    recommendation = stock.get("recommendation") or {}
    return {
        "update_run_id": update_run_id,
        "ticker": stock.get("ticker"),
        "action": recommendation.get("action") or "Hold",
        "score": _num(recommendation.get("score")),
        "confidence": _int(recommendation.get("confidence")),
        "rationale": _text(recommendation.get("rationale")),
        "decision_snapshot": _json(stock.get("decision_snapshot")),
        "daily_summary": _text(stock.get("daily_summary")),
    }


def export_reports(
    database_url: str,
    sector_report_path: Path,
    region_report_path: Path,
    run_label: str,
) -> str:
    sector_report = _load_json(sector_report_path)
    region_report = _load_json(region_report_path)
    engine = create_engine(_db_url(database_url), pool_pre_ping=True)
    started_at = datetime.now(UTC)
    latest_price_date = _latest_price_date(sector_report, region_report)

    with engine.begin() as conn:
        update_run = conn.execute(
            text(
                """
                insert into public.update_runs (
                  run_label, triggered_by, status, started_at, completed_at,
                  market_data_date, latest_price_date, source_commit, diagnostics
                )
                values (
                  :run_label, 'artifact_import', 'started', :started_at, null,
                  :market_data_date, :latest_price_date, :source_commit,
                  cast(:diagnostics as jsonb)
                )
                returning id
                """
            ),
            {
                "run_label": run_label,
                "started_at": started_at,
                "market_data_date": latest_price_date,
                "latest_price_date": latest_price_date,
                "source_commit": _source_commit(),
                "diagnostics": _json(
                    {
                        "sector_report": str(sector_report_path),
                        "region_report": str(region_report_path),
                    }
                ),
            },
        ).mappings().one()
        update_run_id = str(update_run["id"])

        for sector in sector_report.get("sectors", []):
            conn.execute(
                text(
                    """
                    insert into public.sector_snapshots (
                      update_run_id, sector, week_ending, direction, strength,
                      raw_score, diagnostics
                    )
                    values (
                      :update_run_id, :sector, :week_ending, :direction, :strength,
                      :raw_score, cast(:diagnostics as jsonb)
                    )
                    on conflict (update_run_id, sector) do nothing
                    """
                ),
                {
                    "update_run_id": update_run_id,
                    "sector": sector.get("sector"),
                    "week_ending": _date(sector.get("week_ending")),
                    "direction": sector.get("direction"),
                    "strength": _int(sector.get("strength")),
                    "raw_score": _num(sector.get("raw_score")),
                    "diagnostics": _json(sector.get("diagnostics")),
                },
            )
            for row in _iter_sector_stocks(sector):
                enrich_stock_signals(sector, row["stock"])
                conn.execute(_stock_insert_sql(), _stock_params(update_run_id, row))
                conn.execute(_recommendation_insert_sql(), _recommendation_params(update_run_id, row["stock"]))

        for region in region_report.get("regions", []):
            conn.execute(
                text(
                    """
                    insert into public.region_snapshots (
                      update_run_id, region, week_ending, direction, strength,
                      raw_score, diagnostics
                    )
                    values (
                      :update_run_id, :region, :week_ending, :direction, :strength,
                      :raw_score, cast(:diagnostics as jsonb)
                    )
                    on conflict (update_run_id, region) do nothing
                    """
                ),
                {
                    "update_run_id": update_run_id,
                    "region": region.get("region"),
                    "week_ending": _date(region.get("week_ending")),
                    "direction": region.get("direction"),
                    "strength": _int(region.get("strength")),
                    "raw_score": _num(region.get("raw_score")),
                    "diagnostics": _json(region.get("diagnostics")),
                },
            )
            for row in _iter_region_stocks(region):
                enrich_stock_signals(region, row["stock"])
                conn.execute(_stock_insert_sql(), _stock_params(update_run_id, row))
                conn.execute(_recommendation_insert_sql(), _recommendation_params(update_run_id, row["stock"]))

        conn.execute(
            text(
                """
                update public.update_runs
                set status = 'success',
                    completed_at = :completed_at
                where id = :update_run_id
                """
            ),
            {"completed_at": datetime.now(UTC), "update_run_id": update_run_id},
        )

    return update_run_id


def _stock_insert_sql():
    return text(
        """
        insert into public.stock_snapshots (
          update_run_id, ticker, company_name, sector, region, market, country, rank,
          volume_date, price_date, current_price, previous_close, previous_close_date,
          close_change, close_change_pct, close_direction, weekly_return,
          dollar_vol_latest, latest_volume, dollar_vol_week, vol_ratio,
          daily_trading_percentile, market_cap, trailing_pe, forward_pe,
          price_to_book, peg_ratio, dividend_yield, currency, exchange, industry,
          fundamentals
        )
        values (
          :update_run_id, :ticker, :company_name, :sector, :region, :market, :country, :rank,
          :volume_date, :price_date, :current_price, :previous_close, :previous_close_date,
          :close_change, :close_change_pct, :close_direction, :weekly_return,
          :dollar_vol_latest, :latest_volume, :dollar_vol_week, :vol_ratio,
          :daily_trading_percentile, :market_cap, :trailing_pe, :forward_pe,
          :price_to_book, :peg_ratio, :dividend_yield, :currency, :exchange, :industry,
          cast(:fundamentals as jsonb)
        )
        """
    )


def _recommendation_insert_sql():
    return text(
        """
        insert into public.stock_recommendations (
          update_run_id, ticker, action, score, confidence, rationale,
          decision_snapshot, daily_summary
        )
        values (
          :update_run_id, :ticker, :action, :score, :confidence, :rationale,
          cast(:decision_snapshot as jsonb), :daily_summary
        )
        on conflict (update_run_id, ticker) do update set
          action = excluded.action,
          score = excluded.score,
          confidence = excluded.confidence,
          rationale = excluded.rationale,
          decision_snapshot = excluded.decision_snapshot,
          daily_summary = excluded.daily_summary
        """
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Export S.T.A.R.E JSON reports to Supabase Postgres.")
    parser.add_argument("--database-url", default=_env_value("DATABASE_URL"))
    parser.add_argument("--sector-report", type=Path, default=Path("reports/sector_dashboard.json"))
    parser.add_argument("--region-report", type=Path, default=Path("reports/region_dashboard.json"))
    parser.add_argument("--run-label", default="manual artifact import")
    args = parser.parse_args()

    if not args.database_url:
        raise SystemExit("DATABASE_URL is required.")

    update_run_id = export_reports(
        database_url=args.database_url,
        sector_report_path=args.sector_report,
        region_report_path=args.region_report,
        run_label=args.run_label,
    )
    print(f"Created update_run_id={update_run_id}")


if __name__ == "__main__":
    main()
