from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, text

from stare_signals import enrich_stock_signals


EXPECTED_SECTORS = {
    "Communication Services",
    "Consumer Discretionary",
    "Consumer Staples",
    "Energy",
    "Financials",
    "Health Care",
    "Industrials",
    "Information Technology",
    "Materials",
    "Real Estate",
    "Utilities",
}
EXPECTED_REGIONS = {"APAC", "EMEA", "LAC"}
VALID_DIRECTIONS = {"Bullish", "Bearish", "Neutral"}
VALID_ACTIONS = {"Buy", "Hold", "Sell"}
MIN_STOCKS_PER_GROUP = 3
MIN_FIELD_COVERAGE = 0.90


class ReportValidationError(ValueError):
    def __init__(self, errors: list[str]):
        self.errors = errors
        super().__init__("; ".join(errors))


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


def resolve_database_url(
    explicit_url: str | None = None,
    env_path: Path = Path(".env"),
) -> str | None:
    return explicit_url or _env_value("DATABASE_URL", env_path)


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
    return out if math.isfinite(out) else None


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
    if isinstance(value, float) and not math.isfinite(value):
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
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _date(value: Any) -> str | None:
    return _text(value)


def _parsed_date(value: Any, label: str, errors: list[str]) -> date | None:
    normalized = _date(value)
    if not normalized:
        errors.append(f"{label} is missing")
        return None
    try:
        return date.fromisoformat(normalized[:10])
    except ValueError:
        errors.append(f"{label} is not an ISO date")
        return None


def _is_iso_date(value: Any) -> bool:
    normalized = _date(value)
    if not normalized:
        return False
    try:
        date.fromisoformat(normalized[:10])
        return True
    except ValueError:
        return False


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


def validate_reports(
    sector_report: dict[str, Any],
    region_report: dict[str, Any],
    max_data_age_days: int | None = None,
) -> dict[str, Any]:
    errors: list[str] = []
    sectors = sector_report.get("sectors")
    regions = region_report.get("regions")
    if not isinstance(sectors, list):
        sectors = []
        errors.append("sector report does not contain a sectors list")
    if not isinstance(regions, list):
        regions = []
        errors.append("region report does not contain a regions list")

    sector_names = [
        name
        for item in sectors
        if isinstance(item, dict)
        for name in [_text(item.get("sector"))]
        if name
    ]
    region_names = [
        name
        for item in regions
        if isinstance(item, dict)
        for name in [_text(item.get("region"))]
        if name
    ]
    if len(sector_names) != sum(isinstance(item, dict) for item in sectors):
        errors.append("sector report contains an unnamed sector group")
    if len(region_names) != sum(isinstance(item, dict) for item in regions):
        errors.append("region report contains an unnamed region group")
    if len(sector_names) != len(set(sector_names)):
        errors.append("sector report contains duplicate sector groups")
    if len(region_names) != len(set(region_names)):
        errors.append("region report contains duplicate region groups")

    missing_sectors = sorted(EXPECTED_SECTORS - set(sector_names))
    unexpected_sectors = sorted(set(sector_names) - EXPECTED_SECTORS)
    if missing_sectors:
        errors.append(f"missing sectors: {', '.join(missing_sectors)}")
    if unexpected_sectors:
        errors.append(f"unexpected sectors: {', '.join(unexpected_sectors)}")

    missing_regions = sorted(EXPECTED_REGIONS - set(region_names))
    unexpected_regions = sorted(set(region_names) - EXPECTED_REGIONS)
    if missing_regions:
        errors.append(f"missing regions: {', '.join(missing_regions)}")
    if unexpected_regions:
        errors.append(f"unexpected regions: {', '.join(unexpected_regions)}")

    all_rows: list[dict[str, Any]] = []
    group_dates: list[date] = []
    for kind, groups, name_key, iterator in (
        ("sector", sectors, "sector", _iter_sector_stocks),
        ("region", regions, "region", _iter_region_stocks),
    ):
        for group in groups:
            if not isinstance(group, dict):
                errors.append(f"{kind} report contains a non-object group")
                continue
            name = _text(group.get(name_key)) or "unnamed"
            direction = group.get("direction")
            strength = _num(group.get("strength"))
            if direction not in VALID_DIRECTIONS:
                errors.append(f"{kind} {name} has an invalid direction")
            if strength is None or not 0 <= strength <= 100:
                errors.append(f"{kind} {name} has strength outside 0-100")
            group_date = _parsed_date(group.get("week_ending"), f"{kind} {name} week_ending", errors)
            if group_date:
                group_dates.append(group_date)

            rows = iterator(group)
            if len(rows) < MIN_STOCKS_PER_GROUP:
                errors.append(
                    f"{kind} {name} has {len(rows)} stocks; at least {MIN_STOCKS_PER_GROUP} are required"
                )
            tickers = [_text(row["stock"].get("ticker")) for row in rows]
            if any(ticker is None for ticker in tickers):
                errors.append(f"{kind} {name} contains a stock without a ticker")
            for row in rows:
                enrich_stock_signals(group, row["stock"])
            all_rows.extend(rows)

    sector_market_date = _parsed_date(
        sector_report.get("market_data", {}).get("latest_price_date"),
        "sector latest_price_date",
        errors,
    )
    region_market_date = _parsed_date(
        region_report.get("market_data", {}).get("latest_price_date"),
        "region latest_price_date",
        errors,
    )
    market_dates = [value for value in (sector_market_date, region_market_date) if value]
    if len(market_dates) == 2 and abs((market_dates[0] - market_dates[1]).days) > 7:
        errors.append("sector and region market dates differ by more than 7 days")

    latest_market_date = max(market_dates) if market_dates else None
    if max_data_age_days is not None and latest_market_date:
        age_days = (datetime.now(UTC).date() - latest_market_date).days
        if age_days > max_data_age_days:
            errors.append(
                f"latest market data is {age_days} days old; maximum is {max_data_age_days}"
            )
        if age_days < -1:
            errors.append("latest market data is dated in the future")

    priced_rows = 0
    active_rows = 0
    recommendation_tickers: set[str] = set()
    for row in all_rows:
        stock = row["stock"]
        ticker = _text(stock.get("ticker"))
        current_price = _num(stock.get("currentPrice"))
        dollar_volume = _num(stock.get("dollar_vol_latest"))
        latest_volume = _num(stock.get("latest_volume"))
        if current_price is not None and current_price > 0 and _is_iso_date(stock.get("priceDate")):
            priced_rows += 1
        if (
            dollar_volume is not None
            and dollar_volume > 0
            and latest_volume is not None
            and latest_volume > 0
        ):
            active_rows += 1
        recommendation = stock.get("recommendation") or {}
        confidence = _int(recommendation.get("confidence"))
        if (
            recommendation.get("action") not in VALID_ACTIONS
            or _num(recommendation.get("score")) is None
            or confidence is None
            or not 0 <= confidence <= 100
            or not _text(recommendation.get("rationale"))
            or not _text(stock.get("daily_summary"))
        ):
            errors.append(f"{ticker or 'unnamed stock'} has an invalid generated signal")
        if ticker:
            recommendation_tickers.add(ticker)

    total_rows = len(all_rows)
    price_coverage = priced_rows / total_rows if total_rows else 0.0
    activity_coverage = active_rows / total_rows if total_rows else 0.0
    if price_coverage < MIN_FIELD_COVERAGE:
        errors.append(f"price coverage is {price_coverage:.1%}; minimum is {MIN_FIELD_COVERAGE:.0%}")
    if activity_coverage < MIN_FIELD_COVERAGE:
        errors.append(
            f"trading activity coverage is {activity_coverage:.1%}; minimum is {MIN_FIELD_COVERAGE:.0%}"
        )

    validation = {
        "sector_count": len(sectors),
        "region_count": len(regions),
        "sector_stock_rows": sum(len(_iter_sector_stocks(item)) for item in sectors),
        "region_stock_rows": sum(len(_iter_region_stocks(item)) for item in regions),
        "stock_rows": total_rows,
        "recommendation_rows": len(recommendation_tickers),
        "unique_tickers": len(recommendation_tickers),
        "price_coverage": round(price_coverage, 4),
        "activity_coverage": round(activity_coverage, 4),
        "latest_price_date": latest_market_date.isoformat() if latest_market_date else None,
        "group_date_count": len(group_dates),
    }
    if errors:
        raise ReportValidationError(errors)
    return validation


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


def _base_diagnostics(sector_report_path: Path, region_report_path: Path) -> dict[str, Any]:
    return {
        "sector_report": str(sector_report_path),
        "region_report": str(region_report_path),
    }


def _create_update_run(
    engine: Any,
    run_label: str,
    triggered_by: str,
    started_at: datetime,
    diagnostics: dict[str, Any],
) -> str:
    with engine.begin() as conn:
        row = conn.execute(
            text(
                """
                insert into public.update_runs (
                  run_label, triggered_by, status, started_at, completed_at,
                  market_data_date, latest_price_date, source_commit, diagnostics
                )
                values (
                  :run_label, :triggered_by, 'started', :started_at, null,
                  null, null, :source_commit, cast(:diagnostics as jsonb)
                )
                returning id
                """
            ),
            {
                "run_label": run_label,
                "triggered_by": triggered_by,
                "started_at": started_at,
                "source_commit": _source_commit(),
                "diagnostics": _json(diagnostics),
            },
        ).mappings().one()
    return str(row["id"])


def _failure_diagnostics(
    diagnostics: dict[str, Any],
    stage: str,
    exc: Exception,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    failure: dict[str, Any] = {
        "stage": stage,
        "partial_data": bool(diagnostics.get("partial_data", True)),
        "error_type": type(exc).__name__,
        "message": str(exc)[:2000],
    }
    if isinstance(exc, ReportValidationError):
        failure["validation_errors"] = exc.errors[:100]
    return {**diagnostics, **(extra or {}), "failure": failure}


def _mark_update_failed(
    engine: Any,
    update_run_id: str,
    stage: str,
    exc: Exception,
    diagnostics: dict[str, Any],
) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                update public.update_runs
                set status = 'failed',
                    completed_at = :completed_at,
                    diagnostics = cast(:diagnostics as jsonb)
                where id = :update_run_id
                """
            ),
            {
                "completed_at": datetime.now(UTC),
                "diagnostics": _json(_failure_diagnostics(diagnostics, stage, exc)),
                "update_run_id": update_run_id,
            },
        )


def record_failed_update(
    database_url: str,
    run_label: str,
    triggered_by: str,
    stage: str,
    message: str,
    diagnostics: dict[str, Any] | None = None,
    started_at: datetime | None = None,
) -> str:
    engine = create_engine(_db_url(database_url), pool_pre_ping=True)
    base = diagnostics or {}
    try:
        update_run_id = _create_update_run(
            engine,
            run_label,
            triggered_by,
            started_at or datetime.now(UTC),
            base,
        )
        _mark_update_failed(engine, update_run_id, stage, RuntimeError(message), base)
        return update_run_id
    finally:
        engine.dispose()


def _database_counts(conn: Any, update_run_id: str) -> dict[str, int]:
    row = conn.execute(
        text(
            """
            select
              (select count(*) from public.sector_snapshots where update_run_id = :id) as sector_count,
              (select count(*) from public.region_snapshots where update_run_id = :id) as region_count,
              (select count(*) from public.stock_snapshots where update_run_id = :id) as stock_rows,
              (select count(*) from public.stock_recommendations where update_run_id = :id) as recommendation_rows
            """
        ),
        {"id": update_run_id},
    ).mappings().one()
    return {key: int(row[key]) for key in row.keys()}


def _validate_database_counts(actual: dict[str, int], expected: dict[str, Any]) -> None:
    errors = [
        f"database {key} is {actual.get(key)}; expected {expected[key]}"
        for key in ("sector_count", "region_count", "stock_rows", "recommendation_rows")
        if actual.get(key) != expected[key]
    ]
    if errors:
        raise ReportValidationError(errors)


def export_reports(
    database_url: str,
    sector_report_path: Path,
    region_report_path: Path,
    run_label: str,
    triggered_by: str = "artifact_import",
    max_data_age_days: int | None = None,
) -> str:
    engine = create_engine(_db_url(database_url), pool_pre_ping=True)
    started_at = datetime.now(UTC)
    diagnostics = _base_diagnostics(sector_report_path, region_report_path)
    update_run_id: str | None = None
    stage = "artifact_loading"
    try:
        update_run_id = _create_update_run(
            engine, run_label, triggered_by, started_at, diagnostics
        )
        sector_report = _load_json(sector_report_path)
        region_report = _load_json(region_report_path)
        stage = "report_validation"
        validation = validate_reports(
            sector_report,
            region_report,
            max_data_age_days=max_data_age_days,
        )
        diagnostics["validation"] = validation
        latest_price_date = _latest_price_date(sector_report, region_report)

        stage = "snapshot_persistence"
        with engine.begin() as conn:
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
                    conn.execute(_stock_insert_sql(), _stock_params(update_run_id, row))
                    conn.execute(
                        _recommendation_insert_sql(),
                        _recommendation_params(update_run_id, row["stock"]),
                    )

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
                    conn.execute(_stock_insert_sql(), _stock_params(update_run_id, row))
                    conn.execute(
                        _recommendation_insert_sql(),
                        _recommendation_params(update_run_id, row["stock"]),
                    )

            stage = "database_validation"
            database_counts = _database_counts(conn, update_run_id)
            _validate_database_counts(database_counts, validation)
            diagnostics["database_counts"] = database_counts
            conn.execute(
                text(
                    """
                    update public.update_runs
                    set status = 'success',
                        completed_at = :completed_at,
                        market_data_date = :market_data_date,
                        latest_price_date = :latest_price_date,
                        diagnostics = cast(:diagnostics as jsonb)
                    where id = :update_run_id
                    """
                ),
                {
                    "completed_at": datetime.now(UTC),
                    "market_data_date": latest_price_date,
                    "latest_price_date": latest_price_date,
                    "diagnostics": _json(diagnostics),
                    "update_run_id": update_run_id,
                },
            )
        return update_run_id
    except Exception as exc:
        if update_run_id:
            _mark_update_failed(engine, update_run_id, stage, exc, diagnostics)
        raise
    finally:
        engine.dispose()


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
    parser.add_argument("--database-url", default=resolve_database_url())
    parser.add_argument("--sector-report", type=Path, default=Path("reports/sector_dashboard.json"))
    parser.add_argument("--region-report", type=Path, default=Path("reports/region_dashboard.json"))
    parser.add_argument("--run-label", default="manual artifact import")
    parser.add_argument("--triggered-by", default="artifact_import")
    parser.add_argument(
        "--max-data-age-days",
        type=int,
        default=None,
        help="Reject reports whose newest market date is older than this many days.",
    )
    args = parser.parse_args()

    if not args.database_url:
        raise SystemExit("DATABASE_URL is required.")

    update_run_id = export_reports(
        database_url=args.database_url,
        sector_report_path=args.sector_report,
        region_report_path=args.region_report,
        run_label=args.run_label,
        triggered_by=args.triggered_by,
        max_data_age_days=args.max_data_age_days,
    )
    print(f"Created update_run_id={update_run_id}")


if __name__ == "__main__":
    main()
