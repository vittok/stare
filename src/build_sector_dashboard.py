from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from sqlalchemy import create_engine, text


DASH_SCHEMA = """
CREATE TABLE IF NOT EXISTS sector_dashboard_top10 (
  sector TEXT NOT NULL,
  week_ending TEXT NOT NULL,
  rank INTEGER NOT NULL,
  ticker TEXT NOT NULL,
  direction TEXT,
  strength INTEGER,
  raw_score REAL,
  dollar_vol_week REAL,
  weekly_return REAL,
  vol_ratio REAL,
  marketCap REAL,
  trailingPE REAL,
  forwardPE REAL,
  priceToBook REAL,
  profitMargins REAL,
  operatingMargins REAL,
  returnOnEquity REAL,
  dividendYield REAL,
  beta REAL,
  shortName TEXT,
  industry TEXT,
  currency TEXT,
  exchange TEXT,
  PRIMARY KEY (sector, week_ending, rank)
);
"""


@dataclass
class DashboardConfig:
    db_path: Path = Path("data/stocks.db")
    out_json: Path = Path("reports/sector_dashboard.json")
    out_csv: Path = Path("reports/sector_dashboard_top10.csv")
    write_sql_table: bool = True


def init_schema(engine):
    with engine.begin() as conn:
        for stmt in DASH_SCHEMA.strip().split(";"):
            s = stmt.strip()
            if s:
                conn.execute(text(s))


def _load_latest_week_ending(engine) -> str:
    # pick the most recent week_ending we have in sentiment
    with engine.begin() as conn:
        row = conn.execute(text("SELECT MAX(week_ending) FROM sector_sentiment")).fetchone()
    if not row or not row[0]:
        raise RuntimeError("No sector_sentiment data found. Run compute_sector_sentiment.py first.")
    return str(row[0])


def load_sector_sentiment(engine, week_ending: str) -> pd.DataFrame:
    return pd.read_sql(
        text("""
            SELECT sector, week_ending, raw_score, direction, strength, diagnostics_json
            FROM sector_sentiment
            WHERE week_ending = :week_ending
        """),
        engine,
        params={"week_ending": week_ending},
    )


def load_sector_top_active(engine, week_ending: str) -> pd.DataFrame:
    return pd.read_sql(
        text("""
            SELECT sector, week_ending, rank, ticker, dollar_vol_week, weekly_return, vol_ratio
            FROM sector_top_active
            WHERE week_ending = :week_ending
            ORDER BY sector, rank
        """),
        engine,
        params={"week_ending": week_ending},
    )


def load_fundamentals_latest(engine) -> pd.DataFrame:
    df = pd.read_sql(
        text("""
            SELECT ticker, asof_utc, normalized_json
            FROM fundamentals_latest
        """),
        engine,
    )
    if df.empty:
        raise RuntimeError("No fundamentals_latest found. Run fetch_fundamentals.py first.")
    return df


def expand_fundamentals_json(fund_df: pd.DataFrame) -> pd.DataFrame:
    """
    Expand normalized_json into columns.
    """
    def loads_safe(s: Any) -> Dict[str, Any]:
        try:
            return json.loads(s) if isinstance(s, str) and s else {}
        except Exception:
            return {}

    expanded = fund_df["normalized_json"].apply(loads_safe).apply(pd.Series)
    out = pd.concat([fund_df.drop(columns=["normalized_json"]), expanded], axis=1)
    out.rename(columns={"symbol": "symbol_from_info"}, inplace=True)
    return out


def build_flat_dashboard(
    sentiment: pd.DataFrame,
    top_active: pd.DataFrame,
    fundamentals_expanded: pd.DataFrame,
) -> pd.DataFrame:
    # join sentiment onto each top-active row by sector
    df = top_active.merge(
        sentiment[["sector", "week_ending", "raw_score", "direction", "strength"]],
        on=["sector", "week_ending"],
        how="left",
    )

    # join fundamentals by ticker
    df = df.merge(
        fundamentals_expanded,
        on="ticker",
        how="left",
        suffixes=("", "_fund"),
    )

    # Select a useful set of columns
    cols = [
        "sector", "week_ending", "rank", "ticker",
        "direction", "strength", "raw_score",
        "dollar_vol_week", "weekly_return", "vol_ratio",
        "marketCap", "trailingPE", "forwardPE", "priceToBook",
        "profitMargins", "operatingMargins", "returnOnEquity",
        "dividendYield", "beta",
        "shortName", "industry", "currency", "exchange",
        "asof_utc",
    ]
    for c in cols:
        if c not in df.columns:
            df[c] = None
    df = df[cols].copy()

    # Ensure numeric types where possible
    num_cols = [
        "raw_score", "dollar_vol_week", "weekly_return", "vol_ratio",
        "marketCap", "trailingPE", "forwardPE", "priceToBook",
        "profitMargins", "operatingMargins", "returnOnEquity",
        "dividendYield", "beta",
    ]
    for c in num_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    return df


def build_nested_json(sentiment: pd.DataFrame, flat_top10: pd.DataFrame) -> Dict[str, Any]:
    sectors = []
    for _, srow in sentiment.sort_values(["strength", "sector"], ascending=[False, True]).iterrows():
        sector = srow["sector"]
        sector_block = {
            "sector": sector,
            "week_ending": srow["week_ending"],
            "direction": srow["direction"],
            "strength": int(srow["strength"]) if pd.notna(srow["strength"]) else None,
            "raw_score": float(srow["raw_score"]) if pd.notna(srow["raw_score"]) else None,
            "diagnostics": json.loads(srow["diagnostics_json"]) if isinstance(srow.get("diagnostics_json"), str) else None,
            "top10_active": [],
        }

        sub = flat_top10[flat_top10["sector"] == sector].sort_values("rank")
        for _, r in sub.iterrows():
            sector_block["top10_active"].append({
                "rank": int(r["rank"]),
                "ticker": r["ticker"],
                "weekly_return": None if pd.isna(r["weekly_return"]) else float(r["weekly_return"]),
                "dollar_vol_week": None if pd.isna(r["dollar_vol_week"]) else float(r["dollar_vol_week"]),
                "vol_ratio": None if pd.isna(r["vol_ratio"]) else float(r["vol_ratio"]),
                "fundamentals": {
                    "shortName": r.get("shortName"),
                    "industry": r.get("industry"),
                    "exchange": r.get("exchange"),
                    "currency": r.get("currency"),
                    "marketCap": None if pd.isna(r.get("marketCap")) else float(r.get("marketCap")),
                    "trailingPE": None if pd.isna(r.get("trailingPE")) else float(r.get("trailingPE")),
                    "forwardPE": None if pd.isna(r.get("forwardPE")) else float(r.get("forwardPE")),
                    "priceToBook": None if pd.isna(r.get("priceToBook")) else float(r.get("priceToBook")),
                    "profitMargins": None if pd.isna(r.get("profitMargins")) else float(r.get("profitMargins")),
                    "operatingMargins": None if pd.isna(r.get("operatingMargins")) else float(r.get("operatingMargins")),
                    "returnOnEquity": None if pd.isna(r.get("returnOnEquity")) else float(r.get("returnOnEquity")),
                    "dividendYield": None if pd.isna(r.get("dividendYield")) else float(r.get("dividendYield")),
                    "beta": None if pd.isna(r.get("beta")) else float(r.get("beta")),
                }
            })

        sectors.append(sector_block)

    return {
        "generated_from": "stare pipeline",
        "sectors": sectors,
    }


def write_outputs(cfg: DashboardConfig, nested: Dict[str, Any], flat: pd.DataFrame) -> None:
    cfg.out_json.parent.mkdir(parents=True, exist_ok=True)
    cfg.out_csv.parent.mkdir(parents=True, exist_ok=True)

    cfg.out_json.write_text(json.dumps(nested, indent=2, ensure_ascii=False), encoding="utf-8")
    flat.to_csv(cfg.out_csv, index=False)


def upsert_sql_dashboard(engine, flat: pd.DataFrame) -> None:
    if flat.empty:
        return
    records = flat.to_dict(orient="records")
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM sector_dashboard_top10"))
        conn.execute(
            text("""
            INSERT INTO sector_dashboard_top10 (
              sector, week_ending, rank, ticker,
              direction, strength, raw_score,
              dollar_vol_week, weekly_return, vol_ratio,
              marketCap, trailingPE, forwardPE, priceToBook,
              profitMargins, operatingMargins, returnOnEquity,
              dividendYield, beta,
              shortName, industry, currency, exchange
            ) VALUES (
              :sector, :week_ending, :rank, :ticker,
              :direction, :strength, :raw_score,
              :dollar_vol_week, :weekly_return, :vol_ratio,
              :marketCap, :trailingPE, :forwardPE, :priceToBook,
              :profitMargins, :operatingMargins, :returnOnEquity,
              :dividendYield, :beta,
              :shortName, :industry, :currency, :exchange
            )
            """),
            records,
        )


def main():
    cfg = DashboardConfig()
    engine = create_engine(f"sqlite:///{cfg.db_path.as_posix()}", future=True)
    init_schema(engine)

    week_ending = _load_latest_week_ending(engine)
    print("Building dashboard for week ending:", week_ending)

    sentiment = load_sector_sentiment(engine, week_ending)
    top_active = load_sector_top_active(engine, week_ending)

    fundamentals = load_fundamentals_latest(engine)
    fundamentals_expanded = expand_fundamentals_json(fundamentals)

    flat = build_flat_dashboard(sentiment, top_active, fundamentals_expanded)
    nested = build_nested_json(sentiment, flat)

    write_outputs(cfg, nested, flat)

    if cfg.write_sql_table:
        upsert_sql_dashboard(engine, flat)

    print("Wrote:", cfg.out_json)
    print("Wrote:", cfg.out_csv)
    print("Rows:", len(flat))


if __name__ == "__main__":
    main()
