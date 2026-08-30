"use client";

import { useMemo, useState, useTransition } from "react";
import { savePreferences } from "../app/actions";
import type {
  LatestReport,
  RegionSnapshot,
  StockSnapshot,
  UserPreferences
} from "../lib/portal-api";

type PortalDashboardProps = {
  report: LatestReport | null;
  preferences: UserPreferences;
  signedIn: boolean;
};

const currencyFormatter = new Intl.NumberFormat("en-US", {
  maximumFractionDigits: 2,
  minimumFractionDigits: 2
});

const compactFormatter = new Intl.NumberFormat("en-US", {
  notation: "compact",
  maximumFractionDigits: 1
});

function toNumber(value?: number | string | null) {
  if (value === null || value === undefined || value === "") {
    return null;
  }

  const numericValue = Number(value);
  return Number.isFinite(numericValue) ? numericValue : null;
}

function formatPercent(value?: number | string | null) {
  const numericValue = toNumber(value);
  if (numericValue === null) {
    return "n/a";
  }

  const percentValue = numericValue * 100;
  return `${percentValue >= 0 ? "+" : ""}${percentValue.toFixed(2)}%`;
}

function formatPrice(value?: number | string | null) {
  const numericValue = toNumber(value);
  if (numericValue === null) {
    return "n/a";
  }

  return `$${currencyFormatter.format(numericValue)}`;
}

function directionClass(direction?: string | null) {
  if (direction === "Bullish") {
    return "positive";
  }

  if (direction === "Bearish") {
    return "negative";
  }

  return "neutral";
}

function actionClass(action?: string | null) {
  if (action === "Buy") {
    return "positive";
  }

  if (action === "Sell") {
    return "negative";
  }

  return "neutral";
}

function regionLabel(region: string | null | undefined) {
  return region || "Unassigned";
}

function getRegions(report: LatestReport | null) {
  const reportRegions = report?.regions.map((region) => region.region) ?? [];
  const stockRegions = report?.top_stocks.map((stock) => regionLabel(stock.region)) ?? [];
  return Array.from(new Set(["All", ...reportRegions, ...stockRegions]));
}

function findRegion(regions: RegionSnapshot[], selectedRegion: string) {
  return regions.find((region) => region.region === selectedRegion);
}

export function PortalDashboard({
  report,
  preferences,
  signedIn
}: PortalDashboardProps) {
  const regions = useMemo(() => getRegions(report), [report]);
  const initialRegion =
    preferences.default_region && regions.includes(preferences.default_region)
      ? preferences.default_region
      : "All";
  const [selectedRegion, setSelectedRegion] = useState(initialRegion);
  const [watchlist, setWatchlist] = useState(preferences.watchlist ?? []);
  const [status, setStatus] = useState("");
  const [isPending, startTransition] = useTransition();

  const stocks = useMemo(() => {
    const rows = report?.top_stocks ?? [];
    if (selectedRegion === "All") {
      return rows;
    }

    return rows.filter((stock) => regionLabel(stock.region) === selectedRegion);
  }, [report, selectedRegion]);

  const selectedRegionSummary =
    selectedRegion === "All"
      ? null
      : findRegion(report?.regions ?? [], selectedRegion);

  const topWatchlist = stocks.filter((stock) => watchlist.includes(stock.ticker));

  function persist(nextPreferences: UserPreferences) {
    setStatus("");
    startTransition(() => {
      void (async () => {
        const result = await savePreferences(nextPreferences);
        if (result.ok) {
          setStatus("Preferences saved.");
        } else {
          setStatus(result.error);
        }
      })();
    });
  }

  function selectRegion(region: string) {
    setSelectedRegion(region);
    if (signedIn) {
      persist({
        ...preferences,
        default_region: region === "All" ? null : region,
        watchlist
      });
    }
  }

  function toggleWatchlist(ticker: string) {
    const nextWatchlist = watchlist.includes(ticker)
      ? watchlist.filter((item) => item !== ticker)
      : [...watchlist, ticker];

    setWatchlist(nextWatchlist);
    if (signedIn) {
      persist({
        ...preferences,
        default_region: selectedRegion === "All" ? null : selectedRegion,
        watchlist: nextWatchlist
      });
    } else {
      setStatus("Sign in to save a watchlist.");
    }
  }

  if (!report?.update) {
    return (
      <section className="panel empty-state">
        <h2>Report data is not available yet</h2>
        <p>
          The portal is connected, but the backend has not returned a completed
          market update. Once the importer writes a successful update, this
          dashboard will fill in automatically.
        </p>
      </section>
    );
  }

  return (
    <section className="dashboard" aria-label="STARE dashboard">
      <div className="dashboard-header">
        <div>
          <p className="eyebrow">Latest market update</p>
          <h1>S.T.A.R.E Portal</h1>
          <p className="lede">
            Personalized market strength, active stocks, and recommendation
            signals backed by the Supabase report history.
          </p>
        </div>
        <div className="update-card">
          <span>Market data</span>
          <strong>{report.update.latest_price_date ?? "n/a"}</strong>
          <small>Status: {report.update.status ?? "unknown"}</small>
        </div>
      </div>

      <div className="region-rail" aria-label="Region filter">
        {regions.map((region) => (
          <button
            className={region === selectedRegion ? "chip active" : "chip"}
            key={region}
            onClick={() => selectRegion(region)}
            type="button"
          >
            {region}
          </button>
        ))}
      </div>

      <div className="summary-grid">
        {(selectedRegion === "All" ? report.regions : report.regions.filter((region) => region.region === selectedRegion)).map(
          (region) => (
            <article className="panel region-card" key={region.region}>
              <div className="card-row">
                <h2>{region.region}</h2>
                <span className={`pill ${directionClass(region.direction)}`}>
                  {region.direction ?? "Neutral"}
                </span>
              </div>
              <div className="strength-meter">
                <span style={{ width: `${region.strength ?? 0}%` }} />
              </div>
              <div className="card-row muted-row">
                <span>Strength</span>
                <strong>{region.strength ?? 0}/100</strong>
              </div>
            </article>
          )
        )}
      </div>

      <div className="dashboard-grid">
        <section className="panel span-2">
          <div className="section-heading">
            <div>
              <p className="eyebrow">Top active stocks</p>
              <h2>
                {selectedRegion === "All"
                  ? "All regions"
                  : `${selectedRegion} leaders`}
              </h2>
            </div>
            {selectedRegionSummary ? (
              <span className={`pill ${directionClass(selectedRegionSummary.direction)}`}>
                {selectedRegionSummary.direction}
              </span>
            ) : null}
          </div>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Watch</th>
                  <th>Ticker</th>
                  <th>Name</th>
                  <th>Region</th>
                  <th>Price</th>
                  <th>Daily</th>
                  <th>Weekly</th>
                  <th>Signal</th>
                  <th>Confidence</th>
                </tr>
              </thead>
              <tbody>
                {stocks.slice(0, 24).map((stock) => (
                  <tr key={`${stock.region}-${stock.ticker}`}>
                    <td>
                      <button
                        aria-label={`Toggle ${stock.ticker} watchlist`}
                        className={
                          watchlist.includes(stock.ticker)
                            ? "watch-button active"
                            : "watch-button"
                        }
                        onClick={() => toggleWatchlist(stock.ticker)}
                        type="button"
                      >
                        +
                      </button>
                    </td>
                    <td>
                      <strong>{stock.ticker}</strong>
                    </td>
                    <td>
                      <span className="company-name">{stock.company_name ?? "n/a"}</span>
                      <small>{stock.rationale ?? stock.daily_summary ?? ""}</small>
                    </td>
                    <td>{regionLabel(stock.region)}</td>
                    <td>{formatPrice(stock.current_price)}</td>
                    <td>{formatPercent(stock.close_change_pct)}</td>
                    <td>{formatPercent(stock.weekly_return)}</td>
                    <td>
                      <span className={`pill ${actionClass(stock.action)}`}>
                        {stock.action ?? "Hold"}
                      </span>
                    </td>
                    <td>
                      {stock.confidence !== null && stock.confidence !== undefined
                        ? `${stock.confidence}%`
                        : "n/a"}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>

        <aside className="panel">
          <div className="section-heading compact">
            <div>
              <p className="eyebrow">Personal view</p>
              <h2>Watchlist</h2>
            </div>
          </div>
          {topWatchlist.length ? (
            <div className="watchlist">
              {topWatchlist.map((stock) => (
                <div className="watchlist-row" key={stock.ticker}>
                  <strong>{stock.ticker}</strong>
                  <span>{formatPrice(stock.current_price)}</span>
                  <span className={`pill ${actionClass(stock.action)}`}>
                    {stock.action ?? "Hold"}
                  </span>
                </div>
              ))}
            </div>
          ) : (
            <p className="muted">
              Mark stocks from the table to build a saved watchlist.
            </p>
          )}
          <div className="save-status">
            {isPending ? "Saving..." : status || (signedIn ? "Signed-in preferences are active." : "Sign in to save preferences.")}
          </div>
        </aside>
      </div>

      <section className="panel">
        <div className="section-heading">
          <div>
            <p className="eyebrow">NA sectors</p>
            <h2>Sector strength</h2>
          </div>
        </div>
        <div className="sector-grid">
          {(report.sectors ?? []).map((sector) => (
            <div className="sector-row" key={sector.sector}>
              <span>{sector.sector}</span>
              <strong>{sector.strength ?? 0}</strong>
              <span className={`pill ${directionClass(sector.direction)}`}>
                {sector.direction ?? "Neutral"}
              </span>
            </div>
          ))}
        </div>
      </section>

      <section className="panel">
        <div className="section-heading compact">
          <div>
            <p className="eyebrow">Snapshot</p>
            <h2>Decision mix</h2>
          </div>
        </div>
        <div className="decision-grid">
          {["Buy", "Hold", "Sell"].map((action) => {
            const count = (report.top_stocks ?? []).filter(
              (stock: StockSnapshot) => stock.action === action
            ).length;
            return (
              <div className="metric" key={action}>
                <span className="metric-label">{action}</span>
                <span className="metric-value">{compactFormatter.format(count)}</span>
              </div>
            );
          })}
        </div>
      </section>
    </section>
  );
}
