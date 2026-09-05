"use client";

import { useEffect, useRef } from "react";
import type { DecisionItem, StockSnapshot } from "../lib/portal-api";

type StockDetailDialogProps = {
  groupSignal: { direction?: string | null; name: string; strength?: number | null } | null;
  onClose: () => void;
  stock: StockSnapshot | null;
};

type MetricItem = [label: string, value: string];

function toNumber(value?: number | string | null) {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function formatNumber(value?: number | string | null, digits = 2) {
  const number = toNumber(value);
  return number === null ? "n/a" : number.toLocaleString("en-US", { maximumFractionDigits: digits });
}

function formatBig(value?: number | string | null) {
  const number = toNumber(value);
  return number === null ? "n/a" : new Intl.NumberFormat("en-US", { notation: "compact", maximumFractionDigits: 2 }).format(number);
}

function formatPercent(value?: number | string | null) {
  const number = toNumber(value);
  return number === null ? "n/a" : `${number >= 0 ? "+" : ""}${(number * 100).toFixed(2)}%`;
}

function formatYield(value?: number | string | null) {
  const number = toNumber(value);
  return number === null ? "n/a" : `${number.toFixed(2)}%`;
}

function formatPrice(stock: StockSnapshot, value?: number | string | null) {
  const number = toNumber(value);
  if (number === null) return "n/a";
  const currency = stock.currency || stock.fundamentals?.currency || "USD";
  try {
    return new Intl.NumberFormat("en-US", { style: "currency", currency, maximumFractionDigits: 2 }).format(number);
  } catch {
    return `${currency} ${number.toFixed(2)}`;
  }
}

function signalClass(value?: string | null) {
  return (value || "Hold").toLowerCase();
}

function MetricGrid({ items }: { items: MetricItem[] }) {
  return <dl className="stock-detail-metrics">{items.map(([label, value]) => <div key={label}><dt>{label}</dt><dd>{value}</dd></div>)}</dl>;
}

function DecisionDetail({ item, title }: { item?: DecisionItem; title: string }) {
  return <div><dt>{title}</dt><dd><strong>{item?.label || "n/a"}</strong><span>{item?.detail || "No additional detail is available."}</span></dd></div>;
}

export function StockDetailDialog({ groupSignal, onClose, stock }: StockDetailDialogProps) {
  const dialogRef = useRef<HTMLDialogElement>(null);

  useEffect(() => {
    const dialog = dialogRef.current;
    if (!dialog) return;
    if (stock && !dialog.open) dialog.showModal();
    if (!stock && dialog.open) dialog.close();
  }, [stock]);

  const fundamentals = stock?.fundamentals || {};
  const companyName = stock?.company_name || fundamentals.shortName || stock?.ticker || "Stock";
  const snapshot = stock?.decision_snapshot;

  return (
    <dialog
      aria-labelledby="stock-detail-title"
      className="stock-detail-dialog"
      onClick={(event) => event.target === event.currentTarget && event.currentTarget.close()}
      onClose={onClose}
      ref={dialogRef}
    >
      {stock ? <div className="stock-detail-body">
        <header className="stock-detail-header">
          <div>
            <p className="eyebrow">{stock.region || "Market"} / {stock.market || stock.exchange || "Listing"}</p>
            <h2 id="stock-detail-title"><span>{stock.ticker}</span> {companyName}</h2>
            <p>{fundamentals.longBusinessSummary || `${companyName} operates in ${stock.industry || stock.sector || "its reported business category"}.`}</p>
          </div>
          <div className="stock-detail-header-actions">
            <div className="stock-detail-signal-pair">
              <span><small>Standard</small><b className={`stock-detail-signal ${signalClass(stock.action)}`}>{stock.action || "Hold"}</b></span>
              <span><small>Personal</small><b className={`stock-detail-signal ${signalClass(stock.personalized_action)}`}>{stock.personalized_action || "n/a"}</b></span>
            </div>
            <button aria-label="Close stock information" className="stock-detail-close" onClick={() => dialogRef.current?.close()} title="Close" type="button">&times;</button>
          </div>
        </header>

        <section className="stock-detail-section">
          <h3>Company and listing</h3>
          <MetricGrid items={[
            ["Sector", stock.sector || "n/a"],
            ["Industry", stock.industry || fundamentals.industry || "n/a"],
            ["Country", stock.country || "n/a"],
            ["Region", stock.region || "n/a"],
            ["Market", stock.market || "n/a"],
            ["Exchange", stock.exchange || fundamentals.exchange || "n/a"],
            ["Currency", stock.currency || fundamentals.currency || "n/a"],
            ["Market cap", formatBig(stock.market_cap ?? fundamentals.marketCap)]
          ]} />
        </section>

        <section className="stock-detail-section">
          <h3>Price and market activity</h3>
          <MetricGrid items={[
            ["Current price", formatPrice(stock, stock.current_price)],
            ["Price date", stock.price_date || "n/a"],
            ["Previous close", formatPrice(stock, stock.previous_close)],
            ["Previous close date", stock.previous_close_date || "n/a"],
            ["Close change", formatPrice(stock, stock.close_change)],
            ["Close change %", formatPercent(stock.close_change_pct)],
            ["Weekly return", formatPercent(stock.weekly_return)],
            ["Latest volume", formatBig(stock.latest_volume)],
            ["Latest dollar volume", formatBig(stock.dollar_vol_latest)],
            ["Weekly dollar volume", formatBig(stock.dollar_vol_week)],
            ["Volume ratio", formatNumber(stock.vol_ratio)],
            ["Daily trading percentile", toNumber(stock.daily_trading_percentile) === null ? "n/a" : `${formatNumber(stock.daily_trading_percentile, 0)}%`],
            ["Activity rank", formatNumber(stock.rank, 0)],
            ["Volume date", stock.volume_date || "n/a"],
            ["52-week low", formatPrice(stock, fundamentals.fiftyTwoWeekLow)],
            ["52-week high", formatPrice(stock, fundamentals.fiftyTwoWeekHigh)]
          ]} />
        </section>

        <section className="stock-detail-section">
          <h3>Valuation and income</h3>
          <MetricGrid items={[
            ["Trailing P/E", formatNumber(stock.trailing_pe ?? fundamentals.trailingPE)],
            ["Forward P/E", formatNumber(stock.forward_pe ?? fundamentals.forwardPE)],
            ["Price / book", formatNumber(stock.price_to_book ?? fundamentals.priceToBook)],
            ["PEG ratio", formatNumber(stock.peg_ratio ?? fundamentals.pegRatio)],
            ["Dividend yield", formatYield(stock.dividend_yield ?? fundamentals.dividendYield)],
            ["5-year avg dividend yield", formatYield(fundamentals.fiveYearAvgDividendYield)],
            ["Payout ratio", formatPercent(fundamentals.payoutRatio)]
          ]} />
        </section>

        <section className="stock-detail-section">
          <h3>Growth, profitability, and risk</h3>
          <MetricGrid items={[
            ["Revenue growth", formatPercent(fundamentals.revenueGrowth)],
            ["Earnings growth", formatPercent(fundamentals.earningsGrowth)],
            ["Gross margin", formatPercent(fundamentals.grossMargins)],
            ["Operating margin", formatPercent(fundamentals.operatingMargins)],
            ["Profit margin", formatPercent(fundamentals.profitMargins)],
            ["Return on equity", formatPercent(fundamentals.returnOnEquity)],
            ["Return on assets", formatPercent(fundamentals.returnOnAssets)],
            ["Total debt", formatBig(fundamentals.totalDebt)],
            ["Debt / equity", formatNumber(fundamentals.debtToEquity)],
            ["Current ratio", formatNumber(fundamentals.currentRatio)],
            ["Quick ratio", formatNumber(fundamentals.quickRatio)],
            ["Beta", formatNumber(fundamentals.beta)]
          ]} />
        </section>

        <section className="stock-detail-section stock-signal-analysis">
          <h3>Market signal</h3>
          <MetricGrid items={[
            ["Standard recommendation", stock.action || "Hold"],
            ["Standard score", formatNumber(stock.score)],
            ["Standard confidence", toNumber(stock.confidence) === null ? "n/a" : `${formatNumber(stock.confidence, 0)}%`],
            ["Personal recommendation", stock.personalized_action || "n/a"],
            ["Personal score", formatNumber(stock.personalized_score)],
            ["Personal confidence", toNumber(stock.personalized_confidence) === null ? "n/a" : `${formatNumber(stock.personalized_confidence, 0)}%`],
            ["Close direction", stock.close_direction || "n/a"],
            ["Context group", groupSignal?.name || "n/a"],
            ["Context direction", groupSignal?.direction || "n/a"],
            ["Context strength", formatNumber(groupSignal?.strength, 0)]
          ]} />
          <div className="stock-detail-narrative"><strong>Decision summary</strong><p>{snapshot?.summary || "No decision summary is available for this update."}</p></div>
          <div className="stock-detail-narrative"><strong>Standard rationale</strong><p>{stock.rationale || "No standard-model rationale is available for this update."}</p></div>
          <div className="stock-detail-narrative"><strong>Personal rationale</strong><p>{stock.personalized_rationale || "No personalized rationale is available for this update."}</p></div>
          <div className="stock-detail-narrative"><strong>Daily summary</strong><p>{stock.daily_summary || "No daily summary is available for this update."}</p></div>
          <dl className="stock-decision-details">
            <DecisionDetail item={snapshot?.valuation} title="Valuation" />
            <DecisionDetail item={snapshot?.quality} title="Quality" />
            <DecisionDetail item={snapshot?.risk} title="Risk" />
            <DecisionDetail item={snapshot?.momentum} title="Momentum" />
            <DecisionDetail item={snapshot?.income} title="Income" />
          </dl>
        </section>

        <footer className="stock-detail-footer">Snapshot date: {stock.price_date || stock.volume_date || "n/a"}. Research information only, not personalized financial advice.</footer>
      </div> : null}
    </dialog>
  );
}
