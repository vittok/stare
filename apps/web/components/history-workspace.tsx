"use client";

import { Download, FileJson, Plus, X } from "lucide-react";
import { useEffect, useMemo, useState } from "react";
import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis
} from "recharts";

import { downloadCsv, downloadJson } from "../lib/download-data";
import type {
  GroupHistoryResponse,
  RegionSnapshot,
  SectorSnapshot,
  StockSnapshot,
  TickerHistoryResponse
} from "../lib/portal-api";

type HistoryMode = "sectors" | "regions" | "tickers";
type ChartRow = { observed_at: string; [key: string]: string | number | null };

const COLORS = ["#175cd3", "#087849", "#b42318", "#916200", "#7a3e9d", "#087f8c"];

function numberValue(value: number | string | null | undefined) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function shortDate(value: string) {
  const date = new Date(value);
  return Number.isNaN(date.valueOf()) ? value : date.toLocaleDateString("en-GB", { day: "2-digit", month: "short", timeZone: "UTC" });
}

function fullDate(value: string) {
  const date = new Date(value);
  return Number.isNaN(date.valueOf()) ? value : date.toLocaleString("en-GB", { dateStyle: "medium", timeStyle: "short", timeZone: "UTC" });
}

function compact(value: number) {
  return new Intl.NumberFormat("en-US", { notation: "compact", maximumFractionDigits: 1 }).format(value);
}

function chartRows<T extends { observed_at: string }>(
  series: { name: string; points: T[] }[],
  value: (point: T) => number | null,
  suffix = ""
) {
  const rows = new Map<string, ChartRow>();
  for (const item of series) {
    for (const point of item.points) {
      const row = rows.get(point.observed_at) || { observed_at: point.observed_at };
      row[`${item.name}${suffix}`] = value(point);
      rows.set(point.observed_at, row);
    }
  }
  return Array.from(rows.values()).sort((a, b) => a.observed_at.localeCompare(b.observed_at));
}

function TrendChart({
  data,
  keys,
  domain,
  formatter,
  previousClose = false
}: {
  data: ChartRow[];
  keys: string[];
  domain?: [number | "auto", number | "auto"];
  formatter?: (value: number) => string;
  previousClose?: boolean;
}) {
  return <div className="history-chart" role="img" aria-label={`Trend chart comparing ${keys.join(", ")}`}>
    <ResponsiveContainer height="100%" width="100%">
      <LineChart data={data} margin={{ top: 12, right: 18, bottom: 6, left: 4 }}>
        <CartesianGrid stroke="var(--line)" strokeDasharray="2 2" vertical={false} />
        <XAxis dataKey="observed_at" minTickGap={28} stroke="var(--muted)" tickFormatter={shortDate} tickLine={false} />
        <YAxis domain={domain} stroke="var(--muted)" tickFormatter={formatter} tickLine={false} width={58} />
        <Tooltip contentStyle={{ background: "var(--surface)", border: "1px solid var(--line-strong)", borderRadius: 0, color: "var(--ink)" }} labelFormatter={(label) => fullDate(String(label))} formatter={(value) => formatter ? formatter(Number(value)) : Number(value).toLocaleString("en-US", { maximumFractionDigits: 2 })} />
        <Legend />
        {keys.map((key, index) => <Line connectNulls dataKey={key} dot={false} key={key} name={key} stroke={COLORS[Math.floor(index / (previousClose ? 2 : 1)) % COLORS.length]} strokeDasharray={previousClose && key.endsWith("Previous") ? "5 4" : undefined} strokeWidth={previousClose && key.endsWith("Previous") ? 1.5 : 2.5} type="monotone" />)}
      </LineChart>
    </ResponsiveContainer>
  </div>;
}

function SelectionList({ names, selected, onChange, limit = 6 }: { names: string[]; selected: string[]; onChange: (next: string[]) => void; limit?: number }) {
  return <div className="history-selection">{names.map((name) => {
    const checked = selected.includes(name);
    return <label key={name}><input checked={checked} disabled={!checked && selected.length >= limit} onChange={() => onChange(checked ? selected.filter((item) => item !== name) : [...selected, name])} type="checkbox" /><span>{name}</span></label>;
  })}</div>;
}

function ExportButtons({ baseName, data, rows }: { baseName: string; data: unknown; rows: Record<string, unknown>[] }) {
  return <div className="export-buttons">
    <button className="button secondary icon-command" disabled={!rows.length} onClick={() => downloadCsv(`${baseName}.csv`, rows)} title="Download CSV" type="button"><Download aria-hidden="true" size={15} /><span>CSV</span></button>
    <button className="button secondary icon-command" disabled={!rows.length} onClick={() => downloadJson(`${baseName}.json`, data)} title="Download JSON" type="button"><FileJson aria-hidden="true" size={15} /><span>JSON</span></button>
  </div>;
}

export function HistoryWorkspace({ regions, sectors, stocks }: { regions: RegionSnapshot[]; sectors: SectorSnapshot[]; stocks: StockSnapshot[] }) {
  const sectorNames = useMemo(() => sectors.map((item) => item.sector).sort(), [sectors]);
  const regionNames = useMemo(() => Array.from(new Set(["NA", ...regions.map((item) => item.region)])).sort((a, b) => ["NA", "LAC", "EMEA", "APAC"].indexOf(a) - ["NA", "LAC", "EMEA", "APAC"].indexOf(b)), [regions]);
  const tickerOptions = useMemo(() => {
    const unique = new Map<string, StockSnapshot>();
    [...stocks].sort((a, b) => (numberValue(b.dollar_vol_latest) || 0) - (numberValue(a.dollar_vol_latest) || 0)).forEach((stock) => { if (!unique.has(stock.ticker)) unique.set(stock.ticker, stock); });
    return Array.from(unique.values());
  }, [stocks]);
  const [mode, setMode] = useState<HistoryMode>("sectors");
  const [days, setDays] = useState(30);
  const [selectedSectors, setSelectedSectors] = useState(() => [...sectors].sort((a, b) => (b.strength || 0) - (a.strength || 0)).slice(0, 4).map((item) => item.sector));
  const [selectedRegions, setSelectedRegions] = useState(() => regionNames);
  const [selectedTickers, setSelectedTickers] = useState(() => tickerOptions.slice(0, 3).map((item) => item.ticker));
  const [tickerChoice, setTickerChoice] = useState("");
  const [groupData, setGroupData] = useState<GroupHistoryResponse | null>(null);
  const [tickerData, setTickerData] = useState<TickerHistoryResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const activeGroups = mode === "sectors" ? selectedSectors : selectedRegions;
  useEffect(() => {
    if (mode === "tickers") return;
    if (!activeGroups.length) { setGroupData(null); setLoading(false); return; }
    const controller = new AbortController();
    setLoading(true); setError("");
    const query = new URLSearchParams({ kind: mode === "sectors" ? "sector" : "region", names: activeGroups.join(","), days: String(days) });
    void fetch(`/api/history/groups?${query}`, { cache: "no-store", signal: controller.signal })
      .then(async (response) => { const payload = await response.json(); if (!response.ok) throw new Error(payload.detail || payload.error || "History is unavailable."); return payload as GroupHistoryResponse; })
      .then(setGroupData)
      .catch((reason) => { if (reason.name !== "AbortError") setError(reason.message); })
      .finally(() => { if (!controller.signal.aborted) setLoading(false); });
    return () => controller.abort();
  }, [activeGroups.join("|"), days, mode]);

  useEffect(() => {
    if (mode !== "tickers") return;
    if (!selectedTickers.length) { setTickerData(null); setLoading(false); return; }
    const controller = new AbortController();
    setLoading(true); setError("");
    const query = new URLSearchParams({ tickers: selectedTickers.join(","), days: String(days) });
    void fetch(`/api/history/tickers?${query}`, { cache: "no-store", signal: controller.signal })
      .then(async (response) => { const payload = await response.json(); if (!response.ok) throw new Error(payload.detail || payload.error || "Ticker history is unavailable."); return payload as TickerHistoryResponse; })
      .then(setTickerData)
      .catch((reason) => { if (reason.name !== "AbortError") setError(reason.message); })
      .finally(() => { if (!controller.signal.aborted) setLoading(false); });
    return () => controller.abort();
  }, [days, mode, selectedTickers.join("|")]);

  const groupRows = groupData ? chartRows(groupData.series, (point) => numberValue(point.strength)) : [];
  const priceRows = tickerData ? (() => {
    const current = chartRows(tickerData.series, (point) => numberValue(point.current_price));
    const previous = chartRows(tickerData.series, (point) => numberValue(point.previous_close), " Previous");
    const merged = new Map(current.map((row) => [row.observed_at, row]));
    previous.forEach((row) => merged.set(row.observed_at, { ...(merged.get(row.observed_at) || { observed_at: row.observed_at }), ...row }));
    return Array.from(merged.values()).sort((a, b) => a.observed_at.localeCompare(b.observed_at));
  })() : [];
  const weeklyRows = tickerData ? chartRows(tickerData.series, (point) => { const value = numberValue(point.weekly_return); return value === null ? null : value * 100; }) : [];
  const percentileRows = tickerData ? chartRows(tickerData.series, (point) => numberValue(point.daily_trading_percentile)) : [];
  const volumeRows = tickerData ? chartRows(tickerData.series, (point) => numberValue(point.dollar_vol_latest)) : [];
  const tickerKeys = tickerData?.series.map((item) => item.name) || [];
  const recommendationRows = tickerData?.series.flatMap((item) => item.points.map((point) => ({ ticker: item.name, ...point }))).sort((a, b) => b.observed_at.localeCompare(a.observed_at)) || [];
  const groupExportRows = groupData?.series.flatMap((item) => item.points.map((point) => ({ group: item.name, ...point }))) || [];
  const tickerExportRows = recommendationRows;

  function addTicker() {
    if (!tickerChoice || selectedTickers.includes(tickerChoice) || selectedTickers.length >= 5) return;
    setSelectedTickers((current) => [...current, tickerChoice]);
    setTickerChoice("");
  }

  return <section className="history-workspace" aria-label="Historical analysis">
    <div className="history-toolbar">
      <div className="segmented history-modes" role="tablist" aria-label="Historical view">
        <button aria-selected={mode === "sectors"} className={mode === "sectors" ? "active" : ""} onClick={() => setMode("sectors")} role="tab" type="button">Sectors</button>
        <button aria-selected={mode === "regions"} className={mode === "regions" ? "active" : ""} onClick={() => setMode("regions")} role="tab" type="button">Regions</button>
        <button aria-selected={mode === "tickers"} className={mode === "tickers" ? "active" : ""} onClick={() => setMode("tickers")} role="tab" type="button">Ticker Compare</button>
      </div>
      <div className="history-period"><span>Period</span><div className="segmented">{[7, 14, 30].map((value) => <button className={days === value ? "active" : ""} key={value} onClick={() => setDays(value)} type="button">{value}D</button>)}</div></div>
      <ExportButtons baseName={`stare-${mode}-history`} data={mode === "tickers" ? tickerData : groupData} rows={loading ? [] : mode === "tickers" ? tickerExportRows : groupExportRows} />
    </div>

    {mode === "sectors" ? <SelectionList names={sectorNames} onChange={setSelectedSectors} selected={selectedSectors} /> : null}
    {mode === "regions" ? <SelectionList limit={4} names={regionNames} onChange={setSelectedRegions} selected={selectedRegions} /> : null}
    {mode === "tickers" ? <div className="ticker-compare-controls"><div className="ticker-picker"><select aria-label="Ticker to compare" onChange={(event) => setTickerChoice(event.target.value)} value={tickerChoice}><option value="">Select ticker</option>{tickerOptions.filter((stock) => !selectedTickers.includes(stock.ticker)).map((stock) => <option key={stock.ticker} value={stock.ticker}>{stock.ticker} - {stock.company_name || stock.ticker}</option>)}</select><button aria-label="Add ticker" className="button secondary" disabled={!tickerChoice || selectedTickers.length >= 5} onClick={addTicker} title="Add ticker" type="button"><Plus aria-hidden="true" size={16} /></button></div><div className="ticker-chips">{selectedTickers.map((ticker) => <span key={ticker}>{ticker}<button aria-label={`Remove ${ticker}`} onClick={() => setSelectedTickers((current) => current.filter((item) => item !== ticker))} title={`Remove ${ticker}`} type="button"><X aria-hidden="true" size={13} /></button></span>)}</div></div> : null}

    {loading ? <div className="history-state" aria-live="polite">Loading history...</div> : null}
    {error ? <div className="history-state error" role="alert">{error}</div> : null}
    {!loading && !error && mode !== "tickers" && !activeGroups.length ? <div className="history-state">Select at least one group.</div> : null}
    {!loading && !error && mode === "tickers" && !selectedTickers.length ? <div className="history-state">Select at least one ticker.</div> : null}
    {!loading && !error && mode !== "tickers" && activeGroups.length > 0 && !groupRows.length ? <div className="history-state">No group history is available for this period.</div> : null}
    {!loading && !error && mode === "tickers" && selectedTickers.length > 0 && !tickerKeys.length ? <div className="history-state">No ticker history is available for this period.</div> : null}

    {!loading && !error && mode !== "tickers" && groupRows.length ? <div className="history-chart-grid single"><section className="history-chart-block"><div className="history-chart-heading"><div><p className="eyebrow">{days}-day database history</p><h2>{mode === "sectors" ? "Sector" : "Region"} strength</h2></div><span>{groupRows.length} observations</span></div><TrendChart data={groupRows} domain={[0, 100]} formatter={(value) => value.toFixed(0)} keys={groupData?.series.map((item) => item.name) || []} /></section></div> : null}

    {!loading && !error && mode === "tickers" && tickerKeys.length ? <>
      <div className="history-chart-grid single"><section className="history-chart-block"><div className="history-chart-heading"><div><p className="eyebrow">Price history</p><h2>Close and previous close</h2></div><span>{priceRows.length} observations</span></div><TrendChart data={priceRows} keys={tickerKeys.flatMap((ticker) => [ticker, `${ticker} Previous`])} previousClose /></section></div>
      <div className="history-chart-grid"><section className="history-chart-block"><div className="history-chart-heading"><div><p className="eyebrow">Momentum</p><h2>Weekly return</h2></div></div><TrendChart data={weeklyRows} formatter={(value) => `${value.toFixed(1)}%`} keys={tickerKeys} /></section><section className="history-chart-block"><div className="history-chart-heading"><div><p className="eyebrow">Trading activity</p><h2>Daily percentile</h2></div></div><TrendChart data={percentileRows} domain={[0, 100]} formatter={(value) => `${value.toFixed(0)}%`} keys={tickerKeys} /></section></div>
      <div className="history-chart-grid single"><section className="history-chart-block"><div className="history-chart-heading"><div><p className="eyebrow">Liquidity</p><h2>Daily dollar volume</h2></div></div><TrendChart data={volumeRows} formatter={compact} keys={tickerKeys} /></section></div>
      <section className="history-recommendations"><div className="history-chart-heading"><div><p className="eyebrow">Model history</p><h2>Recommendation timeline</h2></div><span>{recommendationRows.length} observations</span></div><div className="history-table-wrap"><table className="history-table"><thead><tr><th>Observed</th><th>Ticker</th><th>Signal</th><th className="num">Score</th><th className="num">Confidence</th><th>Rationale</th></tr></thead><tbody>{recommendationRows.map((row) => <tr key={`${row.ticker}-${row.observed_at}`}><td>{fullDate(row.observed_at)}</td><td><strong>{row.ticker}</strong></td><td><span className={`signal ${(row.action || "Hold").toLowerCase()}`}>{row.action || "Hold"}</span></td><td className="num">{numberValue(row.score)?.toFixed(2) ?? "n/a"}</td><td className="num">{row.confidence ?? "n/a"}%</td><td>{row.rationale || "n/a"}</td></tr>)}</tbody></table></div></section>
    </> : null}
  </section>;
}
