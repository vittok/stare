"use client";

import { useEffect, useMemo, useState, useTransition } from "react";
import type { FocusEvent, MouseEvent } from "react";
import {
  createWatchlist,
  deleteWatchlist,
  getMarketRefreshProgress,
  resetScoringWeights,
  savePreferences,
  saveScoringWeights,
  saveWatchlist,
  startMarketRefresh
} from "../app/actions";
import { AuthButton } from "./auth-button";
import { StockDetailDialog } from "./stock-detail-dialog";
import {
  defaultScoringWeights,
  type DecisionSnapshot,
  type LatestReport,
  type RegionSnapshot,
  type ScoringWeights,
  type SectorSnapshot,
  type StockSnapshot,
  type UserPreferences,
  type UserWatchlist
} from "../lib/portal-api";

type UserIdentity = { displayName: string; email: string };
type Props = {
  report: LatestReport | null;
  preferences: UserPreferences;
  scoringWeights: ScoringWeights;
  signedIn: boolean;
  user: UserIdentity | null;
  watchlists: UserWatchlist[];
};
type RegionMode = "All" | "NA" | "Sectors" | "LAC" | "EMEA" | "APAC";
type Direction = "All" | "Bullish" | "Bearish" | "Neutral";
type Group = RegionSnapshot | SectorSnapshot;
type DisplayStock = StockSnapshot & { daily_percentile: number | null };
type Popover = { title: string; content: string; pinned: boolean; x: number; y: number } | null;
type PopoverHandler = (title: string, content: string, x: number, y: number) => void;

const REGION_ORDER: RegionMode[] = ["All", "NA", "Sectors", "LAC", "EMEA", "APAC"];
const REGION_LABELS: Record<RegionMode, string> = { All: "All Regions", NA: "NA", Sectors: "NA/Sectors", LAC: "LAC", EMEA: "EMEA", APAC: "APAC" };
const COLUMNS: Record<string, string> = {
  watch: "Watch", group: "Sector / Region", rank: "#", ticker: "Ticker", name: "Name",
  current_price: "Price", previous_close: "Prev Close", action: "Signal", decision: "Decision Snapshot",
  weekly_return: "Weekly", daily_percentile: "Daily", dollar_vol_latest: "Day $ Vol", market_cap: "Mkt Cap",
  trailing_pe: "P/E", price_to_book: "P/B", peg_ratio: "PEG", dividend_yield: "Div"
};
const DEFAULT_COLUMNS = Object.keys(COLUMNS);
const NUMERIC_COLUMNS = new Set(["rank", "current_price", "previous_close", "weekly_return", "daily_percentile", "dollar_vol_latest", "market_cap", "trailing_pe", "price_to_book", "peg_ratio", "dividend_yield"]);
const HELP = {
  fundamentals: "Fundamentals are sourced from Yahoo Finance and stored with each ticker. S.T.A.R.E checks P/E for earnings valuation, P/B for market value versus book value, PEG for valuation relative to growth, and dividend yield for income support. Margin, growth, balance-sheet, beta, market-cap, industry, exchange, and currency fields provide broader company context.",
  strength: "Strength summarizes recent behavior across the tracked names in a sector or region. Breadth measures the share of positive returns, median return captures the typical move, and volume ratio compares current trading activity with its recent baseline. The blended raw score determines Bullish, Bearish, or Neutral direction and a 0-100 magnitude.",
  recommendation: "The recommendation starts with the sector or region raw score and adjusts it using weekly momentum and fundamentals. Lower P/E, lower P/B, PEG below 1, and dividend support can improve the score. Expensive valuation, weak momentum, or bearish group sentiment can reduce it. The final deterministic score maps to Buy, Hold, or Sell; confidence reflects score magnitude.",
  topPicks: "Top active stocks are ranked by latest available trading-day dollar volume, not cumulative weekly volume. The period is the last trading day captured by the update. Weekly return and volume ratio remain supporting context but do not determine pick order."
};
const SCORING_FACTORS: { key: keyof ScoringWeights; label: string }[] = [
  { key: "group_sentiment_weight", label: "Group sentiment" },
  { key: "pe_weight", label: "P/E" },
  { key: "pb_weight", label: "P/B" },
  { key: "peg_weight", label: "PEG" },
  { key: "dividend_weight", label: "Dividend yield" },
  { key: "momentum_weight", label: "Momentum" }
];

function toNumber(value?: number | string | null) {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}
function formatPercent(value?: number | string | null) { const n = toNumber(value); return n === null ? "n/a" : `${n >= 0 ? "+" : ""}${(n * 100).toFixed(2)}%`; }
function formatRatio(value?: number | string | null, digits = 2) { const n = toNumber(value); return n === null ? "n/a" : n.toFixed(digits); }
function formatYield(value?: number | string | null) { const n = toNumber(value); return n === null ? "n/a" : `${n.toFixed(2)}%`; }
function formatBig(value?: number | string | null) { const n = toNumber(value); return n === null ? "n/a" : new Intl.NumberFormat("en-US", { notation: "compact", maximumFractionDigits: 2 }).format(n); }
function stockName(stock: StockSnapshot) { return stock.company_name || stock.fundamentals?.shortName || stock.ticker; }
function stockValue(stock: StockSnapshot, key: keyof NonNullable<StockSnapshot["fundamentals"]>, fallback?: number | string | null) { return stock.fundamentals?.[key] ?? fallback ?? null; }
function formatPrice(stock: StockSnapshot, value?: number | string | null) {
  const n = toNumber(value); if (n === null) return "n/a";
  const currency = stock.currency || stock.fundamentals?.currency || "USD";
  try { return new Intl.NumberFormat("en-US", { style: "currency", currency, maximumFractionDigits: 2 }).format(n); }
  catch { return `${currency} ${n.toFixed(2)}`; }
}
function groupName(group: Group) { return "sector" in group ? group.sector : group.region; }
function classFor(value?: string | null) { return (value || "Neutral").toLowerCase(); }
function snapshotClass(label?: string | null) {
  if (["Attractive", "Strong", "Low", "Positive", "Supportive"].includes(label || "")) return "good";
  if (["Expensive", "Weak", "High", "Negative", "Watch"].includes(label || "")) return "bad";
  return "neutral";
}
function decisionItems(snapshot?: DecisionSnapshot) {
  if (!snapshot) return [];
  return [["Val", snapshot.valuation], ["Qual", snapshot.quality], ["Risk", snapshot.risk], ["Mom", snapshot.momentum], ["Inc", snapshot.income]] as const;
}
function companySummary(stock: StockSnapshot) {
  const f = stock.fundamentals || {};
  const business = f.longBusinessSummary?.replace(/\s+/g, " ").trim() || `${stockName(stock)} operates in ${stock.industry || stock.sector || "its reported industry group"}.`;
  return [
    `Business: ${business}`,
    `Market context: ${stock.market || "n/a"}, ${stock.country || "n/a"}; ${stock.region || "n/a"} region.`,
    `Listing: ${stock.exchange || f.exchange || "n/a"}; currency ${stock.currency || f.currency || "n/a"}; industry ${stock.industry || f.industry || "n/a"}.`,
    `Market cap ${formatBig(stockValue(stock, "marketCap", stock.market_cap))}; latest close ${formatPrice(stock, stock.current_price)}; 52-week range ${formatPrice(stock, f.fiftyTwoWeekLow)} to ${formatPrice(stock, f.fiftyTwoWeekHigh)}.`,
    `Growth and profitability: revenue ${formatPercent(f.revenueGrowth)}, earnings ${formatPercent(f.earningsGrowth)}, margin ${formatPercent(f.profitMargins)}, ROE ${formatPercent(f.returnOnEquity)}.`,
    `Balance sheet and risk: debt/equity ${formatRatio(f.debtToEquity)}, current ratio ${formatRatio(f.currentRatio)}, beta ${formatRatio(f.beta)}.`,
    `Dividend context: yield ${formatYield(stockValue(stock, "dividendYield", stock.dividend_yield))}, payout ratio ${formatPercent(f.payoutRatio)}.`
  ].join(" ");
}
function formatTimestamp(value?: string | null) {
  if (!value) return "n/a"; const date = new Date(value);
  return Number.isNaN(date.valueOf()) ? value : `${date.toLocaleString("en-GB", { dateStyle: "medium", timeStyle: "short", timeZone: "UTC" })} UTC`;
}

function ExplainButton({ className, label, title, content, onActivate, onShow, onHide, onPin }: { className: string; label: string; title: string; content: string; onActivate?: () => void; onShow: PopoverHandler; onHide: () => void; onPin: PopoverHandler }) {
  function pointerPosition(event: MouseEvent<HTMLButtonElement>) {
    const bounds = event.currentTarget.getBoundingClientRect();
    return event.clientX || event.clientY
      ? { x: event.clientX, y: event.clientY }
      : { x: bounds.right, y: bounds.bottom };
  }

  function focusPosition(event: FocusEvent<HTMLButtonElement>) {
    const bounds = event.currentTarget.getBoundingClientRect();
    return { x: bounds.right, y: bounds.top };
  }

  return <button className={className} onBlur={onHide} onClick={(event) => { event.stopPropagation(); if (onActivate) { onHide(); onActivate(); return; } const point = pointerPosition(event); onPin(title, content, point.x, point.y); }} onFocus={(event) => { const point = focusPosition(event); onShow(title, content, point.x, point.y); }} onMouseEnter={(event) => { const point = pointerPosition(event); onShow(title, content, point.x, point.y); }} onMouseLeave={onHide} onMouseMove={(event) => { const point = pointerPosition(event); onShow(title, content, point.x, point.y); }} type="button">{label}</button>;
}

export function PortalDashboard({ report, preferences, scoringWeights, signedIn, user, watchlists }: Props) {
  const initialRegion = REGION_ORDER.includes(preferences.default_region as RegionMode) ? preferences.default_region as RegionMode : "Sectors";
  const [regionMode, setRegionMode] = useState<RegionMode>(initialRegion);
  const [direction, setDirection] = useState<Direction>("All");
  const [selectedGroup, setSelectedGroup] = useState(preferences.default_sector);
  const [selectedMarket, setSelectedMarket] = useState(preferences.default_market);
  const [search, setSearch] = useState("");
  const [sortKey, setSortKey] = useState("dollar_vol_latest");
  const [sortDirection, setSortDirection] = useState<"asc" | "desc">("desc");
  const [namedWatchlists, setNamedWatchlists] = useState(watchlists);
  const [activeWatchlistId, setActiveWatchlistId] = useState(
    watchlists.find((item) => item.is_default)?.id || watchlists[0]?.id || ""
  );
  const [watchlistOnly, setWatchlistOnly] = useState(false);
  const [visibleColumns, setVisibleColumns] = useState(preferences.visible_columns.length ? preferences.visible_columns : DEFAULT_COLUMNS);
  const [theme, setTheme] = useState<UserPreferences["theme"]>(preferences.theme || "system");
  const [preferenceState, setPreferenceState] = useState(preferences);
  const [columnMenuOpen, setColumnMenuOpen] = useState(false);
  const [status, setStatus] = useState("");
  const [popover, setPopover] = useState<Popover>(null);
  const [currentReport, setCurrentReport] = useState(report);
  const [reportMessage, setReportMessage] = useState("Connecting to the market data service...");
  const [reportRetry, setReportRetry] = useState(0);
  const [refreshBaseline, setRefreshBaseline] = useState<string | null>(null);
  const [refreshState, setRefreshState] = useState<"idle" | "requesting" | "queued" | "complete" | "error">("idle");
  const [refreshMessage, setRefreshMessage] = useState("");
  const [refreshStage, setRefreshStage] = useState("");
  const [refreshProgress, setRefreshProgress] = useState(0);
  const [refreshRunId, setRefreshRunId] = useState<number | undefined>();
  const [refreshBaselineRunId, setRefreshBaselineRunId] = useState<number | undefined>();
  const [selectedStock, setSelectedStock] = useState<StockSnapshot | null>(null);
  const [watchlistEditor, setWatchlistEditor] = useState<"new" | "rename" | null>(null);
  const [watchlistName, setWatchlistName] = useState("");
  const [personalizationOpen, setPersonalizationOpen] = useState(false);
  const [weightState, setWeightState] = useState(scoringWeights);
  const [isPending, startTransition] = useTransition();

  useEffect(() => {
    const stored = window.localStorage.getItem("stare-theme") as UserPreferences["theme"] | null;
    const next = signedIn ? theme : stored || theme;
    document.documentElement.dataset.theme = next;
    if (!signedIn && stored && stored !== theme) setTheme(stored);
  }, [signedIn, theme]);
  useEffect(() => {
    const close = (event: KeyboardEvent) => event.key === "Escape" && setPopover(null);
    document.addEventListener("keydown", close); return () => document.removeEventListener("keydown", close);
  }, []);
  useEffect(() => {
    if (currentReport?.update) return;

    let cancelled = false;
    let retryTimer: ReturnType<typeof setTimeout> | undefined;
    let attempt = 0;

    async function loadReport() {
      attempt += 1;
      setReportMessage(attempt === 1
        ? "Connecting to the market data service..."
        : "The data service is waking up. This can take about a minute on the free hosting tier.");

      try {
        const response = await fetch("/api/report", { cache: "no-store" });
        if (!response.ok) throw new Error(`Report request returned ${response.status}.`);
        const nextReport = await response.json() as LatestReport;
        if (!nextReport.update) throw new Error("Report response has no completed update.");
        if (!cancelled) setCurrentReport(nextReport);
      } catch {
        if (!cancelled) retryTimer = setTimeout(loadReport, 5_000);
      }
    }

    void loadReport();
    return () => {
      cancelled = true;
      if (retryTimer) clearTimeout(retryTimer);
    };
  }, [currentReport?.update, reportRetry]);
  useEffect(() => {
    if (!refreshBaseline) return;

    let cancelled = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let attempts = 0;

    async function checkForUpdatedReport() {
      attempts += 1;
      try {
        const response = await fetch("/api/report", { cache: "no-store" });
        if (response.ok) {
          const nextReport = await response.json() as LatestReport;
          const nextVersion = nextReport.update?.id || nextReport.update?.completed_at;
          if (nextReport.update && nextVersion && nextVersion !== refreshBaseline) {
            if (!cancelled) {
              setCurrentReport(nextReport);
              setRefreshState("complete");
              setRefreshProgress(100);
              setRefreshStage("Report ready");
              setRefreshMessage(`Update complete. Market data date: ${nextReport.update.latest_price_date || nextReport.update.market_data_date || "n/a"}.`);
              setRefreshBaseline(null);
            }
            return;
          }
        }
      } catch {
        // A sleeping service can miss an individual check; the next poll retries it.
      }

      if (!cancelled && attempts < 80) {
        timer = setTimeout(checkForUpdatedReport, 15_000);
      } else if (!cancelled) {
        setRefreshState("error");
        setRefreshStage("Report confirmation timed out");
        setRefreshMessage("The update is taking longer than expected. Reload later to see the newest completed report.");
        setRefreshBaseline(null);
      }
    }

    timer = setTimeout(checkForUpdatedReport, 15_000);
    return () => {
      cancelled = true;
      if (timer) clearTimeout(timer);
    };
  }, [refreshBaseline]);
  useEffect(() => {
    if (refreshState !== "queued") return;

    let cancelled = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let failures = 0;

    async function checkWorkflowProgress() {
      const result = await getMarketRefreshProgress(refreshBaselineRunId, refreshRunId);
      if (cancelled) return;

      if (!result.ok) {
        failures += 1;
        if (failures >= 4) {
          setRefreshState("error");
          setRefreshStage("Progress unavailable");
          setRefreshMessage(result.error);
          setRefreshBaseline(null);
          return;
        }
      } else {
        failures = 0;
        setRefreshProgress(Math.max(0, Math.min(100, result.progress.progress)));
        setRefreshStage(result.progress.stage);
        setRefreshMessage(result.progress.message);
        if (!refreshRunId && result.progress.workflow_run_id) setRefreshRunId(result.progress.workflow_run_id);

        if (result.progress.status === "failed") {
          setRefreshState("error");
          setRefreshBaseline(null);
          return;
        }
        if (result.progress.status === "success") return;
      }

      timer = setTimeout(checkWorkflowProgress, 7_500);
    }

    void checkWorkflowProgress();
    return () => {
      cancelled = true;
      if (timer) clearTimeout(timer);
    };
  }, [refreshBaselineRunId, refreshRunId, refreshState]);

  const allStocks = currentReport?.top_stocks || [];
  const regions = currentReport?.regions || [];
  const sectors = currentReport?.sectors || [];
  const activeWatchlist = namedWatchlists.find((item) => item.id === activeWatchlistId) || null;
  const watchlist = activeWatchlist?.tickers || (namedWatchlists.length ? [] : preferences.watchlist || []);
  const regionalTopRows = useMemo(() => {
    const rows: StockSnapshot[] = [];
    for (const region of ["NA", "LAC", "EMEA", "APAC"]) {
      const unique = new Map<string, StockSnapshot>();
      allStocks.filter((stock) => stock.region === region).sort((a, b) => (toNumber(b.dollar_vol_latest) || 0) - (toNumber(a.dollar_vol_latest) || 0)).forEach((stock) => { if (!unique.has(stock.ticker)) unique.set(stock.ticker, stock); });
      rows.push(...Array.from(unique.values()).slice(0, 10));
    }
    return rows;
  }, [allStocks]);
  const sourceGroups: Group[] = regionMode === "Sectors" ? sectors : regions.filter((region) => regionMode === "All" || region.region === regionMode);
  const baseRows = regionMode === "All" ? regionalTopRows : allStocks.filter((stock) => regionMode === "Sectors" ? stock.region === "NA" : stock.region === regionMode);
  const markets = useMemo(() => {
    if (["All", "NA", "Sectors"].includes(regionMode)) return [];
    const map = new Map<string, { market: string; country: string; count: number }>();
    allStocks.filter((stock) => stock.region === regionMode).forEach((stock) => {
      const market = stock.market || "Other"; const current = map.get(market) || { market, country: stock.country || market, count: 0 };
      current.count += 1; map.set(market, current);
    });
    return Array.from(map.values()).sort((a, b) => b.count - a.count || a.country.localeCompare(b.country));
  }, [allStocks, regionMode]);
  const groupDirection = (stock: StockSnapshot) => regionMode === "Sectors" ? sectors.find((sector) => sector.sector === stock.sector)?.direction : regions.find((region) => region.region === stock.region)?.direction;
  const groups = sourceGroups.filter((group) => {
    if (direction !== "All" && group.direction !== direction) return false;
    if (selectedGroup && regionMode === "Sectors" && groupName(group) !== selectedGroup) return false;
    if (!search.trim()) return true;
    const query = search.toLowerCase();
    return groupName(group).toLowerCase().includes(query) || baseRows.some((stock) => {
      const belongsToGroup = regionMode === "Sectors" ? stock.sector === groupName(group) : stock.region === groupName(group);
      return belongsToGroup && [stock.ticker, stockName(stock), stock.industry, stock.market, stock.country].some((value) => String(value || "").toLowerCase().includes(query));
    });
  });
  const displayedRows = useMemo(() => {
    let rows = [...baseRows];
    if (selectedGroup && regionMode === "Sectors") rows = rows.filter((stock) => stock.sector === selectedGroup);
    if (selectedMarket) rows = rows.filter((stock) => stock.market === selectedMarket);
    if (direction !== "All") rows = rows.filter((stock) => groupDirection(stock) === direction);
    if (watchlistOnly) rows = rows.filter((stock) => watchlist.includes(stock.ticker));
    if (search.trim()) { const query = search.trim().toLowerCase(); rows = rows.filter((stock) => [stock.ticker, stockName(stock), stock.sector, stock.region, stock.market, stock.country, stock.industry, stock.exchange].some((value) => String(value || "").toLowerCase().includes(query))); }
    const ranked = [...rows].sort((a, b) => (toNumber(a.dollar_vol_latest) || 0) - (toNumber(b.dollar_vol_latest) || 0));
    const percentiles = new Map(ranked.map((stock, index) => [`${stock.region}|${stock.market}|${stock.sector}|${stock.ticker}`, ranked.length ? ((index + 1) / ranked.length) * 100 : null]));
    const enriched: DisplayStock[] = rows.map((stock) => ({ ...stock, daily_percentile: percentiles.get(`${stock.region}|${stock.market}|${stock.sector}|${stock.ticker}`) ?? null }));
    return enriched.sort((a, b) => { const factor = sortDirection === "asc" ? 1 : -1; const av = sortValue(a, sortKey); const bv = sortValue(b, sortKey); if (typeof av === "number" || typeof bv === "number") return ((Number(av) || 0) - (Number(bv) || 0)) * factor; return String(av || "").localeCompare(String(bv || "")) * factor; });
  // The group-direction lookup is derived from the same report arrays listed here.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [baseRows, selectedGroup, selectedMarket, direction, search, sortKey, sortDirection, regionMode, sectors, regions, watchlistOnly, watchlist]);
  const topPicks = useMemo(() => {
    const byVolume = [...displayedRows].sort((a, b) => (toNumber(b.dollar_vol_latest) || 0) - (toNumber(a.dollar_vol_latest) || 0));
    if (regionMode === "All") return ["NA", "LAC", "EMEA", "APAC"].flatMap((region) => byVolume.filter((stock) => stock.region === region).slice(0, 3));
    if (regionMode !== "Sectors") return byVolume.slice(0, 9);
    if (selectedGroup) return byVolume.slice(0, 6);
    return [...groups].sort((a, b) => (b.strength || 0) - (a.strength || 0)).map((group) => byVolume.find((stock) => stock.sector === groupName(group))).filter((stock): stock is DisplayStock => Boolean(stock)).slice(0, 6);
  }, [displayedRows, groups, regionMode, selectedGroup]);
  const bullish = groups.filter((group) => group.direction === "Bullish").length;
  const bearish = groups.filter((group) => group.direction === "Bearish").length;
  const averageStrength = groups.length ? groups.reduce((sum, group) => sum + (group.strength || 0), 0) / groups.length : 0;
  const selectedGroupSignal = selectedStock
    ? selectedStock.region === "NA"
      ? sectors.find((sector) => sector.sector === selectedStock.sector)
      : regions.find((region) => region.region === selectedStock.region)
    : null;

  function positionPopover(x: number, y: number) {
    const margin = 12;
    const gap = 18;
    const width = Math.min(560, window.innerWidth - margin * 2);
    const height = Math.min(460, window.innerHeight - margin * 2);
    const left = x + gap + width > window.innerWidth
      ? Math.max(margin, x - width - gap)
      : x + gap;
    const top = y + gap + height > window.innerHeight
      ? Math.max(margin, y - height - gap)
      : y + gap;
    return { x: left, y: top };
  }
  function showPopover(title: string, content: string, x: number, y: number) { setPopover((current) => current?.pinned ? current : { title, content, pinned: false, ...positionPopover(x, y) }); }
  function hidePopover() { setPopover((current) => current?.pinned ? current : null); }
  function pinPopover(title: string, content: string, x: number, y: number) { setPopover((current) => current?.pinned && current.title === title ? null : { title, content, pinned: true, ...positionPopover(x, y) }); }
  function persist(overrides: Partial<UserPreferences>) {
    const next = { ...preferenceState, watchlist, visible_columns: visibleColumns, ...overrides };
    setPreferenceState(next); if (!signedIn) return; setStatus("");
    startTransition(() => { void (async () => { const result = await savePreferences(next); if (result.ok) { setPreferenceState(result.preferences); setStatus("Preferences saved."); } else setStatus(result.error); })(); });
  }
  function chooseRegion(region: RegionMode) { setRegionMode(region); setSelectedGroup(null); setSelectedMarket(null); persist({ default_region: region, default_sector: null, default_market: null }); }
  function chooseGroup(name: string) { const next = selectedGroup === name ? null : name; setSelectedGroup(next); persist({ default_sector: next }); }
  function chooseMarket(market: string) { const next = selectedMarket === market ? null : market; setSelectedMarket(next); persist({ default_market: next }); }
  function replaceNamedWatchlist(next: UserWatchlist) {
    setNamedWatchlists((current) => current.map((item) => item.id === next.id ? next : item));
  }
  function selectWatchlist(id: string) {
    setActiveWatchlistId(id);
    setWatchlistOnly(false);
    if (!signedIn) return;
    setNamedWatchlists((current) => current.map((item) => ({ ...item, is_default: item.id === id })));
    startTransition(() => { void (async () => {
      const result = await saveWatchlist(id, { is_default: true });
      if (result.ok) replaceNamedWatchlist(result.watchlist); else setStatus(result.error);
    })(); });
  }
  function submitWatchlistName() {
    const name = watchlistName.trim();
    if (!name) return;
    startTransition(() => { void (async () => {
      if (watchlistEditor === "rename" && activeWatchlist) {
        const result = await saveWatchlist(activeWatchlist.id, { name });
        if (result.ok) {
          replaceNamedWatchlist(result.watchlist);
          setStatus("Watchlist renamed.");
          setWatchlistEditor(null);
          setWatchlistName("");
        } else setStatus(result.error);
      } else {
        const result = await createWatchlist(name);
        if (result.ok) {
          setNamedWatchlists((current) => [
            ...current.map((item) => ({ ...item, is_default: false })),
            result.watchlist
          ]);
          setActiveWatchlistId(result.watchlist.id);
          setStatus("Watchlist created.");
          setWatchlistEditor(null);
          setWatchlistName("");
        } else setStatus(result.error);
      }
    })(); });
  }
  function removeActiveWatchlist() {
    if (!activeWatchlist || !window.confirm(`Delete ${activeWatchlist.name}?`)) return;
    startTransition(() => { void (async () => {
      const result = await deleteWatchlist(activeWatchlist.id);
      if (!result.ok) { setStatus(result.error); return; }
      const remaining = namedWatchlists
        .filter((item) => item.id !== activeWatchlist.id)
        .map((item, index) => ({ ...item, is_default: index === 0 }));
      setNamedWatchlists(remaining);
      setActiveWatchlistId(remaining[0]?.id || "");
      setWatchlistOnly(false);
      setStatus("Watchlist deleted.");
    })(); });
  }
  function toggleWatchlist(ticker: string) {
    if (!signedIn) { setStatus("Sign in to save a watchlist."); return; }
    if (!activeWatchlist) {
      startTransition(() => { void (async () => {
        const result = await createWatchlist("My Watchlist", [ticker]);
        if (result.ok) {
          setNamedWatchlists((current) => [
            ...current.map((item) => ({ ...item, is_default: false })),
            result.watchlist
          ]);
          setActiveWatchlistId(result.watchlist.id);
          setStatus(`${ticker} added to My Watchlist.`);
        } else setStatus(result.error);
      })(); });
      return;
    }
    const previous = activeWatchlist;
    const tickers = watchlist.includes(ticker)
      ? watchlist.filter((item) => item !== ticker)
      : [...watchlist, ticker];
    replaceNamedWatchlist({ ...activeWatchlist, tickers });
    startTransition(() => { void (async () => {
      const result = await saveWatchlist(activeWatchlist.id, { tickers });
      if (result.ok) { replaceNamedWatchlist(result.watchlist); setStatus("Watchlist saved."); }
      else { replaceNamedWatchlist(previous); setStatus(result.error); }
    })(); });
  }
  function persistScoringWeights() {
    startTransition(() => { void (async () => {
      const result = await saveScoringWeights(weightState);
      if (result.ok) { setWeightState(result.weights); setStatus("Scoring weights saved."); }
      else setStatus(result.error);
    })(); });
  }
  function restoreScoringWeights() {
    startTransition(() => { void (async () => {
      const result = await resetScoringWeights();
      if (result.ok) { setWeightState(result.weights); setStatus("Scoring weights reset."); }
      else setStatus(result.error);
    })(); });
  }
  function toggleColumn(column: string) { const next = visibleColumns.includes(column) ? visibleColumns.filter((item) => item !== column) : DEFAULT_COLUMNS.filter((item) => item === column || visibleColumns.includes(item)); setVisibleColumns(next); persist({ visible_columns: next }); }
  function toggleTheme() { const next = theme === "dark" ? "light" : "dark"; setTheme(next); window.localStorage.setItem("stare-theme", next); document.documentElement.dataset.theme = next; persist({ theme: next }); }
  function sortBy(key: string) { if (sortKey === key) setSortDirection((current) => current === "asc" ? "desc" : "asc"); else { setSortKey(key); setSortDirection("asc"); } }
  function resetOverview() { setRegionMode("Sectors"); setDirection("All"); setSelectedGroup(null); setSelectedMarket(null); setSearch(""); persist({ default_region: "Sectors", default_sector: null, default_market: null }); }
  async function refreshData() {
    if (refreshState === "requesting" || refreshState === "queued") return;
    setRefreshState("requesting");
    setRefreshProgress(1);
    setRefreshStage("Submitting update request");
    setRefreshRunId(undefined);
    setRefreshBaselineRunId(undefined);
    setRefreshMessage("Requesting a fresh market update...");
    const result = await startMarketRefresh();
    if (!result.ok) {
      setRefreshState("error");
      setRefreshProgress(0);
      setRefreshStage("Update could not start");
      setRefreshMessage(result.error);
      return;
    }

    setRefreshState("queued");
    setRefreshProgress(result.workflowRunId ? 5 : 2);
    setRefreshStage(result.workflowRunId ? "Reading update progress" : "Waiting for GitHub Actions");
    setRefreshRunId(result.workflowRunId);
    setRefreshBaselineRunId(result.baselineRunId);
    setRefreshMessage(result.message);
    setRefreshBaseline(currentReport?.update?.id || currentReport?.update?.completed_at || "initial-report");
  }

  if (!currentReport?.update) return <section className="empty-state report-loading" aria-live="polite"><p className="eyebrow">Market data</p><h1>Loading the latest report</h1><p>{reportMessage}</p><button className="button secondary" onClick={() => setReportRetry((value) => value + 1)} type="button">Retry now</button></section>;

  return <div className="portal-layout" onClick={() => popover?.pinned && setPopover(null)}>
    <aside className="sidebar" onClick={(event) => event.stopPropagation()}>
      {signedIn && user ? <section className="sidebar-account" aria-label="Signed-in account"><div><span className="control-label">Signed in</span><strong title={user.displayName}>{user.displayName}</strong><small title={user.email}>{user.email}</small></div><AuthButton className="button secondary sidebar-signout" label="Sign out" signedIn /></section> : null}
      <div className="sidebar-section"><label className="control-label" htmlFor="stock-search">Search</label><input autoComplete="off" className="search-input" id="stock-search" onChange={(event) => setSearch(event.target.value)} placeholder="Ticker, company, group" type="search" value={search} /></div>
      <div className="sidebar-section"><span className="control-label">Direction</span><div className="segmented direction-control">{(["All", "Bullish", "Bearish", "Neutral"] as Direction[]).map((item) => <button className={direction === item ? "active" : ""} key={item} onClick={() => setDirection(item)} type="button">{item}</button>)}</div></div>
      <div className="sidebar-section"><span className="control-label">Regions</span><div className="segmented region-control">{REGION_ORDER.map((region) => <button className={regionMode === region ? "active" : ""} key={region} onClick={() => chooseRegion(region)} type="button">{REGION_LABELS[region]}</button>)}</div></div>
      {signedIn ? <div className="sidebar-section personalization-controls"><span className="control-label">Watchlists</span><select aria-label="Active watchlist" onChange={(event) => selectWatchlist(event.target.value)} value={activeWatchlistId}>{!namedWatchlists.length ? <option value="">No watchlist</option> : null}{namedWatchlists.map((item) => <option key={item.id} value={item.id}>{item.name} ({item.tickers.length})</option>)}</select><div className="personalization-commands"><button className="button secondary" onClick={() => { setWatchlistEditor("new"); setWatchlistName(""); }} type="button">New</button><button className="button secondary" disabled={!activeWatchlist} onClick={() => { setWatchlistEditor("rename"); setWatchlistName(activeWatchlist?.name || ""); }} type="button">Rename</button><button aria-label="Delete active watchlist" className="button secondary danger" disabled={!activeWatchlist} onClick={removeActiveWatchlist} type="button">Delete</button></div>{watchlistEditor ? <div className="watchlist-editor"><input aria-label={watchlistEditor === "new" ? "New watchlist name" : "Rename watchlist"} autoFocus maxLength={60} onChange={(event) => setWatchlistName(event.target.value)} onKeyDown={(event) => { if (event.key === "Enter") submitWatchlistName(); if (event.key === "Escape") setWatchlistEditor(null); }} placeholder="Watchlist name" value={watchlistName} /><div><button className="button" disabled={!watchlistName.trim()} onClick={submitWatchlistName} type="button">Save</button><button className="button secondary" onClick={() => setWatchlistEditor(null)} type="button">Cancel</button></div></div> : null}<button aria-expanded={personalizationOpen} className="button secondary scoring-toggle" onClick={() => setPersonalizationOpen((open) => !open)} type="button">Scoring weights</button>{personalizationOpen ? <div className="scoring-controls">{SCORING_FACTORS.map(({ key, label }) => <label key={key}><span>{label}<output>{Number(weightState[key] ?? 1).toFixed(1)}x</output></span><input max="2" min="0" onChange={(event) => setWeightState((current) => ({ ...current, [key]: Number(event.target.value) }))} step="0.1" type="range" value={weightState[key] ?? defaultScoringWeights[key]} /></label>)}<div className="personalization-commands"><button className="button" disabled={SCORING_FACTORS.every(({ key }) => Number(weightState[key]) === 0)} onClick={persistScoringWeights} type="button">Save</button><button className="button secondary" onClick={restoreScoringWeights} type="button">Reset</button></div></div> : null}</div> : null}
      {regionMode === "Sectors" ? <div className="sidebar-section group-list"><span className="control-label">Sectors</span>{sectors.map((sector) => <button className={selectedGroup === sector.sector ? "group-button active" : "group-button"} key={sector.sector} onClick={() => chooseGroup(sector.sector)} type="button"><span><strong>{sector.sector}</strong><small>{sector.direction} · {sector.week_ending}</small></span><b className={classFor(sector.direction)}>{sector.strength}</b></button>)}</div>
      : markets.length ? <div className="sidebar-section group-list"><span className="control-label">Countries</span>{markets.map((market) => <button className={selectedMarket === market.market ? "group-button active" : "group-button"} key={market.market} onClick={() => chooseMarket(market.market)} type="button"><span><strong>{market.country}</strong><small>{market.market}</small></span><b>{market.count}</b></button>)}</div> : null}
    </aside>

    <div className="workspace">
      <section className="workspace-header"><div><p className="eyebrow">{REGION_LABELS[regionMode]}</p><h1>Stock Trend Analysis Risk Engine</h1><p className="lede">{regionMode === "Sectors" ? "North American sector strength and active S&P 500 names." : regionMode === "All" ? "Top active stocks and market direction across all covered regions." : `${regionMode} market direction, countries, and top active stocks.`}</p><p className="disclaimer">Deterministic research signals, not personalized financial advice.</p></div><div className="workspace-action-area"><div className="workspace-actions"><button className="button secondary" onClick={resetOverview} type="button">Overview</button><button className="button secondary" disabled={refreshState === "requesting" || refreshState === "queued"} onClick={refreshData} type="button">{refreshState === "requesting" ? "Requesting..." : refreshState === "queued" ? "Updating..." : "Refresh data"}</button><button aria-pressed={theme === "dark"} className="button secondary" onClick={toggleTheme} type="button">{theme === "dark" ? "Light" : "Dark"}</button><button className="button secondary" onClick={() => window.print()} type="button">Print</button></div>{refreshState !== "idle" ? <div className={`refresh-progress-panel ${refreshState}`}><div className="refresh-progress-meta"><span>{refreshStage}</span><strong>{refreshProgress}%</strong></div><div aria-label={refreshStage} aria-valuemax={100} aria-valuemin={0} aria-valuenow={refreshProgress} className="refresh-progress-track" role="progressbar"><span style={{ width: `${refreshProgress}%` }} /></div>{refreshMessage ? <p className={`refresh-status ${refreshState}`} aria-live="polite">{refreshMessage}</p> : null}</div> : null}</div></section>
      <section className="signal-strip"><SignalExplanation title="Fundamentals Snapshot" text="P/E · P/B · PEG · dividend yield" content={HELP.fundamentals} onShow={showPopover} onHide={hidePopover} onPin={pinPopover} /><SignalExplanation title="Sector Strength" text="Breadth · returns · trading activity" content={HELP.strength} onShow={showPopover} onHide={hidePopover} onPin={pinPopover} /><SignalExplanation title="Buy / Hold / Sell" text="Valuation · momentum · market context" content={HELP.recommendation} onShow={showPopover} onHide={hidePopover} onPin={pinPopover} /></section>
      <section className="kpi-strip" aria-label="Dashboard KPIs"><Metric label={regionMode === "Sectors" ? "Sectors" : "Regions"} value={groups.length.toString()} /><Metric label="Bullish / Bearish" value={`${bullish} / ${bearish}`} /><Metric label="Avg Strength" value={averageStrength.toFixed(1)} /><Metric label="Tracked Names" value={new Set(displayedRows.map((stock) => stock.ticker)).size.toString()} /></section>
      <section className="section-block"><div className="section-heading"><div><p className="eyebrow">Market map</p><h2>{regionMode === "Sectors" ? "Sector Heatmap" : "Region Heatmap"}</h2></div><span>{groups.length} groups</span></div><div className="heatmap">{groups.map((group) => <button className={`heat-cell ${classFor(group.direction)}`} key={groupName(group)} onClick={() => regionMode === "Sectors" && chooseGroup(groupName(group))} type="button"><strong>{groupName(group)}</strong><span>{group.strength ?? 0}</span><small>{group.direction || "Neutral"}</small></button>)}</div></section>
      <div className="analysis-grid"><section className="section-block strength-section"><div className="section-heading"><div><p className="eyebrow">Comparison</p><h2>Strength by {regionMode === "Sectors" ? "Sector" : "Region"}</h2></div><span>Select a bar to inspect</span></div><div className="strength-chart">{[...groups].sort((a, b) => (b.strength || 0) - (a.strength || 0)).map((group) => <button className="chart-column" key={groupName(group)} onClick={() => regionMode === "Sectors" && chooseGroup(groupName(group))} type="button"><span className="chart-value">{group.strength ?? 0}</span><span className={`chart-bar ${classFor(group.direction)}`} style={{ height: `${Math.max(3, group.strength || 0)}%` }} /><span className="chart-label">{groupName(group)}</span></button>)}</div></section>
      <section className="section-block picks-section"><div className="section-heading"><div><p className="eyebrow">Last trading day</p><h2>Top Active Stocks <ExplainButton className="help-button" label="?" title="Top Active Stocks" content={HELP.topPicks} onShow={showPopover} onHide={hidePopover} onPin={pinPopover} /></h2></div><span>{topPicks.length} picks</span></div><div className="picks-grid">{topPicks.map((stock) => <article className="stock-card" key={`pick-${stock.region}-${stock.market}-${stock.ticker}`}><div className="stock-card-top"><div><button className="stock-card-ticker" onClick={() => setSelectedStock(stock)} type="button">{stock.ticker}</button><span>{stockName(stock)}</span></div><b className={classFor(stock.action)}>{formatPercent(stock.weekly_return)}</b></div><div className="stock-card-price">{formatPrice(stock, stock.current_price)}</div><div className="stock-card-metrics"><span>Signal <b className={classFor(stock.action)}>{stock.action || "Hold"}</b></span><span>Valuation <b>{stock.decision_snapshot?.valuation?.label || "n/a"}</b></span><span>Risk <b>{stock.decision_snapshot?.risk?.label || "n/a"}</b></span></div></article>)}</div></section></div>
      <section className="section-block table-section"><div className="section-heading table-heading"><div><p className="eyebrow">Complete snapshot</p><h2>{displayedRows.length} stocks</h2></div><div className="table-tools"><span className="save-status">{isPending ? "Saving..." : status}</span><button aria-pressed={watchlistOnly} className={watchlistOnly ? "button secondary active" : "button secondary"} disabled={!activeWatchlist && !watchlist.length} onClick={() => setWatchlistOnly((active) => !active)} type="button">{activeWatchlist?.name || "Watchlist"} ({watchlist.length})</button><div className="column-menu-wrap"><button className="button secondary" onClick={() => setColumnMenuOpen((open) => !open)} type="button">Columns</button>{columnMenuOpen ? <div className="column-menu">{DEFAULT_COLUMNS.map((column) => <label key={column}><input checked={visibleColumns.includes(column)} onChange={() => toggleColumn(column)} type="checkbox" /> {COLUMNS[column]}</label>)}</div> : null}</div></div></div>
      <div className="table-wrap"><table><thead><tr>{DEFAULT_COLUMNS.filter((column) => visibleColumns.includes(column)).map((column) => <th className={NUMERIC_COLUMNS.has(column) ? "num" : ""} key={column}>{!["watch", "decision"].includes(column) ? <button className="sort-button" onClick={() => sortBy(column)} type="button">{COLUMNS[column]} <span aria-hidden="true">{sortKey === column ? sortDirection === "asc" ? "↑" : "↓" : "↕"}</span></button> : COLUMNS[column]}</th>)}</tr></thead><tbody>{displayedRows.map((stock) => <StockRow key={`${stock.region}-${stock.market}-${stock.sector}-${stock.ticker}`} stock={stock} columns={visibleColumns} watched={watchlist.includes(stock.ticker)} onOpenStock={setSelectedStock} onWatch={toggleWatchlist} onShow={showPopover} onHide={hidePopover} onPin={pinPopover} />)}</tbody></table></div></section>
      <footer className="data-footer"><span>Updated {formatTimestamp(currentReport.update.completed_at)} · Market data {currentReport.update.latest_price_date || currentReport.update.market_data_date || "n/a"}</span><span>Source: Yahoo Finance market and fundamental data. Signals are deterministic research outputs and may be incomplete or delayed.</span><span>Created by vittok. GitHub Pages remains available as the static fallback.</span></footer>
    </div>
    {popover ? <aside className={`summary-popover ${popover.pinned ? "pinned" : "hovering"}`} onClick={(event) => event.stopPropagation()} style={{ left: popover.x, top: popover.y }}><div><strong>{popover.title}</strong>{popover.pinned ? <button aria-label="Close explanation" onClick={() => setPopover(null)} type="button">×</button> : null}</div><p>{popover.content}</p></aside> : null}
    <StockDetailDialog groupSignal={selectedGroupSignal ? { direction: selectedGroupSignal.direction, name: groupName(selectedGroupSignal), strength: selectedGroupSignal.strength } : null} onClose={() => setSelectedStock(null)} stock={selectedStock} />
  </div>;
}

function sortValue(stock: DisplayStock, key: string): string | number | null {
  const values: Record<string, string | number | null | undefined> = { group: stock.sector || stock.region, rank: stock.rank, ticker: stock.ticker, name: stockName(stock), current_price: toNumber(stock.current_price), previous_close: toNumber(stock.previous_close), action: stock.action, decision: stock.decision_snapshot?.summary, weekly_return: toNumber(stock.weekly_return), daily_percentile: stock.daily_percentile, dollar_vol_latest: toNumber(stock.dollar_vol_latest), market_cap: toNumber(stockValue(stock, "marketCap", stock.market_cap)), trailing_pe: toNumber(stockValue(stock, "trailingPE", stock.trailing_pe)), price_to_book: toNumber(stockValue(stock, "priceToBook", stock.price_to_book)), peg_ratio: toNumber(stockValue(stock, "pegRatio", stock.peg_ratio)), dividend_yield: toNumber(stockValue(stock, "dividendYield", stock.dividend_yield)) };
  return values[key] ?? null;
}
function SignalExplanation({ title, text, content, onShow, onHide, onPin }: { title: string; text: string; content: string; onShow: PopoverHandler; onHide: () => void; onPin: PopoverHandler }) { return <div><h2>{title} <ExplainButton className="help-button" label="?" title={title} content={content} onShow={onShow} onHide={onHide} onPin={onPin} /></h2><p>{text}</p></div>; }
function Metric({ label, value }: { label: string; value: string }) { return <div><span>{label}</span><strong>{value}</strong></div>; }

function StockRow({ stock, columns, watched, onOpenStock, onWatch, onShow, onHide, onPin }: { stock: DisplayStock; columns: string[]; watched: boolean; onOpenStock: (stock: StockSnapshot) => void; onWatch: (ticker: string) => void; onShow: PopoverHandler; onHide: () => void; onPin: PopoverHandler }) {
  const snapshot = stock.decision_snapshot;
  const closeDirection = stock.close_direction === "up" ? "positive" : stock.close_direction === "down" ? "negative" : "neutral";
  const values: Record<string, React.ReactNode> = {
    watch: <button aria-label={`${watched ? "Remove" : "Add"} ${stock.ticker} ${watched ? "from" : "to"} watchlist`} className={watched ? "watch-button active" : "watch-button"} onClick={() => onWatch(stock.ticker)} type="button">{watched ? "★" : "☆"}</button>,
    group: <span><strong>{stock.sector || stock.region || "n/a"}</strong>{stock.market ? <small>{stock.market}</small> : null}</span>, rank: stock.rank ?? "n/a",
    ticker: <ExplainButton className="text-link ticker-link" label={stock.ticker} title={`${stock.ticker} daily summary`} content={stock.daily_summary || "No generated summary is available for this ticker today."} onActivate={() => onOpenStock(stock)} onShow={onShow} onHide={onHide} onPin={onPin} />,
    name: <ExplainButton className="text-link name-link" label={stockName(stock)} title={stockName(stock)} content={companySummary(stock)} onShow={onShow} onHide={onHide} onPin={onPin} />,
    current_price: formatPrice(stock, stock.current_price), previous_close: <span className={`close-value ${closeDirection}`}>{formatPrice(stock, stock.previous_close)} <b>{stock.close_direction === "up" ? "▲" : stock.close_direction === "down" ? "▼" : "●"}</b><small>{stock.previous_close_date || "n/a"} · {formatPercent(stock.close_change_pct)}</small></span>,
    action: <span className={`signal ${classFor(stock.action)}`}>{stock.action || "Hold"}</span>,
    decision: <div className="snapshot-tags">{decisionItems(snapshot).map(([short, item]) => item?.label ? <ExplainButton className={`snapshot-tag ${snapshotClass(item.label)}`} key={short} label={`${short}: ${item.label}`} title={`${stock.ticker} ${short}`} content={`${item.detail || "No detail available."} ${snapshot?.summary || ""}`.trim()} onShow={onShow} onHide={onHide} onPin={onPin} /> : null)}</div>,
    weekly_return: <span className={(toNumber(stock.weekly_return) || 0) >= 0 ? "positive" : "negative"}>{formatPercent(stock.weekly_return)}</span>, daily_percentile: stock.daily_percentile === null ? "n/a" : `${stock.daily_percentile.toFixed(0)}%`, dollar_vol_latest: formatBig(stock.dollar_vol_latest || stock.dollar_vol_week), market_cap: formatBig(stockValue(stock, "marketCap", stock.market_cap)), trailing_pe: formatRatio(stockValue(stock, "trailingPE", stock.trailing_pe)), price_to_book: formatRatio(stockValue(stock, "priceToBook", stock.price_to_book)), peg_ratio: formatRatio(stockValue(stock, "pegRatio", stock.peg_ratio)), dividend_yield: formatYield(stockValue(stock, "dividendYield", stock.dividend_yield))
  };
  return <tr>{DEFAULT_COLUMNS.filter((column) => columns.includes(column)).map((column) => <td className={NUMERIC_COLUMNS.has(column) ? "num" : ""} key={column}>{values[column]}</td>)}</tr>;
}
