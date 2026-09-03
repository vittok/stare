export type UpdateInfo = {
  id?: string;
  run_label?: string | null;
  status?: string | null;
  started_at?: string | null;
  completed_at?: string | null;
  market_data_date?: string | null;
  latest_price_date?: string | null;
};

export type RegionSnapshot = {
  region: string;
  week_ending?: string | null;
  direction?: string | null;
  strength?: number | null;
  raw_score?: number | string | null;
  diagnostics?: Record<string, unknown>;
};

export type SectorSnapshot = {
  sector: string;
  week_ending?: string | null;
  direction?: string | null;
  strength?: number | null;
  raw_score?: number | string | null;
  diagnostics?: Record<string, unknown>;
};

export type DecisionItem = {
  label?: string | null;
  detail?: string | null;
};

export type DecisionSnapshot = {
  valuation?: DecisionItem;
  quality?: DecisionItem;
  risk?: DecisionItem;
  momentum?: DecisionItem;
  income?: DecisionItem;
  summary?: string | null;
};

export type StockFundamentals = {
  shortName?: string | null;
  industry?: string | null;
  exchange?: string | null;
  currency?: string | null;
  longBusinessSummary?: string | null;
  marketCap?: number | string | null;
  trailingPE?: number | string | null;
  forwardPE?: number | string | null;
  priceToBook?: number | string | null;
  pegRatio?: number | string | null;
  profitMargins?: number | string | null;
  operatingMargins?: number | string | null;
  grossMargins?: number | string | null;
  returnOnEquity?: number | string | null;
  returnOnAssets?: number | string | null;
  revenueGrowth?: number | string | null;
  earningsGrowth?: number | string | null;
  totalDebt?: number | string | null;
  debtToEquity?: number | string | null;
  currentRatio?: number | string | null;
  quickRatio?: number | string | null;
  dividendYield?: number | string | null;
  payoutRatio?: number | string | null;
  fiveYearAvgDividendYield?: number | string | null;
  beta?: number | string | null;
  fiftyTwoWeekLow?: number | string | null;
  fiftyTwoWeekHigh?: number | string | null;
};

export type StockSnapshot = {
  ticker: string;
  company_name?: string | null;
  region?: string | null;
  market?: string | null;
  country?: string | null;
  sector?: string | null;
  rank?: number | null;
  volume_date?: string | null;
  price_date?: string | null;
  current_price?: number | string | null;
  previous_close?: number | string | null;
  previous_close_date?: string | null;
  close_change?: number | string | null;
  close_change_pct?: number | string | null;
  close_direction?: string | null;
  daily_trading_percentile?: number | string | null;
  weekly_return?: number | string | null;
  dollar_vol_latest?: number | string | null;
  latest_volume?: number | string | null;
  dollar_vol_week?: number | string | null;
  vol_ratio?: number | string | null;
  market_cap?: number | string | null;
  trailing_pe?: number | string | null;
  forward_pe?: number | string | null;
  price_to_book?: number | string | null;
  peg_ratio?: number | string | null;
  dividend_yield?: number | string | null;
  currency?: string | null;
  exchange?: string | null;
  industry?: string | null;
  fundamentals?: StockFundamentals;
  action?: "Buy" | "Hold" | "Sell" | null;
  score?: number | string | null;
  confidence?: number | null;
  rationale?: string | null;
  daily_summary?: string | null;
  decision_snapshot?: DecisionSnapshot;
};

export type LatestReport = {
  update: UpdateInfo | null;
  regions: RegionSnapshot[];
  sectors: SectorSnapshot[];
  top_stocks: StockSnapshot[];
};

export type UserPreferences = {
  user_id?: string;
  theme: "light" | "dark" | "system";
  default_region: string | null;
  default_sector: string | null;
  default_market: string | null;
  visible_columns: string[];
  watchlist: string[];
  notification_settings: Record<string, unknown>;
};

export type MarketRefreshResponse = {
  status: "queued" | "already_running";
  message: string;
  workflow_run_url?: string;
  workflow_run_id?: number;
  baseline_run_id?: number;
};

export type MarketRefreshStatus = {
  status: "waiting" | "queued" | "in_progress" | "success" | "failed";
  progress: number;
  stage: string;
  message: string;
  workflow_run_id?: number;
  workflow_run_url?: string;
  conclusion?: string | null;
};

const defaultPreferences: UserPreferences = {
  theme: "system",
  default_region: null,
  default_sector: null,
  default_market: null,
  visible_columns: [],
  watchlist: [],
  notification_settings: {}
};

const REPORT_REQUEST_TIMEOUT_MS = 8_000;

function getApiUrl() {
  const apiUrl = process.env.FASTAPI_URL;
  if (!apiUrl) {
    return null;
  }

  return apiUrl.replace(/\/$/, "");
}

export async function getLatestReport(): Promise<LatestReport | null> {
  const apiUrl = getApiUrl();
  if (!apiUrl) {
    return null;
  }

  try {
    const response = await fetch(`${apiUrl}/api/latest-report`, {
      cache: "no-store",
      signal: AbortSignal.timeout(REPORT_REQUEST_TIMEOUT_MS)
    });

    if (!response.ok) {
      console.error(`Latest report request returned ${response.status}.`);
      return null;
    }

    return response.json();
  } catch (error) {
    console.error("Latest report request failed.", error);
    return null;
  }
}

export async function getUserPreferences(
  accessToken: string | undefined
): Promise<UserPreferences> {
  if (!accessToken) {
    return defaultPreferences;
  }

  const apiUrl = getApiUrl();
  if (!apiUrl) {
    return defaultPreferences;
  }

  try {
    const response = await fetch(`${apiUrl}/api/me/preferences`, {
      cache: "no-store",
      headers: {
        authorization: `Bearer ${accessToken}`
      }
    });

    if (!response.ok) {
      return defaultPreferences;
    }

    return response.json();
  } catch {
    return defaultPreferences;
  }
}

export async function putUserPreferences(
  accessToken: string,
  preferences: UserPreferences
): Promise<UserPreferences> {
  const apiUrl = getApiUrl();
  if (!apiUrl) {
    throw new Error("FASTAPI_URL is not configured.");
  }

  const response = await fetch(`${apiUrl}/api/me/preferences`, {
    method: "PUT",
    headers: {
      "content-type": "application/json",
      authorization: `Bearer ${accessToken}`
    },
    body: JSON.stringify(preferences)
  });

  if (!response.ok) {
    throw new Error("Preferences could not be saved.");
  }

  return response.json();
}

export async function triggerMarketRefresh(accessToken: string): Promise<MarketRefreshResponse> {
  const apiUrl = getApiUrl();
  if (!apiUrl) {
    throw new Error("The market update service is not configured.");
  }

  const response = await fetch(`${apiUrl}/api/refresh`, {
    method: "POST",
    cache: "no-store",
    headers: {
      authorization: `Bearer ${accessToken}`
    }
  });
  const payload = await response.json().catch(() => ({})) as Partial<MarketRefreshResponse> & { detail?: string };

  if (!response.ok) {
    throw new Error(payload.detail || "The market update could not be started.");
  }

  return {
    status: payload.status || "queued",
    message: payload.message || "The market update has been queued.",
    workflow_run_url: payload.workflow_run_url,
    workflow_run_id: payload.workflow_run_id,
    baseline_run_id: payload.baseline_run_id
  };
}

export async function getMarketRefreshStatus(
  accessToken: string,
  baselineRunId?: number,
  workflowRunId?: number
): Promise<MarketRefreshStatus> {
  const apiUrl = getApiUrl();
  if (!apiUrl) throw new Error("The market update service is not configured.");

  const query = new URLSearchParams();
  if (baselineRunId) query.set("baseline_run_id", baselineRunId.toString());
  if (workflowRunId) query.set("workflow_run_id", workflowRunId.toString());
  const response = await fetch(`${apiUrl}/api/refresh/status?${query}`, {
    cache: "no-store",
    headers: { authorization: `Bearer ${accessToken}` }
  });
  const payload = await response.json().catch(() => ({})) as Partial<MarketRefreshStatus> & { detail?: string };

  if (!response.ok) throw new Error(payload.detail || "Market update progress is unavailable.");
  return {
    status: payload.status || "waiting",
    progress: payload.progress ?? 0,
    stage: payload.stage || "Preparing market update",
    message: payload.message || "Checking market update progress.",
    workflow_run_id: payload.workflow_run_id,
    workflow_run_url: payload.workflow_run_url,
    conclusion: payload.conclusion
  };
}
