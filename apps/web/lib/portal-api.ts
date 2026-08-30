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
};

export type SectorSnapshot = {
  sector: string;
  week_ending?: string | null;
  direction?: string | null;
  strength?: number | null;
  raw_score?: number | string | null;
};

export type StockSnapshot = {
  ticker: string;
  company_name?: string | null;
  region?: string | null;
  market?: string | null;
  country?: string | null;
  sector?: string | null;
  rank?: number | null;
  current_price?: number | string | null;
  previous_close?: number | string | null;
  close_change_pct?: number | string | null;
  daily_trading_percentile?: number | string | null;
  weekly_return?: number | string | null;
  action?: "Buy" | "Hold" | "Sell" | null;
  score?: number | string | null;
  confidence?: number | null;
  rationale?: string | null;
  daily_summary?: string | null;
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

const defaultPreferences: UserPreferences = {
  theme: "system",
  default_region: null,
  default_sector: null,
  default_market: null,
  visible_columns: [],
  watchlist: [],
  notification_settings: {}
};

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
      cache: "no-store"
    });

    if (!response.ok) {
      return null;
    }

    return response.json();
  } catch {
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
