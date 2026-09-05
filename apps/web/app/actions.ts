"use server";

import { revalidatePath } from "next/cache";
import { createClient } from "../lib/supabase/server";
import {
  type ScoringWeights,
  type UserPreferences,
  type UserWatchlist,
  deleteUserScoringWeights,
  getMarketRefreshStatus,
  getUserPreferences,
  postUserWatchlist,
  putUserScoringWeights,
  putUserWatchlist,
  removeUserWatchlist,
  putUserPreferences,
  triggerMarketRefresh
} from "../lib/portal-api";

async function getAccessToken() {
  const supabase = await createClient();
  const { data: { user } } = await supabase.auth.getUser();
  const { data: { session } } = await supabase.auth.getSession();
  return user && session ? session.access_token : null;
}

export async function savePreferences(
  preferences: UserPreferences
): Promise<{ ok: true; preferences: UserPreferences } | { ok: false; error: string }> {
  const supabase = await createClient();
  const {
    data: { user }
  } = await supabase.auth.getUser();
  const {
    data: { session }
  } = await supabase.auth.getSession();

  if (!user || !session) {
    return { ok: false, error: "Sign in to save preferences." };
  }

  try {
    const current = await getUserPreferences(session.access_token);
    const saved = await putUserPreferences(session.access_token, {
      ...current,
      ...preferences,
      user_id: user.id,
      watchlist: preferences.watchlist.map((ticker) => ticker.toUpperCase())
    });
    revalidatePath("/");
    return { ok: true, preferences: saved };
  } catch (error) {
    return {
      ok: false,
      error: error instanceof Error ? error.message : "Preferences could not be saved."
    };
  }
}

export async function createWatchlist(
  name: string,
  tickers: string[] = []
): Promise<{ ok: true; watchlist: UserWatchlist } | { ok: false; error: string }> {
  const accessToken = await getAccessToken();
  if (!accessToken) return { ok: false, error: "Sign in to create watchlists." };
  try {
    const watchlist = await postUserWatchlist(accessToken, { name, tickers, is_default: true });
    revalidatePath("/");
    return { ok: true, watchlist };
  } catch (error) {
    return { ok: false, error: error instanceof Error ? error.message : "Watchlist could not be created." };
  }
}

export async function saveWatchlist(
  id: string,
  changes: { name?: string; tickers?: string[]; is_default?: boolean }
): Promise<{ ok: true; watchlist: UserWatchlist } | { ok: false; error: string }> {
  const accessToken = await getAccessToken();
  if (!accessToken) return { ok: false, error: "Sign in to update watchlists." };
  try {
    const watchlist = await putUserWatchlist(accessToken, id, changes);
    revalidatePath("/");
    return { ok: true, watchlist };
  } catch (error) {
    return { ok: false, error: error instanceof Error ? error.message : "Watchlist could not be saved." };
  }
}

export async function deleteWatchlist(
  id: string
): Promise<{ ok: true } | { ok: false; error: string }> {
  const accessToken = await getAccessToken();
  if (!accessToken) return { ok: false, error: "Sign in to delete watchlists." };
  try {
    await removeUserWatchlist(accessToken, id);
    revalidatePath("/");
    return { ok: true };
  } catch (error) {
    return { ok: false, error: error instanceof Error ? error.message : "Watchlist could not be deleted." };
  }
}

export async function saveScoringWeights(
  weights: ScoringWeights
): Promise<{ ok: true; weights: ScoringWeights } | { ok: false; error: string }> {
  const accessToken = await getAccessToken();
  if (!accessToken) return { ok: false, error: "Sign in to save scoring weights." };
  try {
    const saved = await putUserScoringWeights(accessToken, weights);
    revalidatePath("/");
    return { ok: true, weights: saved };
  } catch (error) {
    return { ok: false, error: error instanceof Error ? error.message : "Scoring weights could not be saved." };
  }
}

export async function resetScoringWeights(): Promise<
  { ok: true; weights: ScoringWeights } | { ok: false; error: string }
> {
  const accessToken = await getAccessToken();
  if (!accessToken) return { ok: false, error: "Sign in to reset scoring weights." };
  try {
    const weights = await deleteUserScoringWeights(accessToken);
    revalidatePath("/");
    return { ok: true, weights };
  } catch (error) {
    return { ok: false, error: error instanceof Error ? error.message : "Scoring weights could not be reset." };
  }
}

export async function startMarketRefresh(): Promise<
  { ok: true; status: "queued" | "already_running"; message: string; workflowRunId?: number; baselineRunId?: number }
  | { ok: false; error: string }
> {
  const supabase = await createClient();
  const {
    data: { user }
  } = await supabase.auth.getUser();
  const {
    data: { session }
  } = await supabase.auth.getSession();

  if (!user || !session) {
    return { ok: false, error: "Sign in to refresh market data." };
  }

  try {
    const result = await triggerMarketRefresh(session.access_token);
    return { ok: true, status: result.status, message: result.message, workflowRunId: result.workflow_run_id, baselineRunId: result.baseline_run_id };
  } catch (error) {
    return {
      ok: false,
      error: error instanceof Error ? error.message : "The market update could not be started."
    };
  }
}

export async function getMarketRefreshProgress(
  baselineRunId?: number,
  workflowRunId?: number
) {
  const supabase = await createClient();
  const { data: { user } } = await supabase.auth.getUser();
  const { data: { session } } = await supabase.auth.getSession();
  if (!user || !session) return { ok: false as const, error: "Sign in to view update progress." };

  try {
    const progress = await getMarketRefreshStatus(session.access_token, baselineRunId, workflowRunId);
    return { ok: true as const, progress };
  } catch (error) {
    return {
      ok: false as const,
      error: error instanceof Error ? error.message : "Market update progress is unavailable."
    };
  }
}
