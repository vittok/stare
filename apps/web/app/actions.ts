"use server";

import { revalidatePath } from "next/cache";
import { createClient } from "../lib/supabase/server";
import {
  type UserPreferences,
  getUserPreferences,
  putUserPreferences,
  triggerMarketRefresh
} from "../lib/portal-api";

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

export async function startMarketRefresh(): Promise<
  { ok: true; status: "queued" | "already_running"; message: string }
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
    return { ok: true, status: result.status, message: result.message };
  } catch (error) {
    return {
      ok: false,
      error: error instanceof Error ? error.message : "The market update could not be started."
    };
  }
}
