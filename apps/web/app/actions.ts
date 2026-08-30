"use server";

import { revalidatePath } from "next/cache";
import { createClient } from "../lib/supabase/server";
import {
  type UserPreferences,
  getUserPreferences,
  putUserPreferences
} from "../lib/portal-api";

export async function savePreferences(
  preferences: UserPreferences
): Promise<{ ok: true; preferences: UserPreferences } | { ok: false; error: string }> {
  const supabase = await createClient();
  const {
    data: { user }
  } = await supabase.auth.getUser();

  if (!user) {
    return { ok: false, error: "Sign in to save preferences." };
  }

  try {
    const current = await getUserPreferences(user.id);
    const saved = await putUserPreferences(user.id, {
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
