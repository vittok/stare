"use client";

import { createClient } from "../lib/supabase/client";

type AuthButtonProps = {
  signedIn: boolean;
};

export function AuthButton({ signedIn }: AuthButtonProps) {
  const supabase = createClient();

  async function signIn() {
    const origin = window.location.origin;
    await supabase.auth.signInWithOAuth({
      provider: "google",
      options: {
        redirectTo: `${origin}/auth/callback`
      }
    });
  }

  async function signOut() {
    await supabase.auth.signOut();
    window.location.reload();
  }

  return (
    <button className="button" onClick={signedIn ? signOut : signIn}>
      {signedIn ? "Sign out" : "Sign in with Google"}
    </button>
  );
}
