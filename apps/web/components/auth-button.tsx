"use client";

import { createClient } from "../lib/supabase/client";

type AuthButtonProps = {
  className?: string;
  label?: string;
  signedIn: boolean;
};

export function AuthButton({ className = "button", label, signedIn }: AuthButtonProps) {
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
    <button className={className} onClick={signedIn ? signOut : signIn} type="button">
      {label || (signedIn ? "Sign out" : "Sign in with Google")}
    </button>
  );
}
