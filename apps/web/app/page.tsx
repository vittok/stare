import { LoginExperience } from "../components/login-experience";
import { PortalDashboard } from "../components/portal-dashboard";
import { AuthButton } from "../components/auth-button";
import { PortalHelp } from "../components/portal-help";
import { getLatestReport, getUserPersonalizedSignals, getUserPreferences, getUserScoringWeights, getUserWatchlists } from "../lib/portal-api";
import { createClient } from "../lib/supabase/server";
import Image from "next/image";

export default async function Home() {
  const supabase = await createClient();
  const {
    data: { user }
  } = await supabase.auth.getUser();
  const {
    data: { session }
  } = await supabase.auth.getSession();
  const [latestReport, preferences, watchlists, scoringWeights, personalized] = await Promise.all([
    getLatestReport(),
    getUserPreferences(session?.access_token),
    getUserWatchlists(session?.access_token),
    getUserScoringWeights(session?.access_token),
    getUserPersonalizedSignals(session?.access_token)
  ]);
  const signedIn = Boolean(user);
  const userIdentity = user ? {
    displayName: typeof user.user_metadata.full_name === "string"
      ? user.user_metadata.full_name
      : user.email?.split("@")[0] || "User",
    email: user.email || ""
  } : null;

  return (
    <LoginExperience
      marketDataDate={latestReport?.update?.latest_price_date || latestReport?.update?.market_data_date}
      portalUpdated={latestReport?.update?.completed_at}
      signedIn={signedIn}
    >
      <main className="page">
        <header className="topbar">
          <div className="topbar-inner">
            <div className="brand">
              <Image alt="S.T.A.R.E logo" className="brand-logo" height={104} priority src="/Logo.png" width={104} />
              <span>Stock Trend Analysis Risk Engine</span>
            </div>
            {userIdentity ? <div className="topbar-actions"><section className="topbar-account" aria-label="Signed-in account"><div className="topbar-account-copy"><span>Signed in</span><strong title={userIdentity.displayName}>{userIdentity.displayName}</strong><small title={userIdentity.email}>{userIdentity.email}</small></div></section><AuthButton className="button secondary topbar-signout" label="Sign out" signedIn /><PortalHelp marketDataDate={latestReport?.update?.latest_price_date || latestReport?.update?.market_data_date} portalUpdated={latestReport?.update?.completed_at} signedIn /></div> : null}
          </div>
        </header>

        <div className="app-shell">
          <PortalDashboard
            preferences={preferences}
            personalizedSignals={personalized.update_run_id === latestReport?.update?.id ? personalized.signals : []}
            report={latestReport}
            signedIn={signedIn}
            scoringWeights={scoringWeights}
            user={userIdentity}
            watchlists={watchlists}
          />
        </div>

      </main>
    </LoginExperience>
  );
}
