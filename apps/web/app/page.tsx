import { LoginExperience } from "../components/login-experience";
import { PortalDashboard } from "../components/portal-dashboard";
import { getLatestReport, getUserPreferences, getUserScoringWeights, getUserWatchlists } from "../lib/portal-api";
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
  const [latestReport, preferences, watchlists, scoringWeights] = await Promise.all([
    getLatestReport(),
    getUserPreferences(session?.access_token),
    getUserWatchlists(session?.access_token),
    getUserScoringWeights(session?.access_token)
  ]);
  const signedIn = Boolean(user);
  const userIdentity = user ? {
    displayName: typeof user.user_metadata.full_name === "string"
      ? user.user_metadata.full_name
      : user.email?.split("@")[0] || "User",
    email: user.email || ""
  } : null;

  return (
    <LoginExperience signedIn={signedIn}>
      <main className="page">
        <header className="topbar">
          <div className="topbar-inner">
            <div className="brand">
              <Image alt="S.T.A.R.E logo" className="brand-logo" height={52} priority src="/Logo.png" width={52} />
              <span>Stock Trend Analysis Risk Engine <b>(S.T.A.R.E)</b></span>
            </div>
          </div>
        </header>

        <div className="app-shell">
          <PortalDashboard
            preferences={preferences}
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
