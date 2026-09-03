import { AuthButton } from "../components/auth-button";
import { LoginExperience } from "../components/login-experience";
import { PortalDashboard } from "../components/portal-dashboard";
import { getLatestReport, getUserPreferences } from "../lib/portal-api";
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
  const latestReport = await getLatestReport();
  const preferences = await getUserPreferences(session?.access_token);
  const signedIn = Boolean(user);

  return (
    <LoginExperience signedIn={signedIn}>
      <main className="page">
        <header className="topbar">
          <div className="topbar-inner">
            <div className="brand">
              <Image alt="S.T.A.R.E logo" className="brand-logo" height={52} priority src="/Logo.png" width={52} />
              <span>Sector & Stock Trend Analysis Engine <b>(S.T.A.R.E)</b></span>
            </div>
            {signedIn ? <AuthButton signedIn /> : null}
          </div>
        </header>

        <div className="app-shell">
          <PortalDashboard
            preferences={preferences}
            report={latestReport}
            signedIn={signedIn}
          />
        </div>

      </main>
    </LoginExperience>
  );
}
