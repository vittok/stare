import { AuthButton } from "../components/auth-button";
import { PortalDashboard } from "../components/portal-dashboard";
import { getLatestReport, getUserPreferences } from "../lib/portal-api";
import { createClient } from "../lib/supabase/server";

export default async function Home() {
  const supabase = await createClient();
  const {
    data: { user }
  } = await supabase.auth.getUser();
  const latestReport = await getLatestReport();
  const preferences = await getUserPreferences(user?.id);

  return (
    <main className="page">
      <header className="topbar">
        <div className="shell topbar-inner">
          <div className="brand">
            <div className="brand-mark">S.T.</div>
            <span>Sector & Stock Trend Analysis Engine</span>
          </div>
          <AuthButton signedIn={Boolean(user)} />
        </div>
      </header>

      <div className="shell app-shell">
        <PortalDashboard
          preferences={preferences}
          report={latestReport}
          signedIn={Boolean(user)}
        />
      </div>

      <footer className="footer">
        <div className="shell">
          Created by vittok. GitHub Pages remains the demo until this portal is
          production-ready.
        </div>
      </footer>
    </main>
  );
}
