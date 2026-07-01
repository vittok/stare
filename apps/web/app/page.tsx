import { AuthButton } from "../components/auth-button";
import { createClient } from "../lib/supabase/server";

async function getLatestReport() {
  const apiUrl = process.env.FASTAPI_URL;
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

export default async function Home() {
  const supabase = await createClient();
  const {
    data: { user }
  } = await supabase.auth.getUser();
  const latestReport = await getLatestReport();
  const update = latestReport?.update;

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

      <section className="shell hero">
        <div className="hero-grid">
          <div>
            <h1>S.T.A.R.E Portal</h1>
            <p className="lede">
              The standalone portal will add Google login, saved preferences,
              historical snapshots, and database-backed trend views while the
              GitHub Pages report remains available as the public demo.
            </p>
            <div className="actions">
              <a className="button secondary" href="/auth/callback?next=/">
                Auth callback ready
              </a>
            </div>
          </div>

          <aside className="panel">
            <h2>Portal Foundation</h2>
            <div className="metric-grid">
              <div className="metric">
                <span className="metric-label">Auth</span>
                <span className="metric-value">{user ? "On" : "Ready"}</span>
              </div>
              <div className="metric">
                <span className="metric-label">Database</span>
                <span className="metric-value">{update ? "Live" : "Ready"}</span>
              </div>
              <div className="metric">
                <span className="metric-label">Latest update</span>
                <span className="metric-value">
                  {update?.latest_price_date ?? "Pending"}
                </span>
              </div>
              <div className="metric">
                <span className="metric-label">Signed in</span>
                <span className="metric-value">{user?.email ?? "No"}</span>
              </div>
            </div>
          </aside>
        </div>
      </section>

      <footer className="footer">
        <div className="shell">Created by vittok. GitHub Pages remains the demo until this portal is production-ready.</div>
      </footer>
    </main>
  );
}
