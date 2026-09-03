# S.T.A.R.E Standalone Portal Tasks

Goal: turn S.T.A.R.E from a static GitHub Pages report into a standalone authenticated web portal with user preferences, historical daily data, and room for future personalization.

Chosen stack: Next.js frontend, FastAPI service layer, and Supabase Postgres database in the `vittok` Supabase organization on the free tier.

Supabase project:

- Project name: STARE
- Project ref: `bprknqcgtezsgfjuztqs`
- Project URL: `https://bprknqcgtezsgfjuztqs.supabase.co`
- Publishable key received for frontend configuration.

## Phase 1 - Architecture Decisions

- [x] Confirm target stack: Supabase for auth/database and Render for app hosting plus scheduled jobs.
- [x] Confirm frontend framework: Next.js.
- [x] Confirm backend style: FastAPI service.
- [x] Confirm database: Supabase Postgres.
- [x] Define production domain and DNS plan: start with hosted UAT URL, then attach a custom `stare.<domain>` CNAME after acceptance.
- [ ] Select the final custom production domain.
- [x] Defer domain purchase until Render UAT is accepted.
- [x] Decide whether GitHub Pages remains as a public demo or is retired: keep GitHub Pages as public demo/fallback.

## Phase 2 - Supabase Setup

- [x] Create/use Supabase organization: `vittok`.
- [x] Select Supabase free tier for initial build.
- [x] Create Supabase project: STARE.
- [x] Create Google Cloud OAuth client for STARE.
- [x] Add authorized JavaScript origins in Google OAuth client:
  - [x] `http://localhost:3000`
  - [x] Render UAT portal URL: `https://stare-portal.onrender.com`
  - [ ] production custom portal URL when selected
- [x] Add authorized redirect URI in Google OAuth client:
  - [x] `https://bprknqcgtezsgfjuztqs.supabase.co/auth/v1/callback`
- [x] Copy Google OAuth Client ID and Client Secret into Supabase Auth provider settings.
- [x] Enable Google authentication provider in Supabase.
- [x] Configure Supabase Auth redirect URLs:
  - [x] `http://localhost:3000/auth/callback`
  - [x] Render UAT callback URL: `https://stare-portal.onrender.com/auth/callback`
  - [x] Confirm Supabase Site URL: `https://stare-portal.onrender.com`
  - [ ] production custom portal callback URL when selected
- [x] Create database roles and row-level security policy approach: user tables use Supabase Auth RLS; market data is backend-served through FastAPI.
- [x] Store Supabase URL and publishable key in `.env.example`.
- [x] Add backend-only `DATABASE_URL` locally in ignored `.env`.
- [x] Add backend-only `DATABASE_URL` in deployment secrets.
- [x] Add `CORS_ORIGINS=https://stare-portal.onrender.com` to API deployment secrets.
- [x] Confirm direct `DATABASE_URL` format. Standard format is `postgresql://postgres:<password>@db.<project-ref>.supabase.co:5432/postgres`; encode special password characters if needed.
- [x] Replace local/deployment `DATABASE_URL` with Supabase Session Pooler connection string if the host cannot reach the direct IPv6 database endpoint.
- [ ] Rotate the initial database password before production launch because it was shared during setup.
- [ ] Rotate the Google OAuth client secret before production launch because it was shared during setup.
- [x] Document standalone deployment, auth, DNS, and secret handling in `PORTAL_DEPLOYMENT.md`.
- [x] Add Render blueprint for UAT services in `render.yaml`.

## Phase 3 - Database Schema

- [x] Create initial migration file: `supabase/migrations/001_initial_portal_schema.sql`.
- [x] Apply initial migration to Supabase STARE project.
- [x] Create `update_runs` table for each market update.
- [x] Create `sector_snapshots` table for sector-level history.
- [x] Create `region_snapshots` table for NA/APAC/EMEA/LAC history.
- [x] Create `stock_snapshots` table for per-ticker daily metrics.
- [x] Create `stock_recommendations` table for buy/hold/sell outputs and rationale.
- [x] Create `user_profiles` table linked to authenticated users.
- [x] Create `user_preferences` table for saved filters, default regions/sectors, theme, and watchlists.
- [x] Add indexes for date, ticker, sector, region, and user lookups.
- [ ] Add retention strategy for raw data if storage grows too much.

## Phase 4 - Pipeline Migration

- [ ] Refactor current pipeline so outputs can be written to Postgres instead of only JSON/CSV/HTML.
- [x] Add Supabase/Postgres connection configuration.
- [x] Add first artifact importer: `src/export_reports_to_postgres.py`.
- [x] Write each imported update into `update_runs`.
- [x] Persist sector, region, and stock snapshots from existing JSON artifacts.
- [x] Persist recommendation snapshots from the same code path that generates buy/hold/sell signals.
- [x] Verify a Supabase import with populated `stock_recommendations` after remote execution is available.
- [ ] Keep JSON export generation as a fallback/debug artifact.
- [ ] Add validation checks before marking an update as successful.
- [ ] Add failure logging for partial data pulls.
- [x] Run first Supabase import after replacing direct DB URL with Session Pooler URL.

## Phase 5 - Standalone Web App

- [x] Create new app shell outside the static single-file HTML model.
- [x] Add Next.js frontend scaffold under `apps/web`.
- [x] Add FastAPI service scaffold under `apps/api`.
- [x] Add API health endpoint.
- [x] Add first database-backed latest report endpoint.
- [x] Add first user preference read/write endpoints.
- [x] Implement Google login/logout shell.
- [x] Add local Supabase auth callback route.
- [x] Protect personalized API operations with validated Supabase access tokens;
  keep the market dashboard publicly readable during UAT.
- [x] Rebuild the complete static dashboard experience from database queries.
- [x] Add All Regions, NA, NA/Sectors, LAC, EMEA, and APAC navigation.
- [x] Add direction, search, sector, country/market, and watchlist filters.
- [x] Add heatmap, strength chart, KPIs, top picks, sortable stock table,
  fundamentals, decision snapshots, explanations, print, and update metadata.
- [x] Add saved user preferences for theme, default region, default sector,
  default market, visible columns, and watchlist.
- [x] Add saved watchlist support.
- [x] Add responsive layout matching current minimal sharp-corner design.
- [x] Add owner-restricted manual market refresh with automatic report reload.
- [x] Add a complete stock-information dialog from ticker symbols.

## Phase 6 - Historical Views

- [ ] Add sector strength trend chart over time.
- [ ] Add region strength trend chart over time.
- [ ] Add ticker recommendation history.
- [ ] Add price, previous close, weekly return, daily trading percentile, and volume trend charts.
- [ ] Add compare mode for sectors, regions, or tickers.
- [ ] Add export options for CSV or JSON.

## Phase 7 - Scheduled Updates

- [x] Create Render Blueprint from `render.yaml`.
- [x] Deploy `stare-api` UAT service: `https://stare-api.onrender.com`.
- [x] Deploy `stare-portal` UAT service: `https://stare-portal.onrender.com`.
- [x] Add Render UAT portal URL to Google OAuth authorized origins.
- [x] Add Render UAT callback URL to Supabase Auth redirect URLs.
- [x] Import each GitHub-scheduled UAT update into Supabase as a temporary bridge.
- [x] Allow an authorized portal user to trigger the existing update workflow as a temporary bridge.
- [ ] Create Render scheduled job for market open refresh.
- [ ] Create Render scheduled job for market close refresh.
- [ ] Move SMTP notification into the standalone job flow.
- [ ] Include update status and top changes in email body.
- [ ] Add alerting when an update fails or data is stale.
- [ ] Confirm schedules handle US market daylight saving time.

## Phase 8 - Security and Operations

- [x] Store current deployment secrets in Render, Supabase, or GitHub secret managers.
- [x] Ensure service-role Supabase key is never exposed to the browser.
- [x] Enable row-level security for user preference and market snapshot tables.
- [ ] Add basic request logging.
- [ ] Add database backup/export plan.
- [ ] Add monitoring for update duration and data freshness.
- [ ] Add privacy note for user profile/preferences data.

## Phase 9 - Migration and Cutover

- [ ] Import recent historical JSON/CSV artifacts into Supabase.
- [x] Validate latest database values and row coverage against the static HTML output.
- [x] Run standalone portal in parallel with GitHub Pages.
- [x] Test Google login with at least one real user account.
- [x] Test saved preferences across browser sessions.
- [ ] Test scheduled update writes and historical charts.
- [ ] Switch public URL to standalone app.
- [x] Keep GitHub Pages static app as fallback until standalone portal is stable.

## Open Questions

- [x] Keep the dashboard publicly readable during UAT; require sign-in for saved personalization.
- [ ] Should users be able to create custom watchlists only, or also custom scoring weights?
- [ ] Should historical data be stored for every tracked ticker or only displayed top picks?
- [ ] How long should daily snapshots be retained?
- [ ] Should email reports become per-user configurable?
- [ ] Should future recommendations include user risk profile and investment horizon?
