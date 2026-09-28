# S.T.A.R.E Standalone Portal Deployment Notes

This document tracks the production setup decisions for the standalone S.T.A.R.E portal. GitHub Pages remains the public demo/fallback.

## Recommended Phase 1 Domain Plan

Use a two-step domain rollout:

1. **UAT / first hosted build:** use the hosting provider's generated HTTPS URL.
   - Example shape: `https://stare-portal-<suffix>.onrender.com`
   - This avoids buying or changing DNS before the portal is usable.

2. **Production:** attach a custom subdomain after UAT is accepted.
   - Recommended shape: `https://stare.<your-domain>`
   - DNS: create a `CNAME` record from `stare.<your-domain>` to the host-provided target.
   - Keep `https://vittok.github.io/stare/` as the public static demo.

After the production URL is known, update:

- Google OAuth authorized JavaScript origins
- Supabase Auth redirect URLs
- Render environment variables
- Any user-facing links in documentation or notification templates

## Render UAT Deployment

The repository includes a Render blueprint in `render.yaml` with two free-tier services:

- `stare-api` - FastAPI backend from `apps/api`
- `stare-portal` - Next.js frontend from `apps/web`

Current UAT services:

- API: `https://stare-api.onrender.com`
- Portal: `https://stare-portal.onrender.com`

Create the first UAT deployment from Render's dashboard:

1. Connect Render to the `vittok/stare` GitHub repository.
2. Create a new Blueprint from `render.yaml`.
3. Add the backend-only `DATABASE_URL` secret to `stare-api`.
4. Deploy `stare-api` first and copy its generated HTTPS URL.
5. Set `FASTAPI_URL` on `stare-portal` to `https://stare-api.onrender.com`.
6. Deploy `stare-portal` and copy its generated HTTPS URL.
7. Set `NEXT_PUBLIC_APP_URL` on `stare-portal` to `https://stare-portal.onrender.com`.
8. Set `CORS_ORIGINS` on `stare-api` to `https://stare-portal.onrender.com`.
9. Set `SUPABASE_URL` and `SUPABASE_PUBLISHABLE_KEY` on `stare-api` so it can
   validate user access tokens.
10. Set `GITHUB_ACTIONS_TOKEN` on `stare-api` to a fine-grained GitHub token
    scoped to `vittok/stare` with **Actions: Read and write** permission.
11. Set `REFRESH_ALLOWED_EMAILS` on `stare-api` to the comma-separated Google
    account emails allowed to start manual market updates.
12. Redeploy both services after environment variables are finalized.

Render UAT URLs will look similar to:

```text
https://stare-api.onrender.com
https://stare-portal.onrender.com
```

Use the actual generated URLs from Render; they may include suffixes.

## Supabase Auth Configuration

Already configured for local development:

- Google OAuth provider is enabled.
- Supabase callback URI is configured in Google OAuth:
  - `https://bprknqcgtezsgfjuztqs.supabase.co/auth/v1/callback`
- Local app callback is configured:
  - `http://localhost:3000/auth/callback`

Add after hosting URL is selected:

- Google OAuth authorized JavaScript origin:
  - `https://stare-portal.onrender.com`
- Supabase Auth redirect URL:
  - `https://stare-portal.onrender.com/auth/callback`

When a final custom domain is attached later, add that custom origin and callback too.

## Supabase Database Access

Use the Supabase Transaction Pooler connection string for backend services:

```text
postgresql://postgres.bprknqcgtezsgfjuztqs:[YOUR-PASSWORD]@aws-0-eu-west-1.pooler.supabase.com:6543/postgres
```

Replace `[YOUR-PASSWORD]` only in Render or GitHub Actions secret values. The password must be
URL-encoded if it contains reserved URL characters such as `/`.

Store this value only as:

- local `.env`
- deployment secret/environment variable

Never expose it to the browser or commit it to the repository.

## RLS and Access Model

Current approach:

- Next.js uses Supabase Auth for Google sign-in.
- Next.js forwards the signed Supabase access token only from server-side code
  when reading or saving preferences.
- FastAPI validates the token with Supabase Auth and derives the user ID from
  the validated response; it does not trust a caller-provided user ID.
- FastAPI reads market data from Postgres through backend-only credentials.
- Browser code never receives `DATABASE_URL` or service-role credentials.
- `user_profiles` and `user_preferences` have Row Level Security enabled.
- Users can select, insert, and update only rows where `auth.uid() = user_id`.

Market snapshot tables are currently accessed through FastAPI rather than direct browser queries. If direct Supabase client reads are added later, add explicit read policies before exposing those tables.

## Required Deployment Secrets

For the FastAPI service:

- `DATABASE_URL`
- `CORS_ORIGINS`
- `SUPABASE_URL`
- `SUPABASE_PUBLISHABLE_KEY`
- `GITHUB_ACTIONS_TOKEN` (backend-only fine-grained token)
- `GITHUB_REPOSITORY=vittok/stare`
- `GITHUB_WORKFLOW=pipeline_weekdays.yml`
- `REFRESH_ALLOWED_EMAILS` (comma-separated update administrators)

For the Next.js frontend:

- `NEXT_PUBLIC_SUPABASE_URL`
- `NEXT_PUBLIC_SUPABASE_PUBLISHABLE_KEY`
- `NEXT_PUBLIC_APP_URL`
- `FASTAPI_URL`

## GitHub Actions Scheduled Updates

Render hosts only the Next.js portal and FastAPI service. The workflow
`.github/workflows/pipeline_weekdays.yml` owns scheduled and manual updates:
data pulls, calculations, required Supabase Postgres persistence, static
artifacts, Pages deployment, and Brevo email.

Targets are 09:35 New York for open, 16:10 for regular close, and 13:10 for
early close. Paired UTC schedules cover US daylight saving time.
`src/github_market_schedule.py` uses the NYSE calendar to skip holidays and
inactive occurrences. It matches the triggering cron expression, so delayed
jobs are accepted within the same New York session date. Jobs delayed into a
different date are not backfilled. Calendar failures stop the update.
Fundamentals refresh at the first session's close each week.
Manual dispatch bypasses the calendar guard.

Store these in **GitHub repository > Settings > Secrets and variables > Actions**:

- `DATABASE_URL`: full Supabase Postgres pooler URL, including the URL-encoded password.
- `SMTP_USERNAME`: Brevo SMTP login.
- `SMTP_PASSWORD`: Brevo SMTP key.
- `SMTP_FROM`: a sender verified in Brevo, not necessarily the SMTP login.

Keep `DATABASE_URL` in Render's API environment as well, since the API reads
the snapshots. No database or SMTP passwords belong in `render.yaml`.

### Cutover from Render Cron

1. Suspend `stare-market-open` and `stare-market-close` in the Render
   dashboard if they exist. Removing their definitions from the Blueprint
   does not stop existing services.
2. Sync the updated Blueprint, which contains only the two web services, then
   delete the now-unmanaged cron services if they are no longer needed.
3. Publish the updated workflow on `main`.
4. Dispatch **STARE Market Refresh** once and confirm every step succeeds.
5. The final verification step checks the exact Postgres update ID through
   FastAPI and the portal page, both published Pages JSON reports, and the published HTML.
6. Confirm receipt of the email and open the authenticated portal to check the
   displayed update. SMTP acceptance does not prove inbox delivery.

The workflow captures the previous embedded report in the runner's temporary
directory before calculation. After publication verification, the email receives
`STARE_UPDATE_STATUS=success` and `STARE_PREVIOUS_APP_HTML` pointing to that
baseline. HTML and plain-text bodies show status and the largest changes since
the previous update. Missing history is reported explicitly. These are successful
update notifications, not failure alerts; no new secrets are required.

GitHub schedules are best-effort and may be delayed. Free Render services may
sleep, but Actions writes directly to Supabase and sends directly through Brevo,
so data collection does not depend on Render being awake. The publication check
retries to allow for API cold starts and Pages propagation. Calendar data should
be updated when the exchange announces new holidays or exceptional closures.

When the Render portal process starts, its Next.js instrumentation hook sends a
background request to the FastAPI `/health` endpoint. This wakes the API at the
same time as the portal without delaying the portal startup. The existing report
request retries remain in place while the free API instance finishes its cold
start.

### Verification Record

On 2026-09-27, [Actions update 36332716974](https://github.com/vittok/stare/actions/runs/36332716974)
completed data pulls, Postgres persistence, Pages deployment, and SMTP submission.
The API and portal page both returned snapshot
`4304e187-9fc2-4347-873b-d18b4ce495cb`, completed at 16:20:12 UTC with market data
dated 2026-09-25, 11 sectors, and 182 stocks. Published Pages JSON and HTML matched
the generated files. DST/holiday/early-close behavior is unit-tested; this live
verification used manual dispatch. On 2026-09-27, the account owner confirmed
there are no Render cron services, so none need disabling. Inbox receipt remains
an account-owner check.

## Historical Backfill Verification

On 2026-09-27, `src/backfill_historical_reports.py --apply` filled 13 missing
market dates from September 8 through September 24. It used the final Git JSON
report pair for each date and left six already-covered dates unchanged.
Verified counts: 143 sector snapshots, 39 region snapshots, 2,366 stock rows,
and 2,366 reconstructed recommendations. Prices, previous closes, weekly returns,
daily percentiles, and volumes were checked against all source stock rows.
Original report commit timestamps were preserved. A repeat import skipped all
19 candidate dates without writing new snapshots. The latest live snapshot
remained `4304e187-9fc2-4347-873b-d18b4ce495cb`.
The live sector, region, and ticker history endpoints each returned all 13
restored dates in checks for Information Technology, APAC, and NVDA.

One initial attempt hit a transaction-pooler prepared-statement conflict and
rolled back all snapshot rows for that attempt. The failed audit entry is
retained. Automatic prepared statements are now disabled in the Postgres writer;
the resumed import completed successfully. No schema changes were required.

## Manual Portal Refresh

Authenticated update administrators can select **Refresh data** in the portal.
The request is sent through the Next.js server to FastAPI, which validates the
Supabase user and checks `REFRESH_ALLOWED_EMAILS`. FastAPI then starts the same
`pipeline_weekdays.yml` workflow used for scheduled updates. This ensures a
manual refresh uses the established price pull, calculations, recommendation
generation, Postgres persistence, static fallback publishing, and notification
flow.

The API checks for a queued or active workflow before dispatching another one.
After a request is accepted, the portal checks for a newly completed database
snapshot and replaces the visible report automatically. The GitHub token is
never sent to the browser.

Create the token in GitHub under **Settings > Developer settings > Personal
access tokens > Fine-grained tokens**. Limit repository access to `vittok/stare`
and grant **Actions: Read and write**. Store the token only in the Render
`stare-api` environment as `GITHUB_ACTIONS_TOKEN`.

## Secret Rotation Before Production

Rotate before public production launch because setup values were shared during development:

- Supabase database password
- Google OAuth client secret

After rotation, update:

- local `.env`
- deployment secrets
- Supabase/Google provider configuration as needed
