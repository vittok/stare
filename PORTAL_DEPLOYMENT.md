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

Use the Supabase Session Pooler connection string for backend services:

```text
postgresql://postgres.<project-ref>:<url-encoded-password>@<region>.pooler.supabase.com:6543/postgres
```

The password must be URL-encoded if it contains reserved URL characters such as `/`.

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

Future backend-only secrets may include:

- `SUPABASE_SERVICE_ROLE_KEY`
- SMTP credentials if notifications move from GitHub Actions to the standalone update job

The shared market update can write its final calculated report to Supabase after
generating the static artifacts. Render requires this Postgres output for its
scheduled updates. Manually dispatched GitHub updates also require it so the
portal's temporary manual-update bridge remains functional.

## Render Scheduled Updates

The Blueprint defines two standalone cron services:

- `stare-market-open` targets 09:35 America/New_York on weekdays.
- `stare-market-close` targets 16:10 America/New_York on weekdays and refreshes
  fundamentals on Monday closes.

Render cron expressions use UTC. Each service therefore lists both the daylight
saving and standard-time UTC hour. `src/run_render_scheduled_update.py` checks
the current New York time and exits successfully for the inactive occurrence.
This keeps the jobs aligned with the US market when clocks change.

After syncing an existing Blueprint, manually add these secrets to both cron
services because Render does not populate new `sync: false` variables on an
existing Blueprint:

- `DATABASE_URL`
- `SMTP_USERNAME`
- `SMTP_PASSWORD`
- `SMTP_FROM`

The remaining SMTP, recipient, and portal URL settings come from `render.yaml`.
Use each job's **Trigger Run** control only during its active market window. For
an out-of-window test, temporarily append `--force` to its start command and
restore the version-controlled command after the test.

GitHub Actions continues producing and publishing the static GitHub Pages demo
on its existing schedule, but scheduled GitHub updates no longer write duplicate
portal snapshots or send duplicate email. Manually dispatched GitHub updates
retain both behaviors for the portal's temporary manual-update bridge.

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
