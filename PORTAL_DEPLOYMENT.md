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

## Supabase Auth Configuration

Already configured for local development:

- Google OAuth provider is enabled.
- Supabase callback URI is configured in Google OAuth:
  - `https://bprknqcgtezsgfjuztqs.supabase.co/auth/v1/callback`
- Local app callback is configured:
  - `http://localhost:3000/auth/callback`

Add after hosting URL is selected:

- Google OAuth authorized JavaScript origin:
  - `https://<production-portal-host>`
- Supabase Auth redirect URL:
  - `https://<production-portal-host>/auth/callback`

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
- FastAPI reads market data from Postgres through backend-only credentials.
- Browser code never receives `DATABASE_URL` or service-role credentials.
- `user_profiles` and `user_preferences` have Row Level Security enabled.
- Users can select, insert, and update only rows where `auth.uid() = user_id`.

Market snapshot tables are currently accessed through FastAPI rather than direct browser queries. If direct Supabase client reads are added later, add explicit read policies before exposing those tables.

## Required Deployment Secrets

For the FastAPI service:

- `DATABASE_URL`

For the Next.js frontend:

- `NEXT_PUBLIC_SUPABASE_URL`
- `NEXT_PUBLIC_SUPABASE_PUBLISHABLE_KEY`
- `NEXT_PUBLIC_APP_URL`
- `FASTAPI_URL`

Future backend-only secrets may include:

- `SUPABASE_SERVICE_ROLE_KEY`
- SMTP credentials if notifications move from GitHub Actions to the standalone update job

## Secret Rotation Before Production

Rotate before public production launch because setup values were shared during development:

- Supabase database password
- Google OAuth client secret

After rotation, update:

- local `.env`
- deployment secrets
- Supabase/Google provider configuration as needed
