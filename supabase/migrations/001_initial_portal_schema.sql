-- S.T.A.R.E standalone portal initial schema.
-- Apply this in Supabase SQL Editor or through a migration runner connected
-- with DATABASE_URL.

create extension if not exists pgcrypto;

create or replace function public.set_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

create table if not exists public.update_runs (
  id uuid primary key default gen_random_uuid(),
  run_label text,
  triggered_by text,
  status text not null default 'started'
    check (status in ('started', 'success', 'partial', 'failed')),
  started_at timestamptz not null default now(),
  completed_at timestamptz,
  market_data_date date,
  latest_price_date date,
  source_commit text,
  diagnostics jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create table if not exists public.sector_snapshots (
  id uuid primary key default gen_random_uuid(),
  update_run_id uuid not null references public.update_runs(id) on delete cascade,
  sector text not null,
  week_ending date,
  direction text check (direction in ('Bullish', 'Bearish', 'Neutral')),
  strength integer check (strength between 0 and 100),
  raw_score numeric,
  diagnostics jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  unique (update_run_id, sector)
);

create table if not exists public.region_snapshots (
  id uuid primary key default gen_random_uuid(),
  update_run_id uuid not null references public.update_runs(id) on delete cascade,
  region text not null,
  week_ending date,
  direction text check (direction in ('Bullish', 'Bearish', 'Neutral')),
  strength integer check (strength between 0 and 100),
  raw_score numeric,
  diagnostics jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  unique (update_run_id, region)
);

create table if not exists public.stock_snapshots (
  id uuid primary key default gen_random_uuid(),
  update_run_id uuid not null references public.update_runs(id) on delete cascade,
  ticker text not null,
  company_name text,
  sector text,
  region text,
  market text,
  country text,
  rank integer,
  volume_date date,
  price_date date,
  current_price numeric,
  previous_close numeric,
  previous_close_date date,
  close_change numeric,
  close_change_pct numeric,
  close_direction text,
  weekly_return numeric,
  dollar_vol_latest numeric,
  latest_volume numeric,
  dollar_vol_week numeric,
  vol_ratio numeric,
  daily_trading_percentile numeric,
  market_cap numeric,
  trailing_pe numeric,
  forward_pe numeric,
  price_to_book numeric,
  peg_ratio numeric,
  dividend_yield numeric,
  currency text,
  exchange text,
  industry text,
  fundamentals jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create table if not exists public.stock_recommendations (
  id uuid primary key default gen_random_uuid(),
  update_run_id uuid not null references public.update_runs(id) on delete cascade,
  ticker text not null,
  action text not null check (action in ('Buy', 'Hold', 'Sell')),
  score numeric,
  confidence integer check (confidence between 0 and 100),
  rationale text,
  decision_snapshot jsonb not null default '{}'::jsonb,
  daily_summary text,
  created_at timestamptz not null default now(),
  unique (update_run_id, ticker)
);

create table if not exists public.user_profiles (
  user_id uuid primary key references auth.users(id) on delete cascade,
  display_name text,
  avatar_url text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.user_preferences (
  user_id uuid primary key references auth.users(id) on delete cascade,
  theme text not null default 'light' check (theme in ('light', 'dark', 'system')),
  default_region text,
  default_sector text,
  default_market text,
  visible_columns text[] not null default array[]::text[],
  watchlist text[] not null default array[]::text[],
  notification_settings jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create trigger user_profiles_set_updated_at
before update on public.user_profiles
for each row execute function public.set_updated_at();

create trigger user_preferences_set_updated_at
before update on public.user_preferences
for each row execute function public.set_updated_at();

create index if not exists update_runs_latest_price_date_idx
  on public.update_runs(latest_price_date desc);

create index if not exists sector_snapshots_sector_date_idx
  on public.sector_snapshots(sector, week_ending desc);

create index if not exists region_snapshots_region_date_idx
  on public.region_snapshots(region, week_ending desc);

create index if not exists stock_snapshots_ticker_price_date_idx
  on public.stock_snapshots(ticker, price_date desc);

create index if not exists stock_snapshots_region_market_idx
  on public.stock_snapshots(region, market, price_date desc);

create index if not exists stock_recommendations_ticker_idx
  on public.stock_recommendations(ticker, created_at desc);

alter table public.user_profiles enable row level security;
alter table public.user_preferences enable row level security;

create policy "Users can read their own profile"
on public.user_profiles
for select
using (auth.uid() = user_id);

create policy "Users can insert their own profile"
on public.user_profiles
for insert
with check (auth.uid() = user_id);

create policy "Users can update their own profile"
on public.user_profiles
for update
using (auth.uid() = user_id)
with check (auth.uid() = user_id);

create policy "Users can read their own preferences"
on public.user_preferences
for select
using (auth.uid() = user_id);

create policy "Users can insert their own preferences"
on public.user_preferences
for insert
with check (auth.uid() = user_id);

create policy "Users can update their own preferences"
on public.user_preferences
for update
using (auth.uid() = user_id)
with check (auth.uid() = user_id);
