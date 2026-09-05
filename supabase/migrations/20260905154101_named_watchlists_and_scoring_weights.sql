-- Named watchlists and per-user recommendation factor weights.

create table public.user_watchlists (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references auth.users(id) on delete cascade,
  name text not null check (name = btrim(name) and char_length(name) between 1 and 60),
  is_default boolean not null default false,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (id, user_id)
);

create unique index user_watchlists_user_name_idx
  on public.user_watchlists (user_id, lower(name));

create unique index user_watchlists_one_default_idx
  on public.user_watchlists (user_id)
  where is_default;

create index user_watchlists_user_updated_idx
  on public.user_watchlists (user_id, updated_at desc);

create table public.user_watchlist_items (
  watchlist_id uuid not null,
  user_id uuid not null references auth.users(id) on delete cascade,
  ticker text not null
    check (ticker = upper(btrim(ticker)) and char_length(ticker) between 1 and 32),
  created_at timestamptz not null default now(),
  primary key (watchlist_id, ticker),
  foreign key (watchlist_id, user_id)
    references public.user_watchlists(id, user_id) on delete cascade
);

create index user_watchlist_items_user_idx
  on public.user_watchlist_items (user_id, ticker);

create table public.user_scoring_weights (
  user_id uuid primary key references auth.users(id) on delete cascade,
  group_sentiment_weight numeric(4, 2) not null default 1.00
    check (group_sentiment_weight between 0 and 2),
  pe_weight numeric(4, 2) not null default 1.00
    check (pe_weight between 0 and 2),
  pb_weight numeric(4, 2) not null default 1.00
    check (pb_weight between 0 and 2),
  peg_weight numeric(4, 2) not null default 1.00
    check (peg_weight between 0 and 2),
  dividend_weight numeric(4, 2) not null default 1.00
    check (dividend_weight between 0 and 2),
  momentum_weight numeric(4, 2) not null default 1.00
    check (momentum_weight between 0 and 2),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  check (
    group_sentiment_weight + pe_weight + pb_weight + peg_weight
      + dividend_weight + momentum_weight > 0
  )
);

create trigger user_watchlists_set_updated_at
before update on public.user_watchlists
for each row execute function public.set_updated_at();

create trigger user_scoring_weights_set_updated_at
before update on public.user_scoring_weights
for each row execute function public.set_updated_at();

alter table public.user_watchlists enable row level security;
alter table public.user_watchlist_items enable row level security;
alter table public.user_scoring_weights enable row level security;

revoke all on table public.user_watchlists from anon, authenticated;
revoke all on table public.user_watchlist_items from anon, authenticated;
revoke all on table public.user_scoring_weights from anon, authenticated;

grant select, insert, update, delete on table public.user_watchlists to authenticated;
grant select, insert, update, delete on table public.user_watchlist_items to authenticated;
grant select, insert, update, delete on table public.user_scoring_weights to authenticated;

create policy "Users manage their own watchlists"
on public.user_watchlists
for all
to authenticated
using ((select auth.uid()) = user_id)
with check ((select auth.uid()) = user_id);

create policy "Users manage their own watchlist items"
on public.user_watchlist_items
for all
to authenticated
using ((select auth.uid()) = user_id)
with check ((select auth.uid()) = user_id);

create policy "Users manage their own scoring weights"
on public.user_scoring_weights
for all
to authenticated
using ((select auth.uid()) = user_id)
with check ((select auth.uid()) = user_id);

-- Preserve existing single-list preferences as a default named list.
with legacy_lists as (
  insert into public.user_watchlists (user_id, name, is_default)
  select user_id, 'My Watchlist', true
  from public.user_preferences
  where cardinality(watchlist) > 0
  on conflict do nothing
  returning id, user_id
)
insert into public.user_watchlist_items (watchlist_id, user_id, ticker)
select legacy_lists.id, legacy_lists.user_id, upper(btrim(ticker))
from legacy_lists
join public.user_preferences
  on user_preferences.user_id = legacy_lists.user_id
cross join lateral unnest(user_preferences.watchlist) as ticker
where char_length(btrim(ticker)) between 1 and 32
on conflict do nothing;
