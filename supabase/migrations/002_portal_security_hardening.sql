-- Harden user-owned data access and keep common snapshot lookups efficient.

alter function public.set_updated_at()
  set search_path = pg_catalog, public;

create index if not exists stock_snapshots_update_run_id_idx
  on public.stock_snapshots(update_run_id);

drop policy if exists "Users can read their own profile" on public.user_profiles;
create policy "Users can read their own profile"
on public.user_profiles
for select
to authenticated
using ((select auth.uid()) = user_id);

drop policy if exists "Users can insert their own profile" on public.user_profiles;
create policy "Users can insert their own profile"
on public.user_profiles
for insert
to authenticated
with check ((select auth.uid()) = user_id);

drop policy if exists "Users can update their own profile" on public.user_profiles;
create policy "Users can update their own profile"
on public.user_profiles
for update
to authenticated
using ((select auth.uid()) = user_id)
with check ((select auth.uid()) = user_id);

drop policy if exists "Users can read their own preferences" on public.user_preferences;
create policy "Users can read their own preferences"
on public.user_preferences
for select
to authenticated
using ((select auth.uid()) = user_id);

drop policy if exists "Users can insert their own preferences" on public.user_preferences;
create policy "Users can insert their own preferences"
on public.user_preferences
for insert
to authenticated
with check ((select auth.uid()) = user_id);

drop policy if exists "Users can update their own preferences" on public.user_preferences;
create policy "Users can update their own preferences"
on public.user_preferences
for update
to authenticated
using ((select auth.uid()) = user_id)
with check ((select auth.uid()) = user_id);
