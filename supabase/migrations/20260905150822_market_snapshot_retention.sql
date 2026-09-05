-- Retain a rolling 30 days of market snapshots while always preserving the
-- newest successful report so the portal cannot be emptied during an outage.

create extension if not exists pg_cron with schema pg_catalog;

create schema if not exists private;

create or replace function private.cleanup_market_snapshot_retention(
  retention_days integer default 30
)
returns integer
language plpgsql
security invoker
set search_path = pg_catalog, public
as $$
declare
  deleted_runs integer;
begin
  if retention_days < 1 then
    raise exception 'retention_days must be at least 1';
  end if;

  delete from public.update_runs
  where coalesce(completed_at, created_at) < now() - make_interval(days => retention_days)
    and id is distinct from (
      select id
      from public.update_runs
      where status = 'success'
      order by coalesce(completed_at, created_at) desc
      limit 1
    );

  get diagnostics deleted_runs = row_count;
  return deleted_runs;
end;
$$;

revoke all on function private.cleanup_market_snapshot_retention(integer)
from public, anon, authenticated;

grant execute on function private.cleanup_market_snapshot_retention(integer)
to postgres;

select cron.unschedule(jobid)
from cron.job
where jobname = 'stare-market-snapshot-retention';

select cron.schedule(
  'stare-market-snapshot-retention',
  '15 2 * * *',
  'select private.cleanup_market_snapshot_retention(30);'
);
