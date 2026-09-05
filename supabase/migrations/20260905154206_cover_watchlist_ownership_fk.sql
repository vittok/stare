-- Cover the composite watchlist ownership foreign key used for cascades.

create index if not exists user_watchlist_items_watchlist_owner_idx
  on public.user_watchlist_items (watchlist_id, user_id);
