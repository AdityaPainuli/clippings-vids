-- Shared cache for completed clip results across API workers.
create table if not exists clip_cache (
    cache_key text primary key,
    user_id text not null,
    results jsonb not null,
    expires_at timestamptz not null,
    created_at timestamptz not null default now()
);

create index if not exists clip_cache_user_id_idx
    on clip_cache (user_id);

create index if not exists clip_cache_expires_at_idx
    on clip_cache (expires_at);

alter table clip_cache enable row level security;
