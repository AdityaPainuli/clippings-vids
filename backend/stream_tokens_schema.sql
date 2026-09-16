-- Durable, single-use browser SSE tokens.
-- Run once in the Supabase SQL editor before enabling the hosted service.

create table if not exists stream_tokens (
  token_hash text primary key,
  user_id uuid not null,
  expires_at timestamptz not null,
  used_at timestamptz
);

create index if not exists stream_tokens_expiry_idx
  on stream_tokens (expires_at);

alter table stream_tokens enable row level security;
