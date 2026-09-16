-- Caption service schema — run once in the Supabase SQL editor.
--
-- caption_jobs is the durable job store: survives server restarts, drives
-- the notification feed, and tracks expiry for the 48h retention cleanup.

create table if not exists caption_jobs (
  id            uuid primary key default gen_random_uuid(),
  user_id       uuid not null,
  kind          text not null check (kind in ('transcribe', 'render')),
  status        text not null default 'queued'
                check (status in ('queued', 'transcribing', 'romanizing',
                                  'rendering', 'completed', 'failed', 'cancelled')),
  export        text,
  error         text,
  transcript    jsonb,
  video_info    jsonb,
  source_path   text,
  output_path   text,
  filename      text,
  notified      boolean not null default false,
  seen          boolean not null default false,
  created_at    timestamptz not null default now(),
  updated_at    timestamptz not null default now(),
  expires_at    timestamptz not null default now() + interval '48 hours'
);

-- Migration for installations created before cancellation support.
alter table caption_jobs drop constraint if exists caption_jobs_status_check;
alter table caption_jobs add constraint caption_jobs_status_check
  check (status in ('queued', 'transcribing', 'romanizing', 'rendering',
                    'completed', 'failed', 'cancelled'));

create index if not exists caption_jobs_user_idx on caption_jobs (user_id, created_at desc);
create index if not exists caption_jobs_expiry_idx on caption_jobs (expires_at);

alter table caption_jobs enable row level security;

create policy "users read own caption jobs"
  on caption_jobs for select
  using (auth.uid() = user_id);

insert into storage.buckets (id, name, public)
values ('captions', 'captions', false)
on conflict (id) do nothing;
