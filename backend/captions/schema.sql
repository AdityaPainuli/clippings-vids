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
                                  'rendering', 'completed', 'failed')),
  export        text,                -- burned | overlay | ass | srt (render jobs)
  error         text,
  transcript    jsonb,               -- {language, backend, words: [...]}
  video_info    jsonb,               -- {width, height, duration, fps}
  source_path   text,                -- uploaded source video in storage (if any)
  output_path   text,                -- finished file in storage
  filename      text,               -- user-facing name of the finished file
  notified      boolean not null default false,  -- completion email sent
  seen          boolean not null default false,  -- user saw it in the app feed
  created_at    timestamptz not null default now(),
  updated_at    timestamptz not null default now(),
  expires_at    timestamptz not null default now() + interval '48 hours'
);

create index if not exists caption_jobs_user_idx    on caption_jobs (user_id, created_at desc);
create index if not exists caption_jobs_expiry_idx  on caption_jobs (expires_at);

-- Service role bypasses RLS; enable it anyway so anon/user keys can't read others' jobs.
alter table caption_jobs enable row level security;

create policy "users read own caption jobs"
  on caption_jobs for select
  using (auth.uid() = user_id);

-- Storage bucket for caption sources + outputs (private).
insert into storage.buckets (id, name, public)
values ('captions', 'captions', false)
on conflict (id) do nothing;
