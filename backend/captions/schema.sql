-- Hosted backend schema — run in the Supabase SQL editor. Safe to re-run.
--
-- caption_jobs is the durable job store: survives server restarts, drives
-- the notification feed, and tracks expiry for the 48h retention cleanup.
-- clip_metadata + the clips bucket back the clipper (supabase_client.py).

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

drop policy if exists "users read own caption jobs" on caption_jobs;
create policy "users read own caption jobs"
  on caption_jobs for select
  using (auth.uid() = user_id);

-- Storage bucket for caption sources + outputs (private).
insert into storage.buckets (id, name, public)
values ('captions', 'captions', false)
on conflict (id) do nothing;

-- Clipper output: one row per uploaded clip. Source of truth for the clip
-- dashboard; storage files are swept after CLIP_TTL_SECONDS (default 6h).
create table if not exists clip_metadata (
  id            uuid primary key default gen_random_uuid(),
  user_id       uuid not null,
  job_id        uuid not null,
  filename      text not null,
  storage_path  text not null,       -- {user_id}/{job_id}/{filename} in the clips bucket
  description   text,
  source_url    text,
  start_time    double precision,    -- seconds into the source video
  end_time      double precision,
  created_at    timestamptz not null default now()
);

-- Added after the first deploys; upload_clip_to_storage() retries without
-- them if they're missing, so older tables keep working until this runs.
alter table clip_metadata add column if not exists hook           text;
alter table clip_metadata add column if not exists virality_score integer;  -- 1-10 from Gemini
alter table clip_metadata add column if not exists clip_type      text;

create index if not exists clip_metadata_user_idx on clip_metadata (user_id, created_at desc);

alter table clip_metadata enable row level security;

drop policy if exists "users read own clips" on clip_metadata;
create policy "users read own clips"
  on clip_metadata for select
  using (auth.uid() = user_id);

-- Storage bucket for clipper output (private, served via signed URLs).
insert into storage.buckets (id, name, public)
values ('clips', 'clips', false)
on conflict (id) do nothing;
