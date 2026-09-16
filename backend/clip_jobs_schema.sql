-- Durable hosted clipper jobs.
-- Run once in the Supabase SQL editor before starting the service.

create table if not exists clip_jobs (
  id                 uuid primary key,
  user_id            uuid not null,
  source_url         text,
  source_path        text,
  instructions       text,
  captions           boolean not null default true,
  clip_style         text not null,
  caption_style      text not null,
  clip_count         integer,
  min_clip_length    integer,
  max_clip_length    integer,
  cache_key          text,
  status             text not null default 'queued'
                     check (status in ('queued', 'downloading', 'analyzing',
                                       'clipping', 'uploading', 'completed', 'failed')),
  results            jsonb,
  error              text,
  warnings           text,
  failed_clips      jsonb,
  cached             boolean not null default false,
  worker_id          text,
  lease_until        timestamptz,
  created_at         timestamptz not null default now(),
  updated_at         timestamptz not null default now()
);

create index if not exists clip_jobs_user_idx
  on clip_jobs (user_id, created_at desc);

create index if not exists clip_jobs_queue_idx
  on clip_jobs (status, created_at);

create index if not exists clip_jobs_lease_idx
  on clip_jobs (lease_until)
  where status in ('downloading', 'analyzing', 'clipping', 'uploading');

alter table clip_jobs enable row level security;

create policy "users read own clip jobs"
  on clip_jobs for select
  using (auth.uid() = user_id);

insert into storage.buckets (id, name, public)
values ('clip-sources', 'clip-sources', false)
on conflict (id) do nothing;
