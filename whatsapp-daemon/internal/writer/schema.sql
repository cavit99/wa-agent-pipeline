pragma journal_mode = wal;
pragma synchronous = normal;
pragma foreign_keys = on;

create table if not exists whatsapp_groups (
  jid text primary key,
  label text not null,
  purpose text,
  ingest_text integer not null default 1,
  ingest_media_metadata integer not null default 1,
  copy_media integer not null default 0,
  index_embeddings integer not null default 0,
  retention_days integer,
  updated_at integer not null
);

create table if not exists whatsapp_messages (
  id text primary key,
  source_pk integer not null,
  chat_jid text not null,
  chat_label text not null,
  msg_id text not null,
  sender_jid text,
  sender_name text,
  ts integer not null,
  from_me integer not null,
  text text,
  raw_type integer not null,
  message_type text,
  media_type text,
  media_title text,
  media_path text,
  media_url text,
  media_size integer,
  text_hash text not null,
  ingested_at integer not null,
  deleted_or_missing integer not null default 0,
  media_local_path text,
  media_extracted_text text,
  media_extracted_by text,
  media_hydrated_at integer,
  media_hydration_status text,
  media_hydration_attempts integer not null default 0,
  media_surfaced_at integer,
  foreign key(chat_jid) references whatsapp_groups(jid)
);

create index if not exists idx_whatsapp_messages_chat_ts on whatsapp_messages(chat_jid, ts);
create index if not exists idx_whatsapp_messages_sender_ts on whatsapp_messages(sender_jid, ts);
create index if not exists idx_whatsapp_messages_source_pk on whatsapp_messages(source_pk);
create index if not exists idx_whatsapp_messages_media_hydrated_at on whatsapp_messages(media_hydrated_at) where media_hydrated_at is not null;

create virtual table if not exists whatsapp_messages_fts
using fts5(id unindexed, text, chat_label, sender_name, media_title);

create table if not exists whatsapp_ingest_runs (
  run_id text primary key,
  started_at integer not null,
  finished_at integer,
  mode text not null,
  dry_run integer not null default 0,
  allowed_group_count integer not null,
  scanned_messages integer not null default 0,
  inserted_messages integer not null default 0,
  updated_messages integer not null default 0,
  skipped_messages integer not null default 0,
  media_copied integer not null default 0,
  -- status='ok' means message text persisted; media failures live on whatsapp_messages.media_hydration_status.
  status text not null,
  error text
);
