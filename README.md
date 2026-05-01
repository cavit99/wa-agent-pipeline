# wa-agent-pipeline

Personal pipeline for ingesting WhatsApp group-chat messages into
local storage and surfacing them to LLM agents. One paired
linked-device, one local sqlite store, multi-tenant on the read
side so multiple downstream agents can each see their own scoped
subset of monitored chats.

## Why a daemon and not just WhatsApp Desktop + a SQLite reader

Reading WhatsApp Desktop's `ChatStorage.sqlite` from disk (e.g. via
[wacrawl](https://github.com/steipete/wacrawl)) works for forensic
queries but is unreliable as a live ingest source. WA Desktop is a
foreground GUI app — macOS may suspend it in the background, it only
fetches media lazily when the user clicks, and a file-mirror approach
is poll-based rather than push. WhatsApp's CDN expires media URLs
empirically in ~3-4 weeks, so by the time a poll catches up, media for
older messages is often unrecoverable.

This pipeline pairs as its own linked device under launchd/systemd,
receives messages over its own websocket independent of any GUI app,
and downloads media on receive while URLs are still fresh. It also
adds default-deny allowlisting, per-tenant cursors, and on-demand
history backfill via WhatsApp's own protocol.

## Related: wacli

[steipete/wacli](https://github.com/steipete/wacli) is the closest
neighbour: another Go/whatsmeow tool with a local SQLite/FTS store,
continuous sync, media download, and anchor-based history backfill.
If you want a general human-operated WhatsApp CLI — search, send
text/files, reactions, contacts, groups, presence — use `wacli`.

This repo is narrower: an allowlisted ingest daemon for LLM agents.
It skips the interactive CLI surface and instead adds default-deny
group allowlisting, per-tenant read cursors, a shared Unix-socket
daemon for backfill, and a stdout-markdown + SQL writeback contract
for downstream agents. No `wacli` code is vendored.

## Quickstart

Prerequisites: Go 1.26+, a CGO toolchain, and SQLite headers — on macOS
that's `xcode-select --install`; on Debian/Ubuntu, `build-essential` and
`libsqlite3-dev`. The daemon and Python scripts are cross-platform; only
the example launchd deployment in [HOWTO-DEPLOY.md](whatsapp-daemon/HOWTO-DEPLOY.md)
is macOS-specific.

Defaults assume the repo lives at `~/wa-agent-pipeline`; set
`WA_AGENT_PIPELINE_HOME=/path/to/clone` if you put it elsewhere.

```sh
git clone https://github.com/cavit99/wa-agent-pipeline ~/wa-agent-pipeline
cd ~/wa-agent-pipeline
cp config/whatsapp_groups.example.json config/whatsapp_groups.json
# edit config/whatsapp_groups.json: replace the fixture JID with your real
# group JID(s), then flip "enabled" to true
cd whatsapp-daemon && go build -o bin/whatsapp-daemon ./cmd/whatsapp-daemon
./bin/whatsapp-daemon pair && ./bin/whatsapp-daemon serve
```

Scan the QR with your phone. `serve` runs in the foreground and writes DB rows
to `../db/wa_pipeline.db`.

### Finding your group JIDs

WhatsApp doesn't surface group JIDs in the UI. Easiest way to capture
them: pair the daemon and run `serve` with `enabled: false` (the default
in the example config). Send any message into your target group from
your phone. The daemon will refuse to ingest it but will log the JID
to `whatsapp-daemon/daemon.log`:

```text
message_dropped_disallowed chat_jid=120363406219631820@g.us msg_id=3EB...
```

Copy the `chat_jid` value into `config/whatsapp_groups.json`, flip
`enabled` to `true`, and `kill -HUP $(pgrep -f whatsapp-daemon)`
(or just restart) to hot-reload.

## Architecture

Hard rule: **deterministic stages stay dumb; interpretation lives in
the agent.** The agent on the read side is expected to be multimodal
(reads images / .docx / .pdf directly via its own tooling), so this
pipeline does no local extraction.

1. **Daemon ingest** — `whatsapp-daemon/bin/whatsapp-daemon serve`.
   A dedicated whatsmeow linked-device listener receives messages
   for the configured group JIDs, default-denies every other JID,
   and writes rows directly to `db/wa_pipeline.db`.
2. **Media cache** — for groups with `copy_media: true`, the daemon
   downloads bytes on receive and writes them atomically into
   `media-cache/<jid>/<msg_id>.<ext>`, then updates `media_local_path`
   + `media_hydrated_at`. `copy_media` defaults to `false`, so groups
   that only need text + metadata don't pay the bandwidth.
3. **Unseen** — `scripts/whatsapp_unseen.py --tenant <name>`. Prints
   markdown to stdout for messages since the per-tenant cursor at
   `state/whatsapp_seen_ts.<name>`. Hydrated media without a cached
   interpretation gets a `→ file: <path>` line; already-interpreted
   media gets `→ extracted (<source>): <snippet>`. Cursor advancement
   remains gated by successful daemon ingest run rows in
   `whatsapp_ingest_runs`.
4. **Agent** — typically fired by cron on whatever cadence makes sense
   for your use case. For action-relevant `→ file:` lines, opens with
   its own `read` tool and writes a brief interpretation back to
   `media_extracted_text` + `media_extracted_by = '<agent-name>'`.
5. **Orchestrator** — `scripts/whatsapp_cron_check.py`. Thin wrapper the
   cron prompts call; forwards to `whatsapp_unseen.py`.
6. **On-demand backfill** — `whatsapp-daemon backfill --chat <jid> [--limit N]`.
   IPC client; talks to the running `serve` daemon over
   `whatsapp-daemon/control.sock`. Daemon issues a whatsmeow
   `BuildHistorySyncRequest` over its existing connection, ingests the
   ON_DEMAND `*events.HistorySync` event through the same writer/listener
   path as live messages, and streams progress back as newline-delimited
   JSON events. Used for retroactive media hydration on already-allowlisted
   chats and for backfilling history when adding a new JID. URL freshness
   on WA's media CDN is empirically ~3-4 weeks; downloads that come back
   404 or 410 are flagged `media_hydration_status='failed: expired'` and
   the message metadata is still kept.

   The verdict streamed back at the end is media-oriented: `VIABLE` only
   if the sync produced messages, media envelopes, and same-day-before
   anchor coverage. A pure text-only history backfill can succeed at
   the row level and still report `UNAVAILABLE` for the media verdict.

## Agent integration

This repo is meant to be consumed by an LLM-driven agent, ideally with a
multimodal file-read tool. It pairs naturally with agent runtimes like
**OpenClaw** and **Hermes Agent**, but the contract is just a stdout
stream + a SQL writeback — anything from a cron-driven Claude/GPT
script to Letta or a custom whatsmeow consumer can sit on top.

A cron tick runs the unseen script and feeds its stdout into the
agent prompt:

```sh
timeout 120 python3 "${WA_AGENT_PIPELINE_HOME:-$HOME/wa-agent-pipeline}/scripts/whatsapp_unseen.py" --tenant family
```

The script prints markdown grouped by chat. Media rows have two important
optional suffixes:

```text
- [Fri 01 May 2026 08:12] [Alex] [document: agenda.pdf] → file: /path/to/wa-agent-pipeline/media-cache/123@g.us/abc.pdf (id: 9f3...; open with `read` if action-relevant)
- [Fri 01 May 2026 08:20] [Sam] [image] → extracted (default): schedule photo; meeting moved to Thursday
```

`→ file: <path>` means the daemon has hydrated the image/document/video/audio
locally and the agent can open it with its own read tool. `→ extracted
(<source>): <text>` means a prior agent tick already cached the interpretation.

Contract for any LLM agent runtime:

1. Cron tick runs `whatsapp_unseen.py --tenant <agent>` and passes stdout to
   the agent.
2. The agent opens only action-relevant `→ file:` paths.
3. After reading, it writes a short durable summary back:

```sql
update whatsapp_messages
   set media_extracted_text = ?,
       media_extracted_by = '<agent-name>'
 where id = ?;
```

Future ticks then show the cached `→ extracted (...)` text instead of asking
the agent to reopen the same file. `scripts/whatsapp_cron_check.py` is a
thin orchestrator wrapper; runtimes can also call `whatsapp_unseen.py`
directly.

## Tenants

The pipeline is multi-tenant on the read side: one daemon, one DB, one
allowlist file, but per-tenant cursors and per-tenant filters. Each
group entry in `config/whatsapp_groups.json` carries a `"tenants": [...]`
array (e.g. `["family"]`, `["main"]`, or `["family", "main"]`). The
listener writes every allowlisted message; tenant filtering happens at
read time:

- `scripts/whatsapp_unseen.py --tenant <name>` — required flag; uses
  `state/whatsapp_seen_ts.<name>` cursor and emits only groups whose
  `tenants` array contains `<name>`.
- `whatsapp-daemon backfill --chat <jid> --tenant <name>` — daemon-side
  validates that `<jid>` is owned by `<name>` (rejects with
  `verdict: ERROR, reason: jid_not_owned_by_tenant` if not). Tenant is
  optional here; without it, ownership validation is skipped but the
  chat must still be in the global allowlist.

Tenant scoping is read-side only — messages do not carry tenant IDs
in the schema. Visibility is derived from the current allowlist
config at read time, so a config edit that adds or removes tenants
from a group changes that group's tenant visibility retroactively.

Each downstream agent's cron should call this pipeline with its own
`--tenant` flag.

## Storage

- `db/wa_pipeline.db` — sqlite WAL.
- `state/whatsapp_seen_ts.<tenant>` — per-tenant epoch-seconds cursor.
- `config/whatsapp_groups.json` — exact group JID allowlist. The daemon
  polls its mtime every 2s and also reloads on SIGHUP, so edits hot-reload.
- `media-cache/<jid>/` — daemon-hydrated media files.
- `whatsapp-daemon/auth.db` — whatsmeow persistent auth store.
- `whatsapp-daemon/daemon.log` — daemon health and connection logs.
- `whatsapp-daemon/health.json` — status snapshot (`connected`,
  `last_message_at`, `queue_depth`, `media_failures_5m`, `ipc_listening`).
- `whatsapp-daemon/control.sock` — Unix socket for backfill IPC, mode 0600.
- `tests/` — pytest fixtures.

## Cron entry point

```sh
timeout 120 python3 "${WA_AGENT_PIPELINE_HOME:-$HOME/wa-agent-pipeline}/scripts/whatsapp_cron_check.py" --tenant <name>
```

Do not invoke ingest / hydrate / unseen separately from a cron prompt.

## Acknowledgements

The daemon stands on:

- [whatsmeow](https://github.com/tulir/whatsmeow) (Mozilla Public License 2.0)
  — does the heavy lifting: pairing, multi-device protocol, peer-to-peer
  encryption, history-sync request building, media download. Without it
  none of this is possible.
- [mattn/go-sqlite3](https://github.com/mattn/go-sqlite3) (MIT) — SQLite
  driver used for the message store and whatsmeow's auth store.
- [mdp/qrterminal](https://github.com/mdp/qrterminal) (MIT) — renders
  the linked-device QR code in the terminal during pairing.

Architectural patterns drew from public read-throughs of
[lharries/whatsapp-mcp](https://github.com/lharries/whatsapp-mcp),
[aldinokemal/go-whatsapp-web-multidevice](https://github.com/aldinokemal/go-whatsapp-web-multidevice),
[steipete/wacrawl](https://github.com/steipete/wacrawl), and
[steipete/wacli](https://github.com/steipete/wacli) — none of their
code is forked or vendored here, but their structure helped shape
decisions like file-copy hydration vs. live ingest, IPC for backfill,
and the writer's idempotent upsert shape. wacli is the most directly
comparable tool on the same whatsmeow stack; broader feature surface
(sending, groups, contacts, reactions), single-tenant by design.

## License

MIT. See [LICENSE](LICENSE).
