# Deploying `whatsapp-daemon`

The daemon owns one WhatsApp linked device, listens for allowlisted groups,
downloads media as it arrives, and writes into the local pipeline DB.

Set the repo root once if you cloned somewhere other than `~/wa-agent-pipeline`:

```sh
export WA_AGENT_PIPELINE_HOME="${WA_AGENT_PIPELINE_HOME:-$HOME/wa-agent-pipeline}"
```

Files it owns:
- `$WA_AGENT_PIPELINE_HOME/whatsapp-daemon/bin/whatsapp-daemon` — the binary
- `$WA_AGENT_PIPELINE_HOME/whatsapp-daemon/auth.db` — whatsmeow auth, created at first pair
- `$WA_AGENT_PIPELINE_HOME/whatsapp-daemon/control.sock` — backfill IPC socket
- `$WA_AGENT_PIPELINE_HOME/whatsapp-daemon/daemon.log` — connection / error log
- `$WA_AGENT_PIPELINE_HOME/whatsapp-daemon/health.json` — status snapshot
- `$WA_AGENT_PIPELINE_HOME/db/wa_pipeline.db` — message store
- `$WA_AGENT_PIPELINE_HOME/media-cache/<jid>/<msg_id>.<ext>` — copied media

## 1. Build

```sh
cd "$WA_AGENT_PIPELINE_HOME/whatsapp-daemon"
go build -o bin/whatsapp-daemon ./cmd/whatsapp-daemon
```

`./bin/whatsapp-daemon help` prints `pair`, `serve`, and `backfill`.

## 2. Pair

```sh
"$WA_AGENT_PIPELINE_HOME/whatsapp-daemon/bin/whatsapp-daemon" pair
```

Scan the QR from WhatsApp → Settings → Linked Devices → Link a Device. If
`auth.db` already has a registered device, `pair` exits without changing it.

## 3. Install as a long-running service

This section is the **macOS launchd** path. On Linux, write an equivalent
systemd user unit that runs `whatsapp-daemon serve` with `Restart=on-failure`
and `WA_AGENT_PIPELINE_HOME` set in `Environment=`; the rest of this guide
applies the same way.

The plist committed in `deploy/launchd/com.openclaw.whatsapp-daemon.plist.example`
is a template. Launchd does not expand shell variables in `ProgramArguments`,
so instantiate it locally before loading:

```sh
python3 - <<'PY'
import os
from pathlib import Path

root = Path(os.environ.get("WA_AGENT_PIPELINE_HOME") or "~/wa-agent-pipeline").expanduser().resolve()
src = root / "deploy/launchd/com.openclaw.whatsapp-daemon.plist.example"
dst = Path.home() / "Library/LaunchAgents/com.openclaw.whatsapp-daemon.plist"
dst.parent.mkdir(parents=True, exist_ok=True)
dst.write_text(src.read_text().replace("__WA_AGENT_PIPELINE_HOME__", str(root)))
PY
launchctl load "$HOME/Library/LaunchAgents/com.openclaw.whatsapp-daemon.plist"
```

The service runs `whatsapp-daemon serve`, restarts on crash, and starts at
login. To stop:

```sh
launchctl unload "$HOME/Library/LaunchAgents/com.openclaw.whatsapp-daemon.plist"
```

## 4. Verify

After about a minute:

```sh
cat "$WA_AGENT_PIPELINE_HOME/whatsapp-daemon/health.json"
tail -f "$WA_AGENT_PIPELINE_HOME/whatsapp-daemon/daemon.log"
python3 "$WA_AGENT_PIPELINE_HOME/scripts/whatsapp_unseen.py" --no-advance
```

Expect `connected: true`, `ipc_listening: true`, and no message bodies in logs.

## Backfill

`whatsapp-daemon backfill` talks to the running `serve` daemon over
`$WA_AGENT_PIPELINE_HOME/whatsapp-daemon/control.sock`; the daemon uses its
existing whatsmeow connection to request older messages for an allowlisted chat.

```sh
cd "$WA_AGENT_PIPELINE_HOME/whatsapp-daemon"
./bin/whatsapp-daemon backfill --chat '<group-jid>@g.us' --tenant family --limit 50
```

Flags:

- `--chat <jid>`: required, must be in `config/whatsapp_groups.json`
- `--tenant <name>`: optional ownership check for the chat JID
- `--before <ts>`: optional anchor ceiling; unix seconds, RFC3339, or `YYYY-MM-DD`
- `--anchor-msg-id <id>`: optional exact local anchor message ID
- `--limit N`: optional; capped at 50 by the daemon
- `--control-socket <path>`: optional; defaults under `$WA_AGENT_PIPELINE_HOME`

Returned messages go through the same allowlist, idempotent upsert, and atomic
media write path as live ingestion. Old WhatsApp media URLs may already be
expired; those rows are kept with `media_hydration_status='failed: expired'`.

## Agent Cron

An OpenClaw cron prompt can use the wrapper directly:

```sh
exec timeout=120 python3 "${WA_AGENT_PIPELINE_HOME:-$HOME/wa-agent-pipeline}/scripts/whatsapp_cron_check.py" --tenant family
```

The agent should open action-relevant `→ file:` paths with its multimodal read
tool, then cache a short interpretation:

```sh
python3 - <<'PY'
import os, sqlite3
from pathlib import Path

root = Path(os.environ.get("WA_AGENT_PIPELINE_HOME") or "~/wa-agent-pipeline").expanduser()
con = sqlite3.connect(root / "db/wa_pipeline.db")
con.execute(
    "update whatsapp_messages set media_extracted_text=?, media_extracted_by=? where id=?",
    ("<brief summary>", "<agent-name>", "<message id from unseen output>"),
)
con.commit()
PY
```

## Maintenance

- **Re-link** if WhatsApp invalidates the device: rerun `pair`.
- **Adjust the allowlist** in `$WA_AGENT_PIPELINE_HOME/config/whatsapp_groups.json`,
  then send `SIGHUP` to the daemon or wait for the file watcher.
- **Historical media** is best-effort and depends on phone-side retention.

## Rollback

Unload the launchd agent, keep the DB/media cache, and point your cron prompt
away from `whatsapp_cron_check.py`. The daemon's inserts are idempotent, so
stopping it does not delete data.
