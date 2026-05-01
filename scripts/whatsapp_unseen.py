#!/usr/bin/env python3
"""Print group-chat messages received since the last successful pass.

The downstream agent reads stdout and decides what (if anything) to act on.
Plumbing-level filters only (from_me, reactions, system); no keyword
matching — the agent does the judging.

Caption-less media (images, documents, videos) are surfaced with a
placeholder line so the agent at least knows something was shared and
can decide whether to open it.
"""
from __future__ import annotations
import argparse, json, os, sqlite3, sys, time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(os.environ.get("WA_AGENT_PIPELINE_HOME") or Path(__file__).resolve().parents[1]).expanduser().resolve()
DEFAULT_DB = ROOT / "db" / "wa_pipeline.db"
DEFAULT_CONFIG = ROOT / "config" / "whatsapp_groups.json"
SKIP_TYPES = {"reaction", "sticker", "system", "group_event", "type_46", "type_66", "ptt"}
MEDIA_TYPES = {"image", "document", "video", "audio"}
DEFAULT_OVERLAP = 1800  # 30 min back-overlap to absorb late-delivered messages
INGEST_MARGIN = 60      # cursor cannot pass this many seconds before latest OK ingest
HYDRATED_SNIPPET_CHARS = 600  # how much extracted text to surface inline

def group_tenants(group: dict) -> list[str]:
    tenants = group.get("tenants")
    if not isinstance(tenants, list) or not tenants or not all(isinstance(t, str) and t.strip() for t in tenants):
        raise SystemExit(f'Invalid or missing tenants for group {group.get("jid", "<missing jid>")}: expected non-empty list of strings')
    return [t.strip() for t in tenants]


def tenant_group_jids(config_path: Path, tenant: str) -> list[str]:
    with config_path.open() as f:
        cfg = json.load(f)
    if not cfg.get("enabled", False):
        return []
    jids: list[str] = []
    for group in cfg.get("groups", []):
        jid = str(group.get("jid", "")).strip()
        if tenant in group_tenants(group) and jid:
            jids.append(jid)
    return jids


def tenant_state_path(tenant: str) -> Path:
    return ROOT / "state" / f"whatsapp_seen_ts.{tenant}"


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    try:
        tmp.write_text(text)
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            tmp.unlink()


def media_placeholder(row: dict) -> str:
    """Render a one-line placeholder for caption-less media.

    Format: `[image]`, `[document: filename.pdf]`, `[video: 2.4MB]`, etc.
    The intent is to surface *enough* metadata that the agent can decide
    whether the message needs a follow-up read.
    """
    mtype = (row.get("message_type") or "media").lower()
    parts = [mtype]
    title = (row.get("media_title") or "").strip()
    # WhatsApp media titles are sometimes a base64-looking media key, not a
    # real filename. If it doesn't look like a filename, drop.
    if title and "." in title and len(title) <= 200 and "/" not in title and "+" not in title:
        parts.append(title)
    elif row.get("media_path"):
        ext = os.path.splitext(row["media_path"])[1]
        if ext:
            parts.append(f"*{ext}")
    size = row.get("media_size") or 0
    if size:
        parts.append(_human_size(size))
    return f"[{': '.join(parts[:2])}{(' · ' + parts[2]) if len(parts) > 2 else ''}]"


def _human_size(n: int) -> str:
    for unit, threshold in (("KB", 1024), ("MB", 1024 * 1024), ("GB", 1024 ** 3)):
        if n < threshold * 1024:
            return f"{n / threshold:.1f}{unit}"
    return f"{n / (1024 ** 3):.1f}GB"


def latest_ok_ingest_finished_at(con: sqlite3.Connection) -> int | None:
    """Return finished_at of the most recent successful, non-dry-run ingest, or None.

    The cursor advance is gated on this so that if ingest never ran (or failed),
    `whatsapp_unseen.py` cannot silently move the seen-cursor past data the
    agent never had a chance to see.
    """
    row = con.execute(
        """select max(finished_at) from whatsapp_ingest_runs
            where status = 'ok' and dry_run = 0 and finished_at is not null"""
    ).fetchone()
    return int(row[0]) if row and row[0] is not None else None

def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", type=Path, default=DEFAULT_DB)
    p.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    p.add_argument("--tenant", required=True, help="tenant scope for cursor + group filtering")
    p.add_argument("--since-hours", type=float, default=None,
                   help="hours of history. Default: since last successful advance, "
                        "with 30 min overlap. Pass 0 for full history.")
    p.add_argument("--state", type=Path, default=None)
    p.add_argument("--no-advance", action="store_true",
                   help="don't write the new cutoff. Use for inspection or dry runs.")
    args = p.parse_args()
    state_path = args.state or tenant_state_path(args.tenant)
    allowed_jids = tenant_group_jids(args.config, args.tenant)

    now = int(time.time())
    if args.since_hours == 0:
        cutoff = 0
    elif args.since_hours is not None:
        cutoff = int(now - args.since_hours * 3600)
    elif state_path.exists():
        try:
            cutoff = max(0, int(state_path.read_text().strip()) - DEFAULT_OVERLAP)
        except ValueError:
            cutoff = now - 24 * 3600
    else:
        cutoff = now - 24 * 3600

    # Hydration resurfacing uses per-row `media_surfaced_at` rather than
    # a cursor file. Eliminates the same-second-cursor race Codex flagged
    # (two rows hydrated at the same epoch second could be missed if
    # the cursor advanced between the SELECT and the next tick).
    con = sqlite3.connect(args.db); con.row_factory = sqlite3.Row
    if allowed_jids:
        tenant_clause = f" and chat_jid in ({','.join('?' for _ in allowed_jids)})"
        tenant_params: list[str] = allowed_jids
    else:
        tenant_clause = " and 0 = 1"
        tenant_params = []
    rows = con.execute(
        """select id, chat_jid, chat_label, sender_name, ts, message_type,
                  media_type, media_title, media_path, media_local_path,
                  media_size, media_extracted_text, media_extracted_by,
                  media_hydrated_at, media_hydration_status, media_surfaced_at,
                  coalesce(text,'') as text
             from whatsapp_messages
            where from_me = 0
              {tenant_clause}
              and (
                ts >= ?
                or (media_hydrated_at is not null and media_surfaced_at is null)
              )
            order by chat_label, ts""".format(tenant_clause=tenant_clause),
        (*tenant_params, cutoff),
    ).fetchall()

    msgs: list[dict] = []
    for r in rows:
        d = dict(r)
        mtype = (d.get("message_type") or "").lower()
        if mtype in SKIP_TYPES:
            continue
        text = d.get("text", "").strip()
        # Build base line: caption, media placeholder, or a generic
        # type-only fallback. Never `continue` past this point — silent
        # drop for unknown types is exactly the bug class this script
        # exists to prevent.
        if text:
            display = text
        elif mtype in MEDIA_TYPES:
            display = media_placeholder(d)
        else:
            # Unknown / future / unseen-before message_type. Surface
            # what we know rather than dropping. Examples worth not
            # losing: contact_card, location, list_message, poll, etc.
            display = f"[{mtype or 'unknown'}]"
        # If the row's media has been hydrated (file is now in local
        # cache) and we've never surfaced it before, append either:
        # - the previously-cached extraction (if a prior cron tick had
        #   the agent read the file and write back to media_extracted_text)
        # - or a → file: pointer the agent can `read` to see the image/doc
        #   directly (gpt-5.5 multimodal). The agent then chooses whether
        #   to read based on the cron-prompt selectivity rules.
        hydrated_at = d.get("media_hydrated_at") or 0
        already_surfaced = d.get("media_surfaced_at") is not None
        local_path = d.get("media_local_path") or ""
        if hydrated_at and not already_surfaced:
            extracted = (d.get("media_extracted_text") or "").strip()
            extracted_by = (d.get("media_extracted_by") or "").strip()
            if extracted:
                snippet = extracted[:HYDRATED_SNIPPET_CHARS].replace("\n", " / ")
                if len(extracted) > HYDRATED_SNIPPET_CHARS:
                    snippet += "…"
                tag = f"extracted ({extracted_by})" if extracted_by else "extracted"
                display = f"{display} → {tag}: {snippet}"
            elif local_path:
                display = f"{display} → file: {local_path} (id: {d['id']}; open with `read` if action-relevant)"
        d["display"] = display
        d["_needs_surface_mark"] = bool(hydrated_at and not already_surfaced)
        msgs.append(d)

    by_group: dict[str, list[dict]] = {}
    for m in msgs:
        by_group.setdefault(m["chat_label"], []).append(m)

    cutoff_str = datetime.fromtimestamp(cutoff, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print(f"# Unseen parent-group messages since {cutoff_str}")
    print(f"# {len(msgs)} messages across {len(by_group)} group(s)")
    print()
    if not msgs:
        print("(no new messages)")
    for label, gmsgs in by_group.items():
        jid = gmsgs[0]["chat_jid"]
        print(f"## {label} — jid: `{jid}`  ({len(gmsgs)} messages)")
        print()
        for m in gmsgs:
            when = datetime.fromtimestamp(m["ts"], tz=timezone.utc).strftime("%a %d %b %Y %H:%M")
            display = m["display"].replace("\n", " / ").replace("\r", " ")
            print(f"- [{when}] [{m['sender_name'] or '?'}] {display}")
        print()

    if not args.no_advance:
        # Fail-closed message cursor: never advance past data we haven't
        # ingested yet. Without this, a missed/killed ingest combined with
        # auto-advance silently loses messages (2026-04-30 incident).
        ingest_ts = latest_ok_ingest_finished_at(con)
        if ingest_ts is None:
            print("# WARNING: no successful ingest run on record; cursor unchanged.",
                  file=sys.stderr)
            return
        max_advance = ingest_ts - INGEST_MARGIN
        prior_cursor = 0
        if state_path.exists():
            try:
                prior_cursor = int(state_path.read_text().strip())
            except ValueError:
                prior_cursor = 0
        if max_advance <= prior_cursor:
            print(
                f"# WARNING: latest OK ingest ({ingest_ts}) is not newer than cursor "
                f"({prior_cursor}); cursor unchanged.",
                file=sys.stderr,
            )
        else:
            new_cursor = min(now, max_advance)
            atomic_write_text(state_path, str(new_cursor))

        # Mark hydrated rows we just surfaced. Per-row marker — race-free
        # vs. a cursor file. Rows where the hydration result was just
        # printed get media_surfaced_at = now; future ticks won't pick
        # them up again unless a fresh re-hydration clears the mark.
        ids_to_mark = [m["id"] for m in msgs if m.get("_needs_surface_mark")]
        if ids_to_mark:
            placeholders = ",".join("?" * len(ids_to_mark))
            con.execute(
                f"update whatsapp_messages set media_surfaced_at = ? where id in ({placeholders})",
                (now, *ids_to_mark),
            )
            con.commit()


if __name__ == "__main__":
    main()
