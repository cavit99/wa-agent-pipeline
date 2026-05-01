#!/usr/bin/env python3
"""Cron-side WhatsApp check: print unseen daemon-ingested messages.

Thin wrapper so the cron prompt contract does not need to know where the
SQLite rows came from. Forwards to `whatsapp_unseen.py`, which owns
cursor advancement and media resurfacing.

Exit codes:
  0  unseen printed successfully.
  3  unseen script itself errored.
"""
from __future__ import annotations
import argparse
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(os.environ.get("WA_AGENT_PIPELINE_HOME") or Path(__file__).resolve().parents[1]).expanduser().resolve()
DEFAULT_DB = ROOT / "db" / "wa_pipeline.db"
UNSEEN = ROOT / "scripts" / "whatsapp_unseen.py"


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--db", type=Path, default=DEFAULT_DB)
    p.add_argument("--tenant", required=True)
    args = p.parse_args()

    cmd = [sys.executable, str(UNSEEN), "--db", str(args.db), "--tenant", args.tenant]
    unseen = subprocess.run(cmd, check=False)
    return 0 if unseen.returncode == 0 else 3


if __name__ == "__main__":
    sys.exit(main())
