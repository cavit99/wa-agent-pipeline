#!/usr/bin/env python3
import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "whatsapp_unseen.py"

spec = importlib.util.spec_from_file_location("whatsapp_unseen", SCRIPT)
whatsapp_unseen = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(whatsapp_unseen)


def test_tenant_group_jids_filters_by_tenant(tmp_path):
    config = tmp_path / "whatsapp_groups.json"
    config.write_text(json.dumps({
        "enabled": True,
        "groups": [
            {"jid": "family@g.us", "label": "family", "tenants": ["family"]},
            {"jid": "main@g.us", "label": "main", "tenants": ["main"]},
            {"jid": "shared@g.us", "label": "shared", "tenants": ["family", "main"]},
        ],
    }))

    assert whatsapp_unseen.tenant_group_jids(config, "family") == ["family@g.us", "shared@g.us"]
    assert whatsapp_unseen.tenant_group_jids(config, "main") == ["main@g.us", "shared@g.us"]


def test_missing_tenants_is_an_error(tmp_path):
    config = tmp_path / "whatsapp_groups.json"
    config.write_text(json.dumps({
        "enabled": True,
        "groups": [{"jid": "x@g.us", "label": "x"}],
    }))
    with pytest.raises(SystemExit):
        whatsapp_unseen.tenant_group_jids(config, "family")
