package config

import (
	"os"
	"path/filepath"
	"testing"
)

func writeConfig(t *testing.T, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestLoadDefaultsAndDefaultDenyWhenDisabled(t *testing.T) {
	path := filepath.Join(t.TempDir(), "groups.json")
	writeConfig(t, path, `{"enabled":false,"groups":[{"jid":"120@g.us","label":"hidden","tenants":["family"]}]}`)
	groups, _, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(groups) != 0 {
		t.Fatalf("disabled config should load no groups: %#v", groups)
	}
	writeConfig(t, path, `{"enabled":true,"groups":[{"jid":"120@g.us","label":"allowed","tenants":["family"]}]}`)
	groups, _, err = Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(groups) != 1 || !groups[0].IngestText || !groups[0].IngestMediaMetadata || groups[0].CopyMedia || groups[0].IndexEmbeddings {
		t.Fatalf("defaults wrong: %#v", groups)
	}
	if len(groups[0].Tenants) != 1 || groups[0].Tenants[0] != "family" {
		t.Fatalf("tenants not loaded: %#v", groups[0].Tenants)
	}
	writeConfig(t, path, `{"enabled":true,"groups":[{"jid":"120@g.us","label":"allowed","tenants":["family","main"]}]}`)
	groups, _, err = Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(groups[0].Tenants) != 2 || groups[0].Tenants[1] != "main" {
		t.Fatalf("tenants not loaded: %#v", groups[0].Tenants)
	}
}

func TestManagerReloadReportsAddedJIDs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "groups.json")
	writeConfig(t, path, `{"enabled":true,"groups":[{"jid":"120@g.us","label":"one","tenants":["family"]}]}`)
	m, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	if !m.IsAllowed("120@g.us") || m.IsAllowed("999@g.us") {
		t.Fatalf("allowlist check failed")
	}
	ch := m.Subscribe()
	writeConfig(t, path, `{"enabled":true,"groups":[{"jid":"120@g.us","label":"one","tenants":["family"]},{"jid":"999@g.us","label":"two","tenants":["family"]}]}`)
	if err := m.Reload(); err != nil {
		t.Fatal(err)
	}
	change := <-ch
	if len(change.Added) != 1 || change.Added[0].JID != "999@g.us" {
		t.Fatalf("unexpected change: %#v", change)
	}
}

func TestLoadRejectsBroadOrAmbiguousConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "groups.json")
	writeConfig(t, path, `{"enabled":true,"groups":[{"jid":"447@s.whatsapp.net","label":"dm","tenants":["family"]}]}`)
	if _, _, err := Load(path); err == nil {
		t.Fatal("expected non-group jid rejection")
	}
	writeConfig(t, path, `{"enabled":true,"groups":[{"jid":"1@g.us","label":"x","tenants":["family"]},{"jid":"1@g.us","label":"y","tenants":["family"]}]}`)
	if _, _, err := Load(path); err == nil {
		t.Fatal("expected duplicate jid rejection")
	}
	writeConfig(t, path, `{"enabled":true,"groups":[{"jid":"1@g.us","label":"x","tenants":[]}]}`)
	if _, _, err := Load(path); err == nil {
		t.Fatal("expected empty tenants rejection")
	}
	writeConfig(t, path, `{"enabled":true,"groups":[{"jid":"1@g.us","label":"x"}]}`)
	if _, _, err := Load(path); err == nil {
		t.Fatal("expected missing tenants rejection")
	}
}
