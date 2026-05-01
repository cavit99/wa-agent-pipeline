package listener

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"openclaw/whatsapp-daemon/internal/config"
	"openclaw/whatsapp-daemon/internal/writer"
)

type fakeStore struct {
	calls int
	last  writer.Event
}

func (f *fakeStore) Write(_ context.Context, ev writer.Event) (writer.Result, error) {
	f.calls++
	f.last = ev
	return writer.Result{ID: "id", Inserted: true}, nil
}

func TestHandlerDropsDisallowedChats(t *testing.T) {
	cfg := managerFor(t, `{"enabled":true,"groups":[{"jid":"120@g.us","label":"allowed","tenants":["family"]}]}`)
	store := &fakeStore{}
	h := &Handler{Config: cfg, Store: store}
	accepted, err := h.Handle(context.Background(), writer.Event{ChatJID: "999@g.us", MsgID: "A", Timestamp: 1, Text: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	if accepted || store.calls != 0 {
		t.Fatalf("disallowed message was written")
	}
}

func TestHandlerAppliesGroupPolicyBeforeWriting(t *testing.T) {
	cfg := managerFor(t, `{"enabled":true,"groups":[{"jid":"120@g.us","label":"allowed","tenants":["family"],"ingest_text":false,"ingest_media_metadata":false,"copy_media":false}]}`)
	store := &fakeStore{}
	h := &Handler{Config: cfg, Store: store}
	accepted, err := h.Handle(context.Background(), writer.Event{
		ChatJID: "120@g.us", MsgID: "A", Timestamp: 1, Text: "do not store",
		MediaType: "image", MediaTitle: "x.jpg", MediaPath: "/tmp/x", MediaURL: "https://example.invalid/x", MediaSize: 99,
		MediaBytes: []byte("body"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !accepted || store.calls != 1 {
		t.Fatalf("allowed message not written")
	}
	if store.last.ChatLabel != "allowed" || store.last.Text != "" || store.last.MediaType != "" || store.last.MediaTitle != "" || store.last.MediaSize != 0 || store.last.MediaBytes != nil {
		t.Fatalf("policy not applied: %#v", store.last)
	}
}

func managerFor(t *testing.T, body string) *config.Manager {
	t.Helper()
	path := filepath.Join(t.TempDir(), "groups.json")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.New(path)
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}
