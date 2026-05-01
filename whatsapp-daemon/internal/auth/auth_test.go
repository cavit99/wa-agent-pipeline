package auth

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/protobuf/proto"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	waHistorySync "go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/proto/waWeb"
	"go.mau.fi/whatsmeow/types/events"

	"openclaw/whatsapp-daemon/internal/config"
	"openclaw/whatsapp-daemon/internal/listener"
	"openclaw/whatsapp-daemon/internal/writer"
)

type historySyncStore struct {
	events []writer.Event
}

func (s *historySyncStore) Write(_ context.Context, ev writer.Event) (writer.Result, error) {
	s.events = append(s.events, ev)
	return writer.Result{ID: ev.MsgID, Inserted: true}, nil
}

func TestHandleHistorySyncIngestsAllowedConversationsOnly(t *testing.T) {
	cfg := authManagerFor(t, `{"enabled":true,"groups":[{"jid":"120@g.us","label":"allowed","tenants":["family"]}]}`)
	store := &historySyncStore{}
	handler := &listener.Handler{Config: cfg, Store: store}
	syncType := waHistorySync.HistorySync_RECENT
	evt := &events.HistorySync{Data: &waHistorySync.HistorySync{
		SyncType: syncType.Enum(),
		Conversations: []*waHistorySync.Conversation{
			historyConversation("120@g.us", historyTextMessage("120@g.us", "allowed-1", "447@s.whatsapp.net", 1714470000, "allowed history")),
			historyConversation("999@g.us", historyTextMessage("999@g.us", "skipped-1", "448@s.whatsapp.net", 1714470001, "skipped history")),
			nil,
		},
	}}

	stats := handleHistorySync(context.Background(), &whatsmeow.Client{}, Options{Config: cfg, Handler: handler}, evt)

	if stats.Dispatched != 1 || stats.AllowedConversations != 1 || stats.SkippedConversations != 2 || stats.SkippedMessages != 1 || stats.ParseErrors != 0 {
		t.Fatalf("unexpected stats: %#v", stats)
	}
	if len(store.events) != 1 {
		t.Fatalf("unexpected writes: got %d want 1", len(store.events))
	}
	got := store.events[0]
	if got.ChatJID != "120@g.us" || got.ChatLabel != "allowed" || got.MsgID != "allowed-1" ||
		got.SenderJID != "447@s.whatsapp.net" || got.Timestamp != 1714470000 || got.Text != "allowed history" {
		t.Fatalf("unexpected event: %#v", got)
	}
}

func historyConversation(jid string, messages ...*waHistorySync.HistorySyncMsg) *waHistorySync.Conversation {
	return &waHistorySync.Conversation{
		ID:       proto.String(jid),
		Messages: messages,
	}
}

func historyTextMessage(chatJID, msgID, senderJID string, ts uint64, text string) *waHistorySync.HistorySyncMsg {
	return &waHistorySync.HistorySyncMsg{Message: &waWeb.WebMessageInfo{
		Key: &waCommon.MessageKey{
			RemoteJID:   proto.String(chatJID),
			FromMe:      proto.Bool(false),
			ID:          proto.String(msgID),
			Participant: proto.String(senderJID),
		},
		Message: &waE2E.Message{
			Conversation: proto.String(text),
		},
		MessageTimestamp: proto.Uint64(ts),
		Participant:      proto.String(senderJID),
		PushName:         proto.String("sender"),
	}}
}

func authManagerFor(t *testing.T, body string) *config.Manager {
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
