package auth

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	waHistorySync "go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/proto/waWeb"
	"go.mau.fi/whatsmeow/types/events"

	"openclaw/whatsapp-daemon/internal/config"
	"openclaw/whatsapp-daemon/internal/health"
	"openclaw/whatsapp-daemon/internal/listener"
	"openclaw/whatsapp-daemon/internal/writer"
)

type historySyncStore struct {
	events []writer.Event
}

func TestNormalizePollCreationAndVote(t *testing.T) {
	poll := &waE2E.PollCreationMessage{
		Name: proto.String("Sunday match availability"),
		Options: []*waE2E.PollCreationMessage_Option{
			{OptionName: proto.String("Yes, 09:30 at Crystal Palace")},
			{OptionName: proto.String("No")},
		},
	}
	variants := []*waE2E.Message{
		{PollCreationMessage: poll}, {PollCreationMessageV2: poll}, {PollCreationMessageV3: poll},
		{PollCreationMessageV4: &waE2E.FutureProofMessage{Message: &waE2E.Message{PollCreationMessage: poll}}},
		{PollCreationMessageV5: poll}, {PollCreationMessageV6: poll},
	}
	for _, message := range variants {
		event, _ := normalizeMessage(&events.Message{Message: message})
		if event.MessageType != "poll" || event.Text != "Poll: Sunday match availability\n- Yes, 09:30 at Crystal Palace\n- No" {
			t.Fatalf("poll was not exposed as readable text: %#v", event)
		}
	}
	vote, _ := normalizeMessage(&events.Message{Message: &waE2E.Message{PollUpdateMessage: &waE2E.PollUpdateMessage{}}})
	if vote.MessageType != "poll_vote" || vote.Text != "" {
		t.Fatalf("poll vote should not masquerade as a new schedule: %#v", vote)
	}
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

func TestHandleConnectionEventStreamReplacedMarksHealthDownAndFatal(t *testing.T) {
	healthPath := filepath.Join(t.TempDir(), "health.json")
	status := health.NewStatus(healthPath)
	status.SetConnection(true, true)
	if err := status.Write(); err != nil {
		t.Fatal(err)
	}
	fatal := make(chan error, 1)
	connected := make(chan struct{}, 1)

	if !handleConnectionEvent(Options{Health: status}, &events.StreamReplaced{}, connected, fatal) {
		t.Fatal("StreamReplaced was not handled as a connection event")
	}

	select {
	case err := <-fatal:
		if err == nil || !strings.Contains(err.Error(), "stream replaced") {
			t.Fatalf("unexpected fatal error: %v", err)
		}
	default:
		t.Fatal("StreamReplaced did not publish a fatal error")
	}
	body, err := os.ReadFile(healthPath)
	if err != nil {
		t.Fatal(err)
	}
	var snap struct {
		Connected bool `json:"connected"`
		LoggedIn  bool `json:"logged_in"`
	}
	if err := json.Unmarshal(body, &snap); err != nil {
		t.Fatal(err)
	}
	if snap.Connected || snap.LoggedIn {
		t.Fatalf("health remained up after StreamReplaced: %#v", snap)
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
