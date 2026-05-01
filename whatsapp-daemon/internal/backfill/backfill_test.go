package backfill

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waCommon"
	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	waHistorySync "go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/proto/waWeb"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"

	"openclaw/whatsapp-daemon/internal/auth"
	"openclaw/whatsapp-daemon/internal/config"
	"openclaw/whatsapp-daemon/internal/listener"
	"openclaw/whatsapp-daemon/internal/writer"
)

type captureStore struct {
	events []writer.Event
}

func (s *captureStore) Write(_ context.Context, ev writer.Event) (writer.Result, error) {
	s.events = append(s.events, ev)
	return writer.Result{ID: ev.MsgID, Inserted: true}, nil
}

func TestProcessOnDemandIngestsSyntheticAnchorHistory(t *testing.T) {
	cfg := configFor(t, `{"enabled":true,"groups":[{"jid":"120@g.us","label":"allowed","tenants":["family"],"ingest_media_metadata":true,"copy_media":false}]}`)
	store := &captureStore{}
	handler := &listener.Handler{Config: cfg, Store: store}
	anchor := Anchor{
		ChatJID:   "120@g.us",
		MsgID:     "anchor-1",
		SenderJID: "447@s.whatsapp.net",
		Timestamp: 1714473600,
	}
	evt := &events.HistorySync{Data: &waHistorySync.HistorySync{
		SyncType: waHistorySync.HistorySync_ON_DEMAND.Enum(),
		Conversations: []*waHistorySync.Conversation{
			historyConversation("120@g.us", historyImageMessage("120@g.us", "older-image", "447@s.whatsapp.net", 1714470000, "photo caption")),
		},
	}}

	got, err := ProcessOnDemand(context.Background(), &whatsmeow.Client{}, auth.Options{Config: cfg, Handler: handler}, evt, anchor, time.Unix(1714477200, 0))
	if err != nil {
		t.Fatal(err)
	}
	if got.Summary.SyncType != "ON_DEMAND" || got.Summary.Messages != 1 || got.Summary.MediaEnvelopes != 1 ||
		got.Summary.RequestedChatMessages != 1 || !got.Summary.SameDayBeforeAnchor {
		t.Fatalf("unexpected summary: %#v", got.Summary)
	}
	if got.Stats.Dispatched != 1 || got.Stats.ParseErrors != 0 {
		t.Fatalf("unexpected stats: %#v", got.Stats)
	}
	if len(store.events) != 1 {
		t.Fatalf("unexpected writes: got %d want 1", len(store.events))
	}
	ev := store.events[0]
	if ev.ChatJID != "120@g.us" || ev.ChatLabel != "allowed" || ev.MsgID != "older-image" ||
		ev.MessageType != "image" || ev.MediaType != "image" || ev.MediaTitle != "" || ev.Text != "photo caption" {
		t.Fatalf("unexpected event: %#v", ev)
	}
}

func TestSendHistorySyncRequestRetriesErrNotConnected(t *testing.T) {
	client := &flakyPeerHistoryClient{connected: true, loggedIn: true}
	anchor := Anchor{
		ChatJID:   "120@g.us",
		MsgID:     "anchor-1",
		SenderJID: "447@s.whatsapp.net",
		Timestamp: 1714473600,
	}

	got, err := sendHistorySyncRequest(context.Background(), client, anchor, 50, sendRetryOptions{
		ReadyTimeout:   time.Second,
		StableDuration: -1,
		PollInterval:   time.Millisecond,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got != "retry-ok" {
		t.Fatalf("unexpected send id: got %q want retry-ok", got)
	}
	if client.sendCalls != 2 {
		t.Fatalf("unexpected send calls: got %d want 2", client.sendCalls)
	}
	if client.buildCalls != 2 {
		t.Fatalf("unexpected build calls: got %d want 2", client.buildCalls)
	}
	if client.lastCount != 50 || client.lastChat != "120@g.us" || client.lastMessageID != "anchor-1" {
		t.Fatalf("unexpected request anchor: count=%d chat=%s msg=%s", client.lastCount, client.lastChat, client.lastMessageID)
	}
}

type flakyPeerHistoryClient struct {
	connected     bool
	loggedIn      bool
	buildCalls    int
	sendCalls     int
	lastCount     int
	lastChat      string
	lastMessageID types.MessageID
}

func (c *flakyPeerHistoryClient) BuildHistorySyncRequest(info *types.MessageInfo, count int) *waE2E.Message {
	c.buildCalls++
	c.lastCount = count
	c.lastChat = info.Chat.String()
	c.lastMessageID = info.ID
	return (&whatsmeow.Client{}).BuildHistorySyncRequest(info, count)
}

func (c *flakyPeerHistoryClient) SendPeerMessage(context.Context, *waE2E.Message) (whatsmeow.SendResponse, error) {
	c.sendCalls++
	if c.sendCalls == 1 {
		return whatsmeow.SendResponse{}, whatsmeow.ErrNotConnected
	}
	return whatsmeow.SendResponse{ID: types.MessageID("retry-ok")}, nil
}

func (c *flakyPeerHistoryClient) IsConnected() bool {
	return c.connected
}

func (c *flakyPeerHistoryClient) IsLoggedIn() bool {
	return c.loggedIn
}

func historyConversation(jid string, messages ...*waHistorySync.HistorySyncMsg) *waHistorySync.Conversation {
	return &waHistorySync.Conversation{
		ID:       proto.String(jid),
		Messages: messages,
	}
}

func historyImageMessage(chatJID, msgID, senderJID string, ts uint64, caption string) *waHistorySync.HistorySyncMsg {
	return &waHistorySync.HistorySyncMsg{Message: &waWeb.WebMessageInfo{
		Key: &waCommon.MessageKey{
			RemoteJID:   proto.String(chatJID),
			FromMe:      proto.Bool(false),
			ID:          proto.String(msgID),
			Participant: proto.String(senderJID),
		},
		Message: &waE2E.Message{
			ImageMessage: &waE2E.ImageMessage{
				URL:        proto.String("https://mmg.whatsapp.net/test"),
				Mimetype:   proto.String("image/jpeg"),
				Caption:    proto.String(caption),
				FileLength: proto.Uint64(1234),
			},
		},
		MessageTimestamp: proto.Uint64(ts),
		Participant:      proto.String(senderJID),
		PushName:         proto.String("sender"),
	}}
}

func configFor(t *testing.T, body string) *config.Manager {
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
