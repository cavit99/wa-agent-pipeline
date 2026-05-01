package control

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"openclaw/whatsapp-daemon/internal/config"
)

type fakeExecutor struct {
	calls int32
	req   Request
}

func (f *fakeExecutor) Execute(_ context.Context, req Request, emit func(Event) error) (Event, error) {
	atomic.AddInt32(&f.calls, 1)
	f.req = req
	if err := emit(Event{
		Event:     EventRequestSent,
		SendID:    "send-1",
		Requested: req.Limit,
		Anchor: &Anchor{
			ChatJID:   req.ChatJID,
			MsgID:     "anchor-1",
			Timestamp: 1714473600,
		},
	}); err != nil {
		return Event{}, err
	}
	if err := emit(Event{
		Event:          EventHistorySyncReceived,
		Messages:       3,
		MediaEnvelopes: 1,
		Chats:          []string{req.ChatJID},
	}); err != nil {
		return Event{}, err
	}
	if err := emit(Event{Event: EventIngested, RowsInserted: 2, RowsUpdated: 1, MediaHydrated: 1}); err != nil {
		return Event{}, err
	}
	return Event{Event: EventDone, Verdict: VerdictViable, Reason: "ok"}, nil
}

func TestServerAcceptsBackfillRequestAndStreamsJSONEvents(t *testing.T) {
	exec := &fakeExecutor{}
	srv := &Server{
		Config:   controlConfigFor(t, `{"enabled":true,"groups":[{"jid":"120@g.us","label":"allowed","tenants":["family"]}]}`),
		Executor: exec,
	}

	serverConn, conn := net.Pipe()
	defer conn.Close()
	go srv.handleConn(context.Background(), serverConn)
	if err := json.NewEncoder(conn).Encode(Request{Op: OpBackfill, ChatJID: "120@g.us", Tenant: "family", Limit: 99}); err != nil {
		t.Fatal(err)
	}
	dec := json.NewDecoder(conn)
	var events []Event
	for {
		var evt Event
		if err := dec.Decode(&evt); err != nil {
			break
		}
		events = append(events, evt)
		if evt.Event == EventDone {
			break
		}
	}
	if len(events) != 4 {
		t.Fatalf("unexpected events: %#v", events)
	}
	if events[0].Event != EventRequestSent || events[0].SendID != "send-1" || events[0].Anchor == nil {
		t.Fatalf("malformed request_sent event: %#v", events[0])
	}
	if events[1].Event != EventHistorySyncReceived || events[1].Messages != 3 || events[1].MediaEnvelopes != 1 || len(events[1].Chats) != 1 {
		t.Fatalf("malformed history_sync_received event: %#v", events[1])
	}
	if events[2].Event != EventIngested || events[2].RowsInserted != 2 || events[2].RowsUpdated != 1 || events[2].MediaHydrated != 1 {
		t.Fatalf("malformed ingested event: %#v", events[2])
	}
	if events[3].Verdict != VerdictViable {
		t.Fatalf("malformed done event: %#v", events[3])
	}
	if exec.req.Limit != 50 {
		t.Fatalf("limit was not capped: got %d want 50", exec.req.Limit)
	}
	if exec.req.Tenant != "family" {
		t.Fatalf("tenant was not passed through: got %q", exec.req.Tenant)
	}
}

func TestBackfillClientSendsRequestAndParsesVerdict(t *testing.T) {
	gotReq := make(chan Request, 1)
	dial := func(context.Context, string, string) (net.Conn, error) {
		serverConn, clientConn := net.Pipe()
		go func() {
			defer serverConn.Close()
			var req Request
			if err := json.NewDecoder(serverConn).Decode(&req); err == nil {
				gotReq <- req
			}
			enc := json.NewEncoder(serverConn)
			_ = enc.Encode(Event{Event: EventRequestSent, SendID: "send-2", Requested: req.Limit})
			_ = enc.Encode(Event{Event: EventDone, Verdict: VerdictUnavailable, Reason: "no ON_DEMAND messages returned"})
		}()
		return clientConn, nil
	}

	var out bytes.Buffer
	code, err := RunBackfillClient(context.Background(), ClientOptions{
		SocketPath:  "control.sock",
		Request:     Request{ChatJID: "120@g.us", Tenant: "main", BeforeUnix: 1714473600, Limit: 10},
		Out:         &out,
		DialContext: dial,
	})
	if err != nil {
		t.Fatal(err)
	}
	if code != 2 {
		t.Fatalf("unexpected exit code: got %d want 2", code)
	}
	req := <-gotReq
	if req.Op != OpBackfill || req.ChatJID != "120@g.us" || req.Tenant != "main" || req.BeforeUnix != 1714473600 || req.Limit != 10 {
		t.Fatalf("unexpected client request: %#v", req)
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("unexpected client output: %q", out.String())
	}
	var done Event
	if err := json.Unmarshal([]byte(lines[1]), &done); err != nil {
		t.Fatal(err)
	}
	if done.Event != EventDone || done.Verdict != VerdictUnavailable {
		t.Fatalf("unexpected done event: %#v", done)
	}
}

func TestServerRejectsDisallowedBackfillThroughIPC(t *testing.T) {
	exec := &fakeExecutor{}
	srv := &Server{
		Config:   controlConfigFor(t, `{"enabled":true,"groups":[{"jid":"120@g.us","label":"allowed","tenants":["family"]}]}`),
		Executor: exec,
	}

	serverConn, conn := net.Pipe()
	defer conn.Close()
	go srv.handleConn(context.Background(), serverConn)
	if err := json.NewEncoder(conn).Encode(Request{Op: OpBackfill, ChatJID: "999@g.us", Limit: 10}); err != nil {
		t.Fatal(err)
	}
	var evt Event
	if err := json.NewDecoder(conn).Decode(&evt); err != nil {
		t.Fatal(err)
	}
	if evt.Event != EventDone || evt.Verdict != VerdictError || !strings.Contains(evt.Reason, "allowlist") {
		t.Fatalf("unexpected rejection: %#v", evt)
	}
	if atomic.LoadInt32(&exec.calls) != 0 {
		t.Fatalf("executor was called for disallowed request")
	}
}

func TestServerRejectsTenantThatDoesNotOwnBackfillJID(t *testing.T) {
	exec := &fakeExecutor{}
	srv := &Server{
		Config:   controlConfigFor(t, `{"enabled":true,"groups":[{"jid":"120@g.us","label":"allowed","tenants":["family"]}]}`),
		Executor: exec,
	}

	serverConn, conn := net.Pipe()
	defer conn.Close()
	go srv.handleConn(context.Background(), serverConn)
	if err := json.NewEncoder(conn).Encode(Request{Op: OpBackfill, ChatJID: "120@g.us", Tenant: "main", Limit: 10}); err != nil {
		t.Fatal(err)
	}
	var evt Event
	if err := json.NewDecoder(conn).Decode(&evt); err != nil {
		t.Fatal(err)
	}
	if evt.Event != EventDone || evt.Verdict != VerdictError || evt.Reason != "jid_not_owned_by_tenant" {
		t.Fatalf("unexpected rejection: %#v", evt)
	}
	if atomic.LoadInt32(&exec.calls) != 0 {
		t.Fatalf("executor was called for wrong-tenant request")
	}
}

func controlConfigFor(t *testing.T, body string) *config.Manager {
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
