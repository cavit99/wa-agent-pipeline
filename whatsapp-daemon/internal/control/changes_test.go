package control

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"
)

func TestChangesAreSelectedAndCoalesced(t *testing.T) {
	h := &Changes{}
	ch, stop := h.Subscribe("one@g.us")
	h.Notify("other@g.us")
	select {
	case <-ch:
		t.Fatal("wrong chat signalled")
	default:
	}
	for i := 0; i < 100; i++ {
		h.Notify("one@g.us")
	}
	select {
	case <-ch:
	default:
		t.Fatal("commit wake lost")
	}
	select {
	case <-ch:
		t.Fatal("burst was not coalesced")
	default:
	}
	stop()
	h.Notify("one@g.us")
	select {
	case <-ch:
		t.Fatal("retired subscriber signalled")
	default:
	}
}

func TestSubscriptionInstallsBeforeReadyAndChecksTenant(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &Server{Changes: &Changes{}, Config: controlConfigFor(t, `{"enabled":true,"groups":[{"jid":"120@g.us","label":"work","tenants":["main"]}]}`)}
	a, b := net.Pipe()
	defer b.Close()
	_ = b.SetDeadline(time.Now().Add(time.Second))
	done := make(chan struct{})
	go func() { s.handleConn(ctx, a); close(done) }()
	if err := json.NewEncoder(b).Encode(Request{Op: OpWatch, ChatJID: "120@g.us", Tenant: "main"}); err != nil {
		t.Fatal(err)
	}
	dec := json.NewDecoder(b)
	var event Event
	if err := dec.Decode(&event); err != nil || event.Event != "ready" {
		t.Fatalf("missing ready: %#v %v", event, err)
	}
	s.Changes.Notify("120@g.us")
	if err := dec.Decode(&event); err != nil || event.Event != "changed" {
		t.Fatalf("missing changed: %#v %v", event, err)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("watch did not cancel")
	}
}

func TestChangeSubscriptionRejectsCrossTenantAndSharedChats(t *testing.T) {
	for _, tenants := range []string{`["family"]`, `["main","family"]`} {
		s := &Server{Changes: &Changes{}, Config: controlConfigFor(t, `{"enabled":true,"groups":[{"jid":"120@g.us","label":"test","tenants":`+tenants+`}]}`)}
		a, b := net.Pipe()
		_ = b.SetDeadline(time.Now().Add(time.Second))
		go s.handleConn(context.Background(), a)
		_ = json.NewEncoder(b).Encode(Request{Op: OpWatch, ChatJID: "120@g.us", Tenant: "main"})
		var event Event
		err := json.NewDecoder(b).Decode(&event)
		_ = b.Close()
		if err != nil || event.Verdict != VerdictError {
			t.Fatalf("tenant scope escaped: %#v %v", event, err)
		}
	}
}
