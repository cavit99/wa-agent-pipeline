package control

import (
	"context"
	"encoding/json"
	"net"
	"sync"
	"time"
)

// Changes carries coalesced wake-ups, not messages. SQLite remains authoritative;
// subscribers reconcile on ready/reconnect and ordinary startup/hourly catch-up.
type Changes struct {
	mu   sync.Mutex
	subs map[chan struct{}]string
}

func (c *Changes) Subscribe(jid string) (<-chan struct{}, func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.subs == nil {
		c.subs = make(map[chan struct{}]string)
	}
	ch := make(chan struct{}, 1)
	c.subs[ch] = jid
	return ch, func() { c.mu.Lock(); delete(c.subs, ch); c.mu.Unlock() }
}

func (c *Changes) Notify(jid string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for ch, selected := range c.subs {
		if selected == jid {
			select {
			case ch <- struct{}{}:
			default:
			}
		}
	}
}

func (s *Server) watchChanges(ctx context.Context, conn net.Conn, req Request, enc *json.Encoder) {
	if s.Changes == nil || req.Tenant == "" {
		_ = enc.Encode(doneEvent(VerdictError, "change subscription unavailable"))
		return
	}
	ch, unsubscribe := s.Changes.Subscribe(req.ChatJID)
	defer unsubscribe()
	disconnected := make(chan struct{})
	go func() { var next [1]byte; _, _ = conn.Read(next[:]); close(disconnected) }()
	// The live subscription is installed BEFORE ready; a concurrent commit is
	// either observed by the ready snapshot or retained in the coalesced wake.
	_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if err := enc.Encode(Event{Event: "ready", Chats: []string{req.ChatJID}}); err != nil {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-disconnected:
			return
		case <-ch:
			group, ok := s.Config.Group(req.ChatJID)
			if !ok || len(group.Tenants) != 1 || !group.HasTenant(req.Tenant) {
				return
			}
			_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if err := enc.Encode(Event{Event: "changed", Chats: []string{req.ChatJID}}); err != nil {
				return
			}
		}
	}
}
