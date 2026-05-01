package listener

import (
	"context"
	"time"

	"openclaw/whatsapp-daemon/internal/config"
	"openclaw/whatsapp-daemon/internal/health"
	"openclaw/whatsapp-daemon/internal/writer"
)

type Store interface {
	Write(context.Context, writer.Event) (writer.Result, error)
}

type Handler struct {
	Config *config.Manager
	Store  Store
	Log    *health.Logger
	Status *health.Status
}

func (h *Handler) Handle(ctx context.Context, ev writer.Event) (bool, error) {
	accepted, _, err := h.HandleResult(ctx, ev)
	return accepted, err
}

func (h *Handler) HandleResult(ctx context.Context, ev writer.Event) (bool, writer.Result, error) {
	ev, _, ok := h.ApplyPolicy(ev)
	if !ok {
		return false, writer.Result{}, nil
	}
	res, err := h.Store.Write(ctx, ev)
	if err != nil {
		if len(ev.MediaBytes) > 0 && h.Status != nil {
			h.Status.MediaFailure(time.Now())
		}
		if h.Log != nil {
			h.Log.Printf("message_write_failed chat_jid=%s msg_id=%s error=%q", ev.ChatJID, ev.MsgID, err.Error())
		}
		return true, writer.Result{}, err
	}
	if h.Status != nil {
		h.Status.MessageAt(time.Now())
	}
	if h.Log != nil {
		h.Log.Printf("message_ingested chat_jid=%s msg_id=%s db_id=%s inserted=%t updated=%t media=%t",
			ev.ChatJID, ev.MsgID, res.ID, res.Inserted, res.Updated, res.MediaPath != "")
	}
	return true, res, nil
}

func (h *Handler) ApplyPolicy(ev writer.Event) (writer.Event, config.Group, bool) {
	g, ok := h.Config.Group(ev.ChatJID)
	if !ok {
		if h.Log != nil {
			h.Log.Printf("message_dropped_disallowed chat_jid=%s msg_id=%s", ev.ChatJID, ev.MsgID)
		}
		return ev, config.Group{}, false
	}
	ev.ChatLabel = g.Label
	if !g.IngestText {
		ev.Text = ""
	}
	if !g.IngestMediaMetadata {
		ev.MediaType = ""
		ev.MediaTitle = ""
		ev.MediaPath = ""
		ev.MediaURL = ""
		ev.MediaSize = 0
	}
	if !g.CopyMedia {
		ev.MediaBytes = nil
	}
	return ev, g, true
}
