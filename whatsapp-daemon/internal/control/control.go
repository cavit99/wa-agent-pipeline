package control

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"go.mau.fi/whatsmeow"

	"openclaw/whatsapp-daemon/internal/auth"
	"openclaw/whatsapp-daemon/internal/backfill"
	"openclaw/whatsapp-daemon/internal/config"
	"openclaw/whatsapp-daemon/internal/health"
	"openclaw/whatsapp-daemon/internal/paths"
)

const (
	OpBackfill = "backfill"

	EventRequestSent         = "request_sent"
	EventHistorySyncReceived = "history_sync_received"
	EventIngested            = "ingested"
	EventDone                = "done"

	VerdictViable      = "VIABLE"
	VerdictUnavailable = "UNAVAILABLE"
	VerdictError       = "ERROR"

	DefaultDialTimeout = 3 * time.Second
)

type Request struct {
	Op          string `json:"op"`
	ChatJID     string `json:"chat_jid"`
	Tenant      string `json:"tenant,omitempty"`
	AnchorMsgID string `json:"anchor_msg_id,omitempty"`
	BeforeUnix  int64  `json:"before_unix,omitempty"`
	Limit       int    `json:"limit,omitempty"`
}

type Event struct {
	Event          string   `json:"event"`
	SendID         string   `json:"send_id,omitempty"`
	Anchor         *Anchor  `json:"anchor,omitempty"`
	Requested      int      `json:"requested,omitempty"`
	Messages       int      `json:"messages,omitempty"`
	MediaEnvelopes int      `json:"media_envelopes,omitempty"`
	Chats          []string `json:"chats,omitempty"`
	RowsInserted   int      `json:"rows_inserted,omitempty"`
	RowsUpdated    int      `json:"rows_updated,omitempty"`
	MediaHydrated  int      `json:"media_hydrated,omitempty"`
	Verdict        string   `json:"verdict,omitempty"`
	Reason         string   `json:"reason,omitempty"`
}

type Anchor struct {
	ChatJID   string `json:"chat_jid"`
	MsgID     string `json:"msg_id"`
	SenderJID string `json:"sender_jid,omitempty"`
	Timestamp int64  `json:"timestamp"`
	FromMe    bool   `json:"from_me"`
}

type Executor interface {
	Execute(context.Context, Request, func(Event) error) (Event, error)
}

type Server struct {
	SocketPath string
	Config     *config.Manager
	Executor   Executor
	Status     *health.Status
	Log        *health.Logger

	listener  *net.UnixListener
	cancel    context.CancelFunc
	closeOnce sync.Once
	wg        sync.WaitGroup
	requestMu sync.Mutex
}

func DefaultSocketPath(home string) string {
	return filepath.Join(paths.Root(home), "whatsapp-daemon", "control.sock")
}

func (s *Server) Start(ctx context.Context) error {
	if strings.TrimSpace(s.SocketPath) == "" {
		return fmt.Errorf("missing control socket path")
	}
	if s.Executor == nil {
		return fmt.Errorf("missing control executor")
	}
	if err := prepareSocketPath(s.SocketPath); err != nil {
		return err
	}
	addr, err := net.ResolveUnixAddr("unix", s.SocketPath)
	if err != nil {
		return err
	}
	ln, err := net.ListenUnix("unix", addr)
	if err != nil {
		return err
	}
	if err := os.Chmod(s.SocketPath, 0o600); err != nil {
		_ = ln.Close()
		_ = os.Remove(s.SocketPath)
		return err
	}
	serveCtx, cancel := context.WithCancel(ctx)
	s.listener = ln
	s.cancel = cancel
	if s.Status != nil {
		s.Status.SetIPCListening(true)
		_ = s.Status.Write()
	}
	if s.Log != nil {
		s.Log.Printf("control_socket_listening path=%s", s.SocketPath)
	}
	s.wg.Add(1)
	go s.acceptLoop(serveCtx)
	go func() {
		<-serveCtx.Done()
		_ = s.Close()
	}()
	return nil
}

func (s *Server) Close() error {
	var err error
	s.closeOnce.Do(func() {
		if s.cancel != nil {
			s.cancel()
		}
		if s.listener != nil {
			err = s.listener.Close()
		}
		_ = os.Remove(s.SocketPath)
		if s.Status != nil {
			s.Status.SetIPCListening(false)
			_ = s.Status.Write()
		}
		s.wg.Wait()
		if s.Log != nil {
			s.Log.Printf("control_socket_closed path=%s", s.SocketPath)
		}
	})
	return err
}

func (s *Server) acceptLoop(ctx context.Context) {
	defer s.wg.Done()
	for {
		conn, err := s.listener.AcceptUnix()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if s.Log != nil {
				s.Log.Printf("control_socket_accept_failed error=%q", err.Error())
			}
			continue
		}
		go func() {
			s.handleConn(ctx, conn)
		}()
	}
}

func (s *Server) handleConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	dec := json.NewDecoder(conn)
	enc := json.NewEncoder(conn)
	var req Request
	if err := dec.Decode(&req); err != nil {
		_ = enc.Encode(doneEvent(VerdictError, "decode request: "+err.Error()))
		return
	}
	_ = conn.SetReadDeadline(time.Time{})
	req = normalizeRequest(req)
	if req.Op != OpBackfill {
		_ = enc.Encode(doneEvent(VerdictError, fmt.Sprintf("unsupported op %q", req.Op)))
		return
	}
	if req.ChatJID == "" {
		_ = enc.Encode(doneEvent(VerdictError, "backfill requires chat_jid"))
		return
	}
	if s.Config == nil {
		_ = enc.Encode(doneEvent(VerdictError, "chat_jid is not in the WhatsApp allowlist"))
		return
	}
	group, ok := s.Config.Group(req.ChatJID)
	if !ok {
		_ = enc.Encode(doneEvent(VerdictError, "chat_jid is not in the WhatsApp allowlist"))
		return
	}
	if req.Tenant != "" && !group.HasTenant(req.Tenant) {
		_ = enc.Encode(doneEvent(VerdictError, "jid_not_owned_by_tenant"))
		return
	}

	s.requestMu.Lock()
	defer s.requestMu.Unlock()
	done, err := s.Executor.Execute(ctx, req, func(evt Event) error {
		return enc.Encode(evt)
	})
	if err != nil && done.Event == "" {
		done = doneEvent(VerdictError, err.Error())
	}
	if done.Event == "" {
		done = doneEvent(VerdictError, "backfill executor returned no verdict")
	}
	_ = enc.Encode(done)
}

func normalizeRequest(req Request) Request {
	req.Op = strings.TrimSpace(req.Op)
	req.ChatJID = strings.TrimSpace(req.ChatJID)
	req.Tenant = strings.TrimSpace(req.Tenant)
	req.AnchorMsgID = strings.TrimSpace(req.AnchorMsgID)
	if req.Limit <= 0 {
		req.Limit = backfill.DefaultChunkSize
	}
	if req.Limit > backfill.DefaultChunkSize {
		req.Limit = backfill.DefaultChunkSize
	}
	return req
}

func prepareSocketPath(path string) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return err
	}
	st, err := os.Lstat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if st.Mode()&os.ModeSocket == 0 {
		return fmt.Errorf("control socket path exists and is not a socket: %s", path)
	}
	conn, dialErr := net.DialTimeout("unix", path, 100*time.Millisecond)
	if dialErr == nil {
		_ = conn.Close()
		return fmt.Errorf("control socket already in use: %s", path)
	}
	return os.Remove(path)
}

type DaemonBackfillExecutor struct {
	DBPath   string
	Client   *whatsmeow.Client
	OnDemand <-chan auth.HistorySyncResult
	Log      *health.Logger
	Now      func() time.Time
	Timeout  time.Duration
}

func (e *DaemonBackfillExecutor) Execute(ctx context.Context, req Request, emit func(Event) error) (Event, error) {
	if e.Client == nil {
		return doneEvent(VerdictError, "daemon WhatsApp client is not ready"), nil
	}
	if e.OnDemand == nil {
		return doneEvent(VerdictError, "daemon history-sync channel is not ready"), nil
	}
	anchor, err := e.resolveAnchor(ctx, req)
	if err != nil {
		return doneEvent(VerdictError, err.Error()), nil
	}
	tracef := backfill.TraceFunc(nil)
	if e.Log != nil {
		tracef = e.Log.Printf
		e.Log.Printf("control_backfill_started chat_jid=%s anchor_msg_id=%s limit=%d", req.ChatJID, anchor.MsgID, req.Limit)
	}
	result, err := backfill.RunProcessedRequests(ctx, e.Client, e.OnDemand, backfill.Request{
		ChatJID:   req.ChatJID,
		Anchor:    anchor,
		Limit:     req.Limit,
		ChunkSize: backfill.DefaultChunkSize,
		Timeout:   e.Timeout,
		Now:       e.Now,
		Tracef:    tracef,
		OnRequestSent: func(sent backfill.RequestSent) error {
			return emit(Event{
				Event:     EventRequestSent,
				SendID:    sent.SendID,
				Anchor:    anchorToWire(sent.Anchor),
				Requested: sent.Count,
			})
		},
		OnHistorySyncReceived: func(summary backfill.Summary) error {
			return emit(Event{
				Event:          EventHistorySyncReceived,
				Messages:       summary.Messages,
				MediaEnvelopes: summary.MediaEnvelopes,
				Chats:          summary.ChatJIDs,
			})
		},
		OnHistorySyncIngested: func(stats auth.HistorySyncStats) error {
			return emit(Event{
				Event:         EventIngested,
				RowsInserted:  stats.RowsInserted,
				RowsUpdated:   stats.RowsUpdated,
				MediaHydrated: stats.MediaHydrated,
			})
		},
	})
	done := verdictForResult(result, err)
	if e.Log != nil {
		e.Log.Printf("control_backfill_finished chat_jid=%s verdict=%s reason=%q requested=%d messages=%d rows_inserted=%d rows_updated=%d media_hydrated=%d",
			req.ChatJID, done.Verdict, done.Reason, result.Requested, result.Summary.Messages,
			result.Stats.RowsInserted, result.Stats.RowsUpdated, result.Stats.MediaHydrated)
	}
	return done, nil
}

func (e *DaemonBackfillExecutor) resolveAnchor(ctx context.Context, req Request) (backfill.Anchor, error) {
	if strings.TrimSpace(e.DBPath) == "" {
		return backfill.Anchor{}, fmt.Errorf("missing message database path")
	}
	if req.AnchorMsgID != "" {
		return backfill.AnchorByMessageID(ctx, e.DBPath, req.ChatJID, req.AnchorMsgID)
	}
	var before time.Time
	if req.BeforeUnix > 0 {
		before = time.Unix(req.BeforeUnix, 0).UTC()
	}
	return backfill.LatestAnchor(ctx, e.DBPath, backfill.AnchorQuery{ChatJID: req.ChatJID, Before: before})
}

func verdictForResult(result backfill.RunResult, err error) Event {
	if err != nil {
		if errors.Is(err, backfill.ErrEnvelopeUnavailable) {
			return doneEvent(VerdictUnavailable, err.Error())
		}
		return doneEvent(VerdictError, err.Error())
	}
	ok, reason := result.Summary.Verdict()
	if ok {
		return doneEvent(VerdictViable, reason)
	}
	return doneEvent(VerdictUnavailable, reason)
}

func doneEvent(verdict, reason string) Event {
	return Event{Event: EventDone, Verdict: verdict, Reason: reason}
}

func anchorToWire(anchor backfill.Anchor) *Anchor {
	return &Anchor{
		ChatJID:   anchor.ChatJID,
		MsgID:     anchor.MsgID,
		SenderJID: anchor.SenderJID,
		Timestamp: anchor.Timestamp,
		FromMe:    anchor.FromMe,
	}
}

type ClientOptions struct {
	SocketPath  string
	Request     Request
	Out         io.Writer
	Human       bool
	DialTimeout time.Duration
	DialContext func(context.Context, string, string) (net.Conn, error)
}

func RunBackfillClient(ctx context.Context, opts ClientOptions) (int, error) {
	if opts.Out == nil {
		opts.Out = io.Discard
	}
	if opts.DialTimeout <= 0 {
		opts.DialTimeout = DefaultDialTimeout
	}
	req := normalizeRequest(opts.Request)
	req.Op = OpBackfill
	dialContext := opts.DialContext
	if dialContext == nil {
		dialer := net.Dialer{Timeout: opts.DialTimeout}
		dialContext = dialer.DialContext
	}
	conn, err := dialContext(ctx, "unix", opts.SocketPath)
	if err != nil {
		return 1, fmt.Errorf("daemon not running or control socket unavailable at %s: %w", opts.SocketPath, err)
	}
	defer conn.Close()
	if err := json.NewEncoder(conn).Encode(req); err != nil {
		return 1, err
	}

	scanner := bufio.NewScanner(conn)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	var final *Event
	for scanner.Scan() {
		line := append([]byte(nil), scanner.Bytes()...)
		var evt Event
		if err := json.Unmarshal(line, &evt); err != nil {
			return 1, fmt.Errorf("decode daemon event: %w", err)
		}
		if opts.Human {
			printHumanEvent(opts.Out, evt)
		} else {
			if _, err := opts.Out.Write(append(line, '\n')); err != nil {
				return 1, err
			}
		}
		if evt.Event == EventDone {
			cp := evt
			final = &cp
		}
	}
	if err := scanner.Err(); err != nil {
		return 1, err
	}
	if final == nil {
		return 1, fmt.Errorf("daemon closed the control connection without a done event")
	}
	return exitCodeForVerdict(final.Verdict), nil
}

func exitCodeForVerdict(verdict string) int {
	switch verdict {
	case VerdictViable:
		return 0
	case VerdictUnavailable:
		return 2
	default:
		return 1
	}
}

func printHumanEvent(out io.Writer, evt Event) {
	switch evt.Event {
	case EventRequestSent:
		fmt.Fprintf(out, "Request sent: send_id=%s requested=%d", evt.SendID, evt.Requested)
		if evt.Anchor != nil {
			fmt.Fprintf(out, " anchor_chat=%s anchor_msg_id=%s anchor_ts=%s",
				evt.Anchor.ChatJID, evt.Anchor.MsgID, time.Unix(evt.Anchor.Timestamp, 0).UTC().Format(time.RFC3339))
		}
		fmt.Fprintln(out)
	case EventHistorySyncReceived:
		fmt.Fprintf(out, "History sync received: messages=%d media_envelopes=%d", evt.Messages, evt.MediaEnvelopes)
		if len(evt.Chats) > 0 {
			fmt.Fprintf(out, " chats=%s", strings.Join(evt.Chats, ","))
		}
		fmt.Fprintln(out)
	case EventIngested:
		fmt.Fprintf(out, "Ingested: rows_inserted=%d rows_updated=%d media_hydrated=%d\n",
			evt.RowsInserted, evt.RowsUpdated, evt.MediaHydrated)
	case EventDone:
		if evt.Reason == "" {
			fmt.Fprintf(out, "Verdict: %s\n", evt.Verdict)
		} else {
			fmt.Fprintf(out, "Verdict: %s (%s)\n", evt.Verdict, evt.Reason)
		}
	default:
		fmt.Fprintf(out, "%s\n", evt.Event)
	}
}
