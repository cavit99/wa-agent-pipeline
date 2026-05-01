package backfill

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"go.mau.fi/whatsmeow"
	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	waHistorySync "go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/proto/waWeb"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"

	"openclaw/whatsapp-daemon/internal/auth"
	"openclaw/whatsapp-daemon/internal/config"
	"openclaw/whatsapp-daemon/internal/sqlite"
)

const (
	DefaultChunkSize       = 50
	DefaultResponseTimeout = 60 * time.Second
	DefaultBackoff         = 2 * time.Second
	DefaultPruneAge        = 30 * 24 * time.Hour

	DefaultSendReadyTimeout        = 10 * time.Second
	DefaultSendReadyStable         = 750 * time.Millisecond
	DefaultSendReadyPollInterval   = 100 * time.Millisecond
	DefaultSendRetryInitialBackoff = 250 * time.Millisecond
	DefaultSendRetryMaxBackoff     = 2 * time.Second
)

var (
	ErrAnchorNotFound      = errors.New("no local anchor message found")
	ErrChatNotAllowed      = errors.New("chat is not in the WhatsApp allowlist")
	ErrEnvelopeUnavailable = errors.New("envelope not available - server-side retention may have pruned it")
)

type Anchor struct {
	ChatJID   string
	MsgID     string
	SenderJID string
	Timestamp int64
	FromMe    bool
}

func (a Anchor) Time() time.Time {
	return time.Unix(a.Timestamp, 0).UTC()
}

func (a Anchor) MessageInfo() (*types.MessageInfo, error) {
	if strings.TrimSpace(a.ChatJID) == "" || strings.TrimSpace(a.MsgID) == "" || a.Timestamp == 0 {
		return nil, fmt.Errorf("anchor missing chat_jid, msg_id, or timestamp")
	}
	chat, err := types.ParseJID(a.ChatJID)
	if err != nil {
		return nil, fmt.Errorf("parse anchor chat jid: %w", err)
	}
	sender := types.EmptyJID
	if strings.TrimSpace(a.SenderJID) != "" {
		sender, err = types.ParseJID(a.SenderJID)
		if err != nil {
			return nil, fmt.Errorf("parse anchor sender jid: %w", err)
		}
	}
	return &types.MessageInfo{
		MessageSource: types.MessageSource{
			Chat:     chat,
			Sender:   sender,
			IsFromMe: a.FromMe,
			IsGroup:  chat.Server == types.GroupServer,
		},
		ID:        types.MessageID(a.MsgID),
		Timestamp: a.Time(),
	}, nil
}

type AnchorQuery struct {
	ChatJID     string
	Before      time.Time
	FallbackAny bool
}

func LatestAnchor(ctx context.Context, dbPath string, q AnchorQuery) (Anchor, error) {
	if err := ctx.Err(); err != nil {
		return Anchor{}, err
	}
	db, err := sqlite.Open(dbPath)
	if err != nil {
		return Anchor{}, err
	}
	defer db.Close()
	anchor, err := latestAnchorFromDB(db, q.ChatJID, q.Before)
	if err == nil {
		return anchor, nil
	}
	if !errors.Is(err, ErrAnchorNotFound) || !q.FallbackAny {
		return Anchor{}, err
	}
	return latestAnchorFromDB(db, "", q.Before)
}

func AnchorByMessageID(ctx context.Context, dbPath, chatJID, msgID string) (Anchor, error) {
	if err := ctx.Err(); err != nil {
		return Anchor{}, err
	}
	chatJID = strings.TrimSpace(chatJID)
	msgID = strings.TrimSpace(msgID)
	if chatJID == "" || msgID == "" {
		return Anchor{}, fmt.Errorf("anchor lookup requires chat_jid and msg_id")
	}
	db, err := sqlite.Open(dbPath)
	if err != nil {
		return Anchor{}, err
	}
	defer db.Close()
	rows, err := db.Query(
		`select chat_jid,msg_id,coalesce(sender_jid,'') as sender_jid,ts,from_me
		  from whatsapp_messages
		  where chat_jid=? and msg_id=?
		  order by ts desc, ingested_at desc, source_pk desc limit 1`,
		chatJID, msgID,
	)
	if err != nil {
		return Anchor{}, err
	}
	if len(rows) == 0 {
		return Anchor{}, ErrAnchorNotFound
	}
	return anchorFromRow(rows[0])
}

func latestAnchorFromDB(db *sqlite.DB, chatJID string, before time.Time) (Anchor, error) {
	var where []string
	var args []any
	if strings.TrimSpace(chatJID) != "" {
		where = append(where, "chat_jid=?")
		args = append(args, chatJID)
	}
	if !before.IsZero() {
		where = append(where, "ts<?")
		args = append(args, before.Unix())
	}
	query := `select chat_jid,msg_id,coalesce(sender_jid,'') as sender_jid,ts,from_me
	  from whatsapp_messages`
	if len(where) > 0 {
		query += " where " + strings.Join(where, " and ")
	}
	query += " order by ts desc, ingested_at desc, source_pk desc limit 1"
	rows, err := db.Query(query, args...)
	if err != nil {
		return Anchor{}, err
	}
	if len(rows) == 0 {
		return Anchor{}, ErrAnchorNotFound
	}
	return anchorFromRow(rows[0])
}

func anchorFromRow(row map[string]any) (Anchor, error) {
	chatJID, _ := row["chat_jid"].(string)
	msgID, _ := row["msg_id"].(string)
	senderJID, _ := row["sender_jid"].(string)
	ts, ok := row["ts"].(int64)
	if !ok {
		return Anchor{}, fmt.Errorf("anchor row has non-integer ts")
	}
	fromMeInt, _ := row["from_me"].(int64)
	a := Anchor{
		ChatJID:   chatJID,
		MsgID:     msgID,
		SenderJID: senderJID,
		Timestamp: ts,
		FromMe:    fromMeInt != 0,
	}
	if _, err := a.MessageInfo(); err != nil {
		return Anchor{}, err
	}
	return a, nil
}

type Summary struct {
	SyncType                 string
	Conversations            int
	Messages                 int
	RequestedChatMessages    int
	MediaEnvelopes           int
	PrunedAgeMediaEnvelopes  int
	MinTimestamp             int64
	MaxTimestamp             int64
	ChatJIDs                 []string
	ContainsRequestedChat    bool
	SameDayBeforeAnchor      bool
	OldestRequestedChat      Anchor
	OldestRequestedChatFound bool
}

func (s Summary) DateRange() string {
	if s.MinTimestamp == 0 || s.MaxTimestamp == 0 {
		return "n/a"
	}
	minTime := time.Unix(s.MinTimestamp, 0).UTC().Format(time.RFC3339)
	maxTime := time.Unix(s.MaxTimestamp, 0).UTC().Format(time.RFC3339)
	if s.MinTimestamp == s.MaxTimestamp {
		return minTime
	}
	return minTime + " to " + maxTime
}

func (s Summary) Verdict() (bool, string) {
	switch {
	case s.Messages == 0:
		return false, "no ON_DEMAND messages returned"
	case s.MediaEnvelopes == 0:
		return false, "no image/document/video media envelopes returned"
	case !s.SameDayBeforeAnchor:
		return false, "no same-day messages before the anchor returned"
	default:
		return true, "ON_DEMAND returned same-day message history with media envelopes"
	}
}

func SummarizeHistorySync(evt *events.HistorySync, anchor Anchor, requestedChat string, now time.Time) Summary {
	s := Summary{SyncType: "unknown"}
	if evt == nil || evt.Data == nil {
		return s
	}
	s.SyncType = evt.Data.GetSyncType().String()
	conversations := evt.Data.GetConversations()
	s.Conversations = len(conversations)
	chatSet := map[string]bool{}
	for _, conv := range conversations {
		if conv == nil {
			continue
		}
		chatJID := conv.GetID()
		if chatJID != "" {
			chatSet[chatJID] = true
		}
		if chatJID == requestedChat {
			s.ContainsRequestedChat = true
		}
		for _, histMsg := range conv.GetMessages() {
			webMsg := historyWebMessage(histMsg)
			if !validWebMessage(webMsg) {
				continue
			}
			ts := int64(webMsg.GetMessageTimestamp())
			s.Messages++
			if s.MinTimestamp == 0 || ts < s.MinTimestamp {
				s.MinTimestamp = ts
			}
			if ts > s.MaxTimestamp {
				s.MaxTimestamp = ts
			}
			if hasMediaEnvelope(webMsg.GetMessage()) {
				s.MediaEnvelopes++
				if !now.IsZero() && now.Sub(time.Unix(ts, 0)) >= DefaultPruneAge {
					s.PrunedAgeMediaEnvelopes++
				}
			}
			if sameUTCDay(ts, anchor.Timestamp) && ts < anchor.Timestamp {
				s.SameDayBeforeAnchor = true
			}
			if chatJID == requestedChat {
				s.RequestedChatMessages++
				if msgAnchor, ok := AnchorFromWebMessage(chatJID, webMsg); ok {
					if !s.OldestRequestedChatFound || msgAnchor.Timestamp < s.OldestRequestedChat.Timestamp {
						s.OldestRequestedChat = msgAnchor
						s.OldestRequestedChatFound = true
					}
				}
			}
		}
	}
	for jid := range chatSet {
		s.ChatJIDs = append(s.ChatJIDs, jid)
	}
	sort.Strings(s.ChatJIDs)
	return s
}

func AnchorFromWebMessage(chatJID string, webMsg *waWeb.WebMessageInfo) (Anchor, bool) {
	if !validWebMessage(webMsg) {
		return Anchor{}, false
	}
	key := webMsg.GetKey()
	if chatJID == "" {
		chatJID = key.GetRemoteJID()
	}
	sender := key.GetParticipant()
	if sender == "" && !key.GetFromMe() {
		sender = key.GetRemoteJID()
	}
	return Anchor{
		ChatJID:   chatJID,
		MsgID:     key.GetID(),
		SenderJID: sender,
		Timestamp: int64(webMsg.GetMessageTimestamp()),
		FromMe:    key.GetFromMe(),
	}, true
}

func IsOnDemand(evt *events.HistorySync) bool {
	return evt != nil &&
		evt.Data != nil &&
		evt.Data.GetSyncType() == waHistorySync.HistorySync_ON_DEMAND
}

type ProcessResult struct {
	Summary Summary
	Stats   auth.HistorySyncStats
}

func ProcessOnDemand(ctx context.Context, client *whatsmeow.Client, opts auth.Options, evt *events.HistorySync, anchor Anchor, now time.Time) (ProcessResult, error) {
	if !IsOnDemand(evt) {
		return ProcessResult{}, fmt.Errorf("ignoring non-ON_DEMAND history sync")
	}
	cfg := configFromOptions(opts)
	requestedChat := anchor.ChatJID
	if cfg != nil && requestedChat != "" && !cfg.IsAllowed(requestedChat) {
		return ProcessResult{}, ErrChatNotAllowed
	}
	summary := SummarizeHistorySync(evt, anchor, requestedChat, now)
	stats := auth.HandleHistorySync(ctx, client, opts, evt)
	return ProcessResult{Summary: summary, Stats: stats}, nil
}

type Request struct {
	ChatJID   string
	Anchor    Anchor
	Limit     int
	ChunkSize int
	Timeout   time.Duration
	Backoff   time.Duration
	Now       func() time.Time
	Tracef    TraceFunc

	OnRequestSent         func(RequestSent) error
	OnHistorySyncReceived func(Summary) error
	OnHistorySyncIngested func(auth.HistorySyncStats) error
}

type RunResult struct {
	Anchor       Anchor
	Requested    int
	Chunks       int
	Summary      Summary
	Stats        auth.HistorySyncStats
	LastSendID   string
	LastResponse time.Time
}

type RequestSent struct {
	SendID string
	Anchor Anchor
	Count  int
}

type peerHistorySyncClient interface {
	BuildHistorySyncRequest(*types.MessageInfo, int) *waE2E.Message
	SendPeerMessage(context.Context, *waE2E.Message) (whatsmeow.SendResponse, error)
	IsConnected() bool
	IsLoggedIn() bool
}

type TraceFunc func(string, ...any)

type sendRetryOptions struct {
	ReadyTimeout   time.Duration
	StableDuration time.Duration
	PollInterval   time.Duration
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	Logf           TraceFunc
}

type connectionSnapshot struct {
	connected bool
	loggedIn  bool
}

func (s connectionSnapshot) ready() bool {
	return s.connected && s.loggedIn
}

func RunProcessedRequests(ctx context.Context, client *whatsmeow.Client, onDemand <-chan auth.HistorySyncResult, req Request) (RunResult, error) {
	if client == nil {
		return RunResult{}, fmt.Errorf("nil whatsmeow client")
	}
	if req.Limit <= 0 {
		return RunResult{}, fmt.Errorf("limit must be positive")
	}
	chunkSize := req.ChunkSize
	if chunkSize <= 0 || chunkSize > DefaultChunkSize {
		chunkSize = DefaultChunkSize
	}
	timeout := requestTimeout(req.Timeout)
	backoff := req.Backoff
	if backoff <= 0 {
		backoff = DefaultBackoff
	}
	now := req.Now
	if now == nil {
		now = time.Now
	}
	anchor := req.Anchor
	if anchor.ChatJID == "" {
		anchor.ChatJID = req.ChatJID
	}
	if _, err := anchor.MessageInfo(); err != nil {
		return RunResult{}, err
	}
	result := RunResult{Anchor: anchor}
	remaining := req.Limit
	for remaining > 0 {
		count := chunkSize
		if remaining < count {
			count = remaining
		}
		tracef := req.Tracef
		sendID, err := sendHistorySyncRequest(ctx, client, anchor, count, defaultSendRetryOptions(tracef))
		if err != nil {
			return result, err
		}
		result.LastSendID = string(sendID)
		result.Requested += count
		result.Chunks++
		if req.OnRequestSent != nil {
			if err := req.OnRequestSent(RequestSent{SendID: string(sendID), Anchor: anchor, Count: count}); err != nil {
				return result, err
			}
		}
		processed, err := WaitForProcessedOnDemand(ctx, onDemand, req.ChatJID, timeout)
		if err != nil {
			return result, err
		}
		summary := SummarizeHistorySync(processed.Event, anchor, req.ChatJID, now())
		result.Summary = MergeSummaries(result.Summary, summary)
		result.Stats = MergeStats(result.Stats, processed.Stats)
		result.LastResponse = now()
		if req.OnHistorySyncReceived != nil {
			if err := req.OnHistorySyncReceived(summary); err != nil {
				return result, err
			}
		}
		if req.OnHistorySyncIngested != nil {
			if err := req.OnHistorySyncIngested(processed.Stats); err != nil {
				return result, err
			}
		}
		if summary.RequestedChatMessages == 0 || !summary.OldestRequestedChatFound {
			return result, ErrEnvelopeUnavailable
		}
		anchor = summary.OldestRequestedChat
		result.Anchor = anchor
		remaining -= count
		if remaining > 0 {
			if err := sleepContext(ctx, backoff); err != nil {
				return result, err
			}
		}
	}
	return result, nil
}

func SendHistorySyncRequest(ctx context.Context, client *whatsmeow.Client, anchor Anchor, count int) (types.MessageID, error) {
	return sendHistorySyncRequest(ctx, client, anchor, count, defaultSendRetryOptions(nil))
}

func sendHistorySyncRequest(ctx context.Context, client peerHistorySyncClient, anchor Anchor, count int, opts sendRetryOptions) (types.MessageID, error) {
	if client == nil {
		return "", fmt.Errorf("nil whatsmeow client")
	}
	if count <= 0 {
		return "", fmt.Errorf("history sync count must be positive")
	}
	if count > DefaultChunkSize {
		count = DefaultChunkSize
	}
	info, err := anchor.MessageInfo()
	if err != nil {
		return "", err
	}

	opts = normalizeSendRetryOptions(opts)
	deadline := time.Now().Add(opts.ReadyTimeout)
	backoff := opts.InitialBackoff
	var lastErr error
	for attempt := 1; ; attempt++ {
		readyTimeout := time.Until(deadline)
		if readyTimeout <= 0 {
			if lastErr != nil {
				return "", fmt.Errorf("history sync peer send not ready after %s: %w", opts.ReadyTimeout, lastErr)
			}
			return "", fmt.Errorf("timed out waiting %s for WhatsApp websocket send readiness", opts.ReadyTimeout)
		}
		if err := waitForPeerSendReady(ctx, client, readyTimeout, opts); err != nil {
			if lastErr != nil {
				return "", fmt.Errorf("%w after transient send failure: %w", err, lastErr)
			}
			return "", err
		}
		if opts.Logf != nil {
			opts.Logf("history_sync_peer_send_attempt attempt=%d connected=%t logged_in=%t", attempt, client.IsConnected(), client.IsLoggedIn())
		}
		resp, err := client.SendPeerMessage(ctx, client.BuildHistorySyncRequest(info, count))
		if err == nil {
			if attempt > 1 && opts.Logf != nil {
				opts.Logf("history_sync_peer_send_recovered attempt=%d id=%s", attempt, resp.ID)
			}
			return resp.ID, nil
		}
		if !isTransientSendConnectionError(err) {
			return "", err
		}
		lastErr = err
		if opts.Logf != nil {
			opts.Logf("history_sync_peer_send_transient_failure attempt=%d connected=%t logged_in=%t error=%q", attempt, client.IsConnected(), client.IsLoggedIn(), err.Error())
		}
		sleep := backoff
		if remaining := time.Until(deadline); sleep > remaining {
			sleep = remaining
		}
		if sleep <= 0 {
			return "", fmt.Errorf("history sync peer send not ready after %s: %w", opts.ReadyTimeout, err)
		}
		if err := sleepContext(ctx, sleep); err != nil {
			return "", err
		}
		backoff *= 2
		if backoff > opts.MaxBackoff {
			backoff = opts.MaxBackoff
		}
	}
}

func WaitForOnDemand(ctx context.Context, onDemand <-chan *events.HistorySync, chatJID string, timeout time.Duration) (*events.HistorySync, error) {
	timer := time.NewTimer(requestTimeout(timeout))
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
			return nil, ErrEnvelopeUnavailable
		case evt, ok := <-onDemand:
			if !ok {
				return nil, ErrEnvelopeUnavailable
			}
			if !IsOnDemand(evt) {
				continue
			}
			if chatJID == "" || historySyncHasConversation(evt, chatJID) {
				return evt, nil
			}
		}
	}
}

func WaitForProcessedOnDemand(ctx context.Context, onDemand <-chan auth.HistorySyncResult, chatJID string, timeout time.Duration) (auth.HistorySyncResult, error) {
	timer := time.NewTimer(requestTimeout(timeout))
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return auth.HistorySyncResult{}, ctx.Err()
		case <-timer.C:
			return auth.HistorySyncResult{}, ErrEnvelopeUnavailable
		case result, ok := <-onDemand:
			if !ok {
				return auth.HistorySyncResult{}, ErrEnvelopeUnavailable
			}
			if !IsOnDemand(result.Event) {
				continue
			}
			if chatJID == "" || historySyncHasConversation(result.Event, chatJID) {
				return result, nil
			}
		}
	}
}

func ParseTimestamp(value string) (time.Time, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, nil
	}
	if secs, err := strconv.ParseInt(value, 10, 64); err == nil {
		return time.Unix(secs, 0).UTC(), nil
	}
	if t, err := time.Parse(time.RFC3339, value); err == nil {
		return t, nil
	}
	if t, err := time.Parse("2006-01-02 15:04:05", value); err == nil {
		return t, nil
	}
	if t, err := time.Parse("2006-01-02", value); err == nil {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("timestamp must be unix seconds, RFC3339, or YYYY-MM-DD")
}

func defaultSendRetryOptions(logf TraceFunc) sendRetryOptions {
	return sendRetryOptions{
		ReadyTimeout:   DefaultSendReadyTimeout,
		StableDuration: DefaultSendReadyStable,
		PollInterval:   DefaultSendReadyPollInterval,
		InitialBackoff: DefaultSendRetryInitialBackoff,
		MaxBackoff:     DefaultSendRetryMaxBackoff,
		Logf:           logf,
	}
}

func normalizeSendRetryOptions(opts sendRetryOptions) sendRetryOptions {
	defaults := defaultSendRetryOptions(opts.Logf)
	if opts.ReadyTimeout <= 0 {
		opts.ReadyTimeout = defaults.ReadyTimeout
	}
	if opts.StableDuration < 0 {
		opts.StableDuration = 0
	}
	if opts.PollInterval <= 0 {
		opts.PollInterval = defaults.PollInterval
	}
	if opts.InitialBackoff <= 0 {
		opts.InitialBackoff = defaults.InitialBackoff
	}
	if opts.MaxBackoff <= 0 {
		opts.MaxBackoff = defaults.MaxBackoff
	}
	if opts.InitialBackoff > opts.MaxBackoff {
		opts.InitialBackoff = opts.MaxBackoff
	}
	return opts
}

func waitForPeerSendReady(ctx context.Context, client peerHistorySyncClient, timeout time.Duration, opts sendRetryOptions) error {
	if client == nil {
		return fmt.Errorf("nil whatsmeow client")
	}
	opts = normalizeSendRetryOptions(opts)
	if timeout <= 0 || timeout > opts.ReadyTimeout {
		timeout = opts.ReadyTimeout
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	ticker := time.NewTicker(opts.PollInterval)
	defer ticker.Stop()

	var last connectionSnapshot
	haveLast := false
	var stableSince time.Time
	for {
		now := time.Now()
		snap := connectionSnapshot{connected: client.IsConnected(), loggedIn: client.IsLoggedIn()}
		if !haveLast || snap != last {
			if opts.Logf != nil {
				opts.Logf("history_sync_peer_send_state connected=%t logged_in=%t", snap.connected, snap.loggedIn)
			}
			haveLast = true
			last = snap
		}
		if snap.ready() {
			if opts.StableDuration == 0 {
				return nil
			}
			if stableSince.IsZero() {
				stableSince = now
			}
			if now.Sub(stableSince) >= opts.StableDuration {
				if opts.Logf != nil {
					opts.Logf("history_sync_peer_send_ready stable_ms=%d", opts.StableDuration.Milliseconds())
				}
				return nil
			}
		} else {
			stableSince = time.Time{}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return fmt.Errorf("timed out waiting %s for WhatsApp websocket send readiness (connected=%t logged_in=%t)", timeout.Round(time.Millisecond), last.connected, last.loggedIn)
		case <-ticker.C:
		}
	}
}

func isTransientSendConnectionError(err error) bool {
	return errors.Is(err, whatsmeow.ErrNotConnected)
}

func MergeStats(a, b auth.HistorySyncStats) auth.HistorySyncStats {
	if a.SyncType == "" {
		a.SyncType = b.SyncType
	}
	a.Conversations += b.Conversations
	a.AllowedConversations += b.AllowedConversations
	a.Messages += b.Messages
	a.Dispatched += b.Dispatched
	a.RowsInserted += b.RowsInserted
	a.RowsUpdated += b.RowsUpdated
	a.MediaHydrated += b.MediaHydrated
	a.SkippedConversations += b.SkippedConversations
	a.SkippedMessages += b.SkippedMessages
	a.ParseErrors += b.ParseErrors
	return a
}

func MergeSummaries(a, b Summary) Summary {
	if a.SyncType == "" {
		a.SyncType = b.SyncType
	}
	a.Conversations += b.Conversations
	a.Messages += b.Messages
	a.RequestedChatMessages += b.RequestedChatMessages
	a.MediaEnvelopes += b.MediaEnvelopes
	a.PrunedAgeMediaEnvelopes += b.PrunedAgeMediaEnvelopes
	if a.MinTimestamp == 0 || (b.MinTimestamp != 0 && b.MinTimestamp < a.MinTimestamp) {
		a.MinTimestamp = b.MinTimestamp
	}
	if b.MaxTimestamp > a.MaxTimestamp {
		a.MaxTimestamp = b.MaxTimestamp
	}
	a.ContainsRequestedChat = a.ContainsRequestedChat || b.ContainsRequestedChat
	a.SameDayBeforeAnchor = a.SameDayBeforeAnchor || b.SameDayBeforeAnchor
	if b.OldestRequestedChatFound &&
		(!a.OldestRequestedChatFound || b.OldestRequestedChat.Timestamp < a.OldestRequestedChat.Timestamp) {
		a.OldestRequestedChat = b.OldestRequestedChat
		a.OldestRequestedChatFound = true
	}
	chatSet := map[string]bool{}
	for _, jid := range a.ChatJIDs {
		chatSet[jid] = true
	}
	for _, jid := range b.ChatJIDs {
		chatSet[jid] = true
	}
	a.ChatJIDs = a.ChatJIDs[:0]
	for jid := range chatSet {
		a.ChatJIDs = append(a.ChatJIDs, jid)
	}
	sort.Strings(a.ChatJIDs)
	return a
}

func configFromOptions(opts auth.Options) *config.Manager {
	if opts.Config != nil {
		return opts.Config
	}
	if opts.Handler != nil {
		return opts.Handler.Config
	}
	return nil
}

func historyWebMessage(histMsg *waHistorySync.HistorySyncMsg) *waWeb.WebMessageInfo {
	if histMsg == nil {
		return nil
	}
	return histMsg.GetMessage()
}

func validWebMessage(webMsg *waWeb.WebMessageInfo) bool {
	return webMsg != nil &&
		webMsg.GetKey() != nil &&
		webMsg.GetKey().GetID() != "" &&
		webMsg.GetMessage() != nil &&
		webMsg.GetMessageTimestamp() != 0
}

func hasMediaEnvelope(msg *waE2E.Message) bool {
	return msg != nil &&
		(msg.GetImageMessage() != nil ||
			msg.GetDocumentMessage() != nil ||
			msg.GetVideoMessage() != nil)
}

func sameUTCDay(a, b int64) bool {
	if a == 0 || b == 0 {
		return false
	}
	ay, am, ad := time.Unix(a, 0).UTC().Date()
	by, bm, bd := time.Unix(b, 0).UTC().Date()
	return ay == by && am == bm && ad == bd
}

func historySyncHasConversation(evt *events.HistorySync, chatJID string) bool {
	if evt == nil || evt.Data == nil {
		return false
	}
	for _, conv := range evt.Data.GetConversations() {
		if conv != nil && conv.GetID() == chatJID {
			return true
		}
	}
	return false
}

func requestTimeout(timeout time.Duration) time.Duration {
	if timeout <= 0 {
		return DefaultResponseTimeout
	}
	return timeout
}

func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
