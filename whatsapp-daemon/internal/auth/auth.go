package auth

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/mdp/qrterminal/v3"
	"go.mau.fi/whatsmeow"
	waHistorySync "go.mau.fi/whatsmeow/proto/waHistorySync"
	waWeb "go.mau.fi/whatsmeow/proto/waWeb"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"

	"openclaw/whatsapp-daemon/internal/config"
	"openclaw/whatsapp-daemon/internal/health"
	"openclaw/whatsapp-daemon/internal/listener"
	"openclaw/whatsapp-daemon/internal/writer"
)

const messageQueueSize = 128

type Options struct {
	AuthDB  string
	Config  *config.Manager
	Handler *listener.Handler
	Health  *health.Status
	Log     *health.Logger

	ServeReady func(context.Context, ServeState) (io.Closer, error)
}

type ServeState struct {
	Client            *whatsmeow.Client
	OnDemandProcessed <-chan HistorySyncResult
}

func Pair(ctx context.Context, opts Options) error {
	bundle, err := openClient(ctx, opts)
	if err != nil {
		return err
	}
	defer bundle.Container.Close()
	if bundle.Client.Store.ID != nil {
		fmt.Fprintln(os.Stdout, "WhatsApp device is already paired.")
		if opts.Log != nil {
			opts.Log.Printf("pair_skipped already_paired=true jid=%s", bundle.Client.Store.ID)
		}
		return nil
	}

	pairCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	// settled fires when the post-pair handshake (app-state sync) is far
	// enough along that WhatsApp's server considers the device live.
	// Disconnecting before that point causes the server to roll back the
	// device registration ("linking failed" / "device_removed" on phone).
	settled := make(chan struct{}, 1)
	handlerID := bundle.Client.AddEventHandler(func(raw any) {
		switch raw.(type) {
		case *events.AppStateSyncComplete:
			select {
			case settled <- struct{}{}:
			default:
			}
		}
	})
	defer bundle.Client.RemoveEventHandler(handlerID)

	qrChan, err := bundle.Client.GetQRChannel(pairCtx)
	if err != nil {
		return err
	}
	if err := bundle.Client.ConnectContext(pairCtx); err != nil {
		return err
	}
	defer bundle.Client.Disconnect()

	for {
		select {
		case <-pairCtx.Done():
			return fmt.Errorf("pairing timed out: %w", pairCtx.Err())
		case qr, ok := <-qrChan:
			if !ok {
				return fmt.Errorf("pairing QR channel closed before success")
			}
			switch qr.Event {
			case whatsmeow.QRChannelEventCode:
				fmt.Fprintf(os.Stdout, "\nScan this QR code with WhatsApp. It expires in %s.\n\n", qr.Timeout.Round(time.Second))
				qrterminal.GenerateHalfBlock(qr.Code, qrterminal.L, os.Stdout)
			case whatsmeow.QRChannelEventError:
				return fmt.Errorf("pairing failed: %w", qr.Error)
			case whatsmeow.QRChannelSuccess.Event:
				fmt.Fprintln(os.Stdout, "Pair acknowledged. Waiting for post-pair sync to complete (do NOT close WhatsApp on your phone yet)...")
				if opts.Log != nil {
					opts.Log.Printf("pair_acknowledged")
				}
				// Keep the websocket alive long enough for the post-pair
				// handshake. Exit early on AppStateSyncComplete (the
				// happy path), or fall through to a 90s ceiling if no
				// such event arrives — 90s is empirically long enough
				// for WhatsApp to commit the device registration.
				settleCtx, cancelSettle := context.WithTimeout(pairCtx, 90*time.Second)
				select {
				case <-settled:
					cancelSettle()
					fmt.Fprintln(os.Stdout, "WhatsApp pairing succeeded; device is live.")
					if opts.Log != nil {
						opts.Log.Printf("pair_settled reason=app_state_sync_complete")
					}
				case <-settleCtx.Done():
					cancelSettle()
					fmt.Fprintln(os.Stdout, "WhatsApp pairing succeeded; settle window elapsed without explicit sync event (device should still be registered).")
					if opts.Log != nil {
						opts.Log.Printf("pair_settled reason=timeout duration_s=90")
					}
				}
				return nil
			default:
				return fmt.Errorf("pairing ended with event %q", qr.Event)
			}
		}
	}
}

func Serve(ctx context.Context, opts Options) error {
	if opts.Handler == nil {
		return fmt.Errorf("missing listener handler")
	}
	bundle, err := openClient(ctx, opts)
	if err != nil {
		return err
	}
	defer bundle.Container.Close()
	if bundle.Client.Store.ID == nil {
		return fmt.Errorf("WhatsApp is not paired; run whatsapp-daemon pair first")
	}
	if opts.Health != nil {
		opts.Health.SetConnected(false)
		_ = opts.Health.Write()
	}

	serveCtx, cancel := context.WithCancel(ctx)
	messages := make(chan *events.Message, messageQueueSize)
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		for {
			select {
			case <-serveCtx.Done():
				return
			case msg := <-messages:
				if opts.Health != nil {
					opts.Health.SetQueueDepth(len(messages))
				}
				handleMessage(serveCtx, bundle.Client, opts, msg)
				if opts.Health != nil {
					opts.Health.SetQueueDepth(len(messages))
				}
			}
		}
	}()

	fatal := make(chan error, 1)
	connected := make(chan struct{}, 1)
	onDemandProcessed := make(chan HistorySyncResult, 8)
	var historySyncs sync.WaitGroup
	var historySyncMu sync.Mutex
	historySyncClosed := false
	handlerID := bundle.Client.AddEventHandler(func(raw any) {
		switch evt := raw.(type) {
		case *events.Message:
			select {
			case messages <- evt:
				if opts.Health != nil {
					opts.Health.SetQueueDepth(len(messages))
				}
			default:
				if opts.Log != nil {
					opts.Log.Printf("message_queue_full chat_jid=%s msg_id=%s", evt.Info.Chat.String(), evt.Info.ID)
				}
			}
		case *events.Connected:
			setConnected(opts, true, "connected")
			select {
			case connected <- struct{}{}:
			default:
			}
		case *events.Disconnected:
			setConnected(opts, false, "disconnected")
		case *events.LoggedOut:
			setConnected(opts, false, "logged_out")
			select {
			case fatal <- fmt.Errorf("WhatsApp logged out: %s", evt.Reason.String()):
			default:
			}
		case *events.PairSuccess:
			if opts.Log != nil {
				opts.Log.Printf("pair_success_event jid=%s lid=%s platform=%q", evt.ID, evt.LID, evt.Platform)
			}
		case *events.HistorySync:
			if opts.Log != nil {
				opts.Log.Printf("history_sync_received sync_type=%s conversations=%d", historySyncType(evt), historySyncConversationCount(evt))
			}
			historySyncMu.Lock()
			if historySyncClosed {
				historySyncMu.Unlock()
				return
			}
			historySyncs.Add(1)
			historySyncMu.Unlock()
			go func() {
				defer historySyncs.Done()
				stats := handleHistorySync(serveCtx, bundle.Client, opts, evt)
				if opts.Log != nil {
					opts.Log.Printf(
						"history_sync_processed sync_type=%s conversations=%d allowed_conversations=%d messages=%d dispatched=%d rows_inserted=%d rows_updated=%d media_hydrated=%d skipped_conversations=%d skipped_messages=%d parse_errors=%d",
						stats.SyncType, stats.Conversations, stats.AllowedConversations, stats.Messages, stats.Dispatched,
						stats.RowsInserted, stats.RowsUpdated, stats.MediaHydrated, stats.SkippedConversations, stats.SkippedMessages, stats.ParseErrors,
					)
				}
				if isOnDemandHistorySync(evt) {
					result := HistorySyncResult{Event: evt, Stats: stats, ProcessedAt: time.Now()}
					select {
					case onDemandProcessed <- result:
					default:
						if opts.Log != nil {
							opts.Log.Printf("history_sync_on_demand_result_dropped reason=channel_full")
						}
					}
				}
			}()
		}
	})
	defer func() {
		bundle.Client.RemoveEventHandler(handlerID)
		bundle.Client.Disconnect()
		historySyncMu.Lock()
		historySyncClosed = true
		historySyncMu.Unlock()
		cancel()
		historySyncs.Wait()
		close(onDemandProcessed)
		<-workerDone
	}()

	if err := bundle.Client.ConnectContext(ctx); err != nil {
		return err
	}
	if err := waitForClientReady(ctx, bundle.Client, connected, 30*time.Second); err != nil {
		return err
	}
	if opts.ServeReady != nil {
		readyCloser, err := opts.ServeReady(serveCtx, ServeState{
			Client:            bundle.Client,
			OnDemandProcessed: onDemandProcessed,
		})
		if err != nil {
			return err
		}
		if readyCloser != nil {
			defer readyCloser.Close()
		}
	}
	if opts.Log != nil {
		opts.Log.Printf("serve_started jid=%s", bundle.Client.Store.ID)
	}

	select {
	case <-ctx.Done():
		return nil
	case err := <-fatal:
		return err
	}
}

type clientBundle struct {
	Client    *whatsmeow.Client
	Container *sqlstore.Container
}

func openClient(ctx context.Context, opts Options) (*clientBundle, error) {
	if strings.TrimSpace(opts.AuthDB) == "" {
		return nil, fmt.Errorf("missing auth db path")
	}
	authDB, err := filepath.Abs(opts.AuthDB)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(authDB), 0o700); err != nil {
		return nil, err
	}
	container, err := sqlstore.New(ctx, "sqlite3", sqliteDSN(authDB), whatsmeowLogger{base: opts.Log, module: "whatsmeow"})
	if err != nil {
		return nil, err
	}
	device, err := container.GetFirstDevice(ctx)
	if err != nil {
		_ = container.Close()
		return nil, err
	}
	client := whatsmeow.NewClient(device, whatsmeowLogger{base: opts.Log, module: "whatsmeow"})
	return &clientBundle{Client: client, Container: container}, nil
}

func sqliteDSN(path string) string {
	return "file:" + path + "?_foreign_keys=on&_busy_timeout=5000"
}

func setConnected(opts Options, connected bool, state string) {
	if opts.Health != nil {
		opts.Health.SetConnected(connected)
		_ = opts.Health.Write()
	}
	health.LogConnection(opts.Log, state)
}

func waitForClientReady(ctx context.Context, client *whatsmeow.Client, connected <-chan struct{}, timeout time.Duration) error {
	if client == nil {
		return fmt.Errorf("nil whatsmeow client")
	}
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if client.IsConnected() && client.IsLoggedIn() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-connected:
		case <-ticker.C:
		case <-timer.C:
			return fmt.Errorf("timed out waiting %s for WhatsApp serve readiness (connected=%t logged_in=%t)", timeout.Round(time.Millisecond), client.IsConnected(), client.IsLoggedIn())
		}
	}
}

func isOnDemandHistorySync(evt *events.HistorySync) bool {
	return evt != nil &&
		evt.Data != nil &&
		evt.Data.GetSyncType() == waHistorySync.HistorySync_ON_DEMAND
}

type historySyncStats struct {
	SyncType             string
	Conversations        int
	AllowedConversations int
	Messages             int
	Dispatched           int
	RowsInserted         int
	RowsUpdated          int
	MediaHydrated        int
	SkippedConversations int
	SkippedMessages      int
	ParseErrors          int
}

type HistorySyncStats = historySyncStats

type HistorySyncResult struct {
	Event       *events.HistorySync
	Stats       HistorySyncStats
	ProcessedAt time.Time
}

func HandleHistorySync(ctx context.Context, client *whatsmeow.Client, opts Options, evt *events.HistorySync) HistorySyncStats {
	return handleHistorySync(ctx, client, opts, evt)
}

func handleHistorySync(ctx context.Context, client *whatsmeow.Client, opts Options, evt *events.HistorySync) historySyncStats {
	stats := historySyncStats{SyncType: historySyncType(evt)}
	if evt == nil || evt.Data == nil {
		return stats
	}
	conversations := evt.Data.GetConversations()
	stats.Conversations = len(conversations)
	if len(conversations) == 0 {
		return stats
	}
	cfg := historySyncConfig(opts)
	if cfg == nil {
		stats.SkippedConversations = len(conversations)
		for _, conv := range conversations {
			stats.SkippedMessages += len(historySyncMessages(conv))
		}
		return stats
	}
	for _, conv := range conversations {
		if err := ctx.Err(); err != nil {
			return stats
		}
		if conv == nil {
			stats.SkippedConversations++
			continue
		}
		chatJID, ok := parseHistorySyncChatJID(conv, opts)
		if !ok {
			stats.SkippedConversations++
			stats.SkippedMessages += len(conv.GetMessages())
			continue
		}
		if !cfg.IsAllowed(chatJID.String()) {
			stats.SkippedConversations++
			stats.SkippedMessages += len(conv.GetMessages())
			continue
		}
		stats.AllowedConversations++
		for _, histMsg := range conv.GetMessages() {
			if err := ctx.Err(); err != nil {
				return stats
			}
			stats.Messages++
			webMsg := historySyncWebMessage(histMsg)
			if !validHistorySyncWebMessage(webMsg) {
				stats.SkippedMessages++
				continue
			}
			msg, err := parseHistorySyncMessage(client, chatJID, webMsg)
			if err != nil {
				stats.ParseErrors++
				if opts.Log != nil {
					opts.Log.Printf("history_sync_parse_failed chat_jid=%s msg_id=%s error=%q", chatJID.String(), webMsg.GetKey().GetID(), err.Error())
				}
				continue
			}
			if msg == nil {
				stats.SkippedMessages++
				continue
			}
			outcome := handleMessageFrom(ctx, client, opts, msg, messageSourceHistorySync)
			stats.Dispatched++
			if outcome.Result.Inserted {
				stats.RowsInserted++
			}
			if outcome.Result.Updated {
				stats.RowsUpdated++
			}
			if outcome.Result.MediaPath != "" {
				stats.MediaHydrated++
			}
		}
	}
	return stats
}

func historySyncConfig(opts Options) *config.Manager {
	if opts.Config != nil {
		return opts.Config
	}
	if opts.Handler != nil {
		return opts.Handler.Config
	}
	return nil
}

func historySyncType(evt *events.HistorySync) string {
	if evt == nil || evt.Data == nil {
		return "unknown"
	}
	return evt.Data.GetSyncType().String()
}

func historySyncConversationCount(evt *events.HistorySync) int {
	if evt == nil || evt.Data == nil {
		return 0
	}
	return len(evt.Data.GetConversations())
}

func historySyncMessages(conv *waHistorySync.Conversation) []*waHistorySync.HistorySyncMsg {
	if conv == nil {
		return nil
	}
	return conv.GetMessages()
}

func historySyncWebMessage(histMsg *waHistorySync.HistorySyncMsg) *waWeb.WebMessageInfo {
	if histMsg == nil {
		return nil
	}
	return histMsg.GetMessage()
}

func parseHistorySyncMessage(client *whatsmeow.Client, chatJID types.JID, webMsg *waWeb.WebMessageInfo) (msg *events.Message, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("panic parsing history sync message: %v", recovered)
			msg = nil
		}
	}()
	if client == nil {
		return nil, fmt.Errorf("nil whatsmeow client")
	}
	return client.ParseWebMessage(chatJID, webMsg)
}

func parseHistorySyncChatJID(conv *waHistorySync.Conversation, opts Options) (types.JID, bool) {
	raw := strings.TrimSpace(conv.GetID())
	if raw == "" {
		return types.EmptyJID, false
	}
	chatJID, err := types.ParseJID(raw)
	if err != nil {
		if opts.Log != nil {
			opts.Log.Printf("history_sync_bad_chat_jid chat_jid=%q error=%q", raw, err.Error())
		}
		return types.EmptyJID, false
	}
	return chatJID, true
}

func validHistorySyncWebMessage(webMsg *waWeb.WebMessageInfo) bool {
	return webMsg != nil &&
		webMsg.GetKey() != nil &&
		webMsg.GetKey().GetID() != "" &&
		webMsg.GetMessage() != nil &&
		webMsg.GetMessageTimestamp() != 0
}

type messageSource int

const (
	messageSourceLive messageSource = iota
	messageSourceHistorySync
)

func (s messageSource) String() string {
	switch s {
	case messageSourceHistorySync:
		return "history_sync"
	default:
		return "live"
	}
}

type messageHandleOutcome struct {
	Accepted bool
	Result   writer.Result
	Err      error
}

func handleMessage(ctx context.Context, client *whatsmeow.Client, opts Options, msg *events.Message) {
	handleMessageFrom(ctx, client, opts, msg, messageSourceLive)
}

func handleMessageFrom(ctx context.Context, client *whatsmeow.Client, opts Options, msg *events.Message, source messageSource) messageHandleOutcome {
	ev, media := normalizeMessage(msg)
	policyEv, group, ok := opts.Handler.ApplyPolicy(ev)
	if !ok {
		return messageHandleOutcome{}
	}
	if group.CopyMedia && media != nil {
		body, err := client.Download(ctx, media)
		if err != nil {
			health.LogMediaFailure(opts.Log, ev.ChatJID, ev.MsgID, err)
			if opts.Health != nil {
				opts.Health.MediaFailure(time.Now())
			}
			if source == messageSourceHistorySync && isExpiredMediaDownload(err) {
				policyEv.MediaHydrationStatus = "failed: expired"
				policyEv.MediaHydrationAttempts = 1
				if opts.Log != nil {
					opts.Log.Printf("history_sync_media_expired chat_jid=%s msg_id=%s", ev.ChatJID, ev.MsgID)
				}
			}
		} else {
			policyEv.MediaBytes = body
			if opts.Log != nil {
				opts.Log.Printf("media_downloaded chat_jid=%s msg_id=%s source=%s bytes=%d", ev.ChatJID, ev.MsgID, source.String(), len(body))
			}
		}
	}
	accepted, res, err := opts.Handler.HandleResult(ctx, policyEv)
	if err != nil && opts.Log != nil {
		opts.Log.Printf("message_handle_failed chat_jid=%s msg_id=%s error=%q", ev.ChatJID, ev.MsgID, err.Error())
	}
	return messageHandleOutcome{Accepted: accepted, Result: res, Err: err}
}

func isExpiredMediaDownload(err error) bool {
	return errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith404) || errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith410)
}

func normalizeMessage(evt *events.Message) (writer.Event, whatsmeow.DownloadableMessage) {
	info := evt.Info
	ev := writer.Event{
		ChatJID:    info.Chat.String(),
		MsgID:      string(info.ID),
		SenderJID:  info.Sender.String(),
		SenderName: info.PushName,
		Timestamp:  info.Timestamp.Unix(),
		FromMe:     info.IsFromMe,
		RawType:    0,
	}
	if ev.Timestamp == 0 {
		ev.Timestamp = time.Now().Unix()
	}
	msg := evt.Message
	if msg == nil {
		ev.MessageType = fallbackMessageType(info.Type)
		return ev, nil
	}

	ev.Text = firstText(msg.GetConversation())
	if ext := msg.GetExtendedTextMessage(); ext != nil {
		ev.Text = firstText(ev.Text, ext.GetText())
		ev.MessageType = "text"
	}

	switch {
	case msg.GetImageMessage() != nil:
		img := msg.GetImageMessage()
		ev.Text = firstText(img.GetCaption(), ev.Text)
		setMedia(&ev, "image", img.GetMimetype(), "", img.GetURL(), img.GetFileLength())
		return ev, img
	case msg.GetDocumentMessage() != nil:
		doc := msg.GetDocumentMessage()
		ev.Text = firstText(doc.GetCaption(), ev.Text)
		setMedia(&ev, "document", doc.GetMimetype(), firstText(doc.GetFileName(), doc.GetTitle()), doc.GetURL(), doc.GetFileLength())
		return ev, doc
	case msg.GetVideoMessage() != nil:
		video := msg.GetVideoMessage()
		ev.Text = firstText(video.GetCaption(), ev.Text)
		setMedia(&ev, "video", video.GetMimetype(), "", video.GetURL(), video.GetFileLength())
		return ev, video
	case msg.GetAudioMessage() != nil:
		audio := msg.GetAudioMessage()
		setMedia(&ev, "audio", audio.GetMimetype(), "", audio.GetURL(), audio.GetFileLength())
		return ev, audio
	case msg.GetPtvMessage() != nil:
		video := msg.GetPtvMessage()
		ev.Text = firstText(video.GetCaption(), ev.Text)
		setMedia(&ev, "video", video.GetMimetype(), "", video.GetURL(), video.GetFileLength())
		return ev, video
	case msg.GetStickerMessage() != nil:
		sticker := msg.GetStickerMessage()
		setMedia(&ev, "sticker", sticker.GetMimetype(), "", sticker.GetURL(), sticker.GetFileLength())
		return ev, nil
	case ev.MessageType == "":
		ev.MessageType = fallbackMessageType(info.Type)
	}
	return ev, nil
}

func setMedia(ev *writer.Event, kind, mimeType, title, url string, size uint64) {
	ev.MessageType = kind
	ev.MediaType = kind
	ev.MediaTitle = title
	ev.MediaURL = url
	ev.MediaSize = uint64ToInt64(size)
	ev.MediaExt = mediaExt(mimeType, title)
}

func firstText(values ...string) string {
	for _, value := range values {
		if text := strings.TrimSpace(value); text != "" {
			return text
		}
	}
	return ""
}

func fallbackMessageType(infoType string) string {
	infoType = strings.TrimSpace(strings.ToLower(infoType))
	if infoType == "" {
		return "unknown"
	}
	return infoType
}

func uint64ToInt64(v uint64) int64 {
	const maxInt64 = uint64(^uint64(0) >> 1)
	if v > maxInt64 {
		return int64(maxInt64)
	}
	return int64(v)
}

func mediaExt(mimeType, title string) string {
	if ext := filepath.Ext(title); ext != "" && len(ext) <= 12 {
		return ext
	}
	base := strings.ToLower(strings.TrimSpace(strings.Split(mimeType, ";")[0]))
	switch base {
	case "image/jpeg":
		return ".jpg"
	case "image/png":
		return ".png"
	case "image/gif":
		return ".gif"
	case "image/webp":
		return ".webp"
	case "video/mp4":
		return ".mp4"
	case "audio/ogg":
		return ".ogg"
	case "audio/mpeg":
		return ".mp3"
	case "application/pdf":
		return ".pdf"
	}
	if exts, err := mime.ExtensionsByType(base); err == nil && len(exts) > 0 && len(exts[0]) <= 12 {
		return exts[0]
	}
	return ""
}

type whatsmeowLogger struct {
	base   *health.Logger
	module string
}

func (l whatsmeowLogger) Errorf(msg string, args ...interface{}) {
	l.log("error", msg, args...)
}

func (l whatsmeowLogger) Warnf(msg string, args ...interface{}) {
	l.log("warn", msg, args...)
}

func (l whatsmeowLogger) Infof(msg string, args ...interface{}) {
	l.log("info", msg, args...)
}

func (l whatsmeowLogger) Debugf(msg string, args ...interface{}) {
	l.log("debug", msg, args...)
}

func (l whatsmeowLogger) Sub(module string) waLog.Logger {
	if l.module == "" {
		l.module = module
	} else {
		l.module += "/" + module
	}
	return l
}

func (l whatsmeowLogger) log(level, msg string, args ...interface{}) {
	if l.base == nil {
		return
	}
	l.base.Printf("whatsmeow level=%s module=%s msg=%q", level, l.module, fmt.Sprintf(msg, args...))
}
