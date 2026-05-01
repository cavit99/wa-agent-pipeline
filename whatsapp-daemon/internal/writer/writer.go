package writer

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"openclaw/whatsapp-daemon/internal/config"
	"openclaw/whatsapp-daemon/internal/sqlite"
)

//go:embed schema.sql
var Schema string

type Event struct {
	ChatJID                string
	ChatLabel              string
	MsgID                  string
	SenderJID              string
	SenderName             string
	Timestamp              int64
	FromMe                 bool
	Text                   string
	RawType                int
	MessageType            string
	MediaType              string
	MediaTitle             string
	MediaPath              string
	MediaURL               string
	MediaSize              int64
	MediaBytes             []byte
	MediaExt               string
	MediaHydrationStatus   string
	MediaHydrationAttempts int
	SourcePK               int64
}

type Result struct {
	ID        string
	Inserted  bool
	Updated   bool
	MediaPath string
}

type Writer struct {
	db                *sqlite.DB
	mediaRoot         string
	now               func() time.Time
	allowedGroupCount int
	ftsUnavailable    bool
}

func New(dbPath, mediaRoot string) (*Writer, error) {
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o700); err != nil {
		return nil, err
	}
	db, err := sqlite.Open(dbPath)
	if err != nil {
		return nil, err
	}
	w := &Writer{db: db, mediaRoot: mediaRoot, now: time.Now}
	if err := w.EnsureSchema(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	_ = os.Chmod(dbPath, 0o600)
	return w, nil
}

func (w *Writer) Close() error {
	return w.db.Close()
}

func (w *Writer) EnsureSchema(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	base, post := splitSchema()
	if err := w.db.ExecScript(base); err != nil {
		return err
	}
	if err := w.addMissingColumns("whatsapp_groups", []columnMigration{
		{"purpose", "text"},
		{"ingest_text", "integer not null default 1"},
		{"ingest_media_metadata", "integer not null default 1"},
		{"copy_media", "integer not null default 0"},
		{"index_embeddings", "integer not null default 0"},
		{"retention_days", "integer"},
		{"updated_at", "integer not null default 0"},
	}); err != nil {
		return err
	}
	if err := w.addMissingColumns("whatsapp_messages", []columnMigration{
		{"media_url", "text"},
		{"media_size", "integer"},
		{"deleted_or_missing", "integer not null default 0"},
		{"media_local_path", "text"},
		{"media_extracted_text", "text"},
		{"media_extracted_by", "text"},
		{"media_hydrated_at", "integer"},
		{"media_hydration_status", "text"},
		{"media_hydration_attempts", "integer not null default 0"},
		{"media_surfaced_at", "integer"},
	}); err != nil {
		return err
	}
	if err := w.db.ExecScript(post); err != nil {
		return err
	}
	return w.addMissingColumns("whatsapp_ingest_runs", []columnMigration{
		{"started_at", "integer not null default 0"},
		{"finished_at", "integer"},
		{"mode", "text not null default ''"},
		{"dry_run", "integer not null default 0"},
		{"allowed_group_count", "integer not null default 0"},
		{"scanned_messages", "integer not null default 0"},
		{"inserted_messages", "integer not null default 0"},
		{"updated_messages", "integer not null default 0"},
		{"skipped_messages", "integer not null default 0"},
		{"media_copied", "integer not null default 0"},
		{"status", "text not null default ''"},
		{"error", "text"},
	})
}

func (w *Writer) EnsureGroups(_ context.Context, groups []config.Group) error {
	w.allowedGroupCount = len(groups)
	now := w.now().Unix()
	return w.db.WithTx(func() error {
		for _, g := range groups {
			if err := w.db.ExecTx(
				`insert into whatsapp_groups(
				  jid,label,purpose,ingest_text,ingest_media_metadata,copy_media,index_embeddings,retention_days,updated_at
				) values(?,?,?,?,?,?,?,?,?)
				on conflict(jid) do update set
				  label=excluded.label,
				  purpose=excluded.purpose,
				  ingest_text=excluded.ingest_text,
				  ingest_media_metadata=excluded.ingest_media_metadata,
				  copy_media=excluded.copy_media,
				  index_embeddings=excluded.index_embeddings,
				  retention_days=excluded.retention_days,
				  updated_at=excluded.updated_at`,
				g.JID, g.Label, g.Purpose, g.IngestText, g.IngestMediaMetadata,
				g.CopyMedia, g.IndexEmbeddings, retentionValue(g.RetentionDays), now,
			); err != nil {
				return err
			}
		}
		return nil
	})
}

type columnMigration struct {
	name string
	def  string
}

func splitSchema() (string, string) {
	const marker = "create index if not exists idx_whatsapp_messages_chat_ts"
	idx := strings.Index(Schema, marker)
	if idx < 0 {
		return Schema, ""
	}
	return Schema[:idx], Schema[idx:]
}

func (w *Writer) addMissingColumns(table string, cols []columnMigration) error {
	rows, err := w.db.Query(fmt.Sprintf("pragma table_info(%s)", table))
	if err != nil {
		return err
	}
	have := make(map[string]bool, len(rows))
	for _, row := range rows {
		if name, ok := row["name"].(string); ok {
			have[name] = true
		}
	}
	for _, col := range cols {
		if have[col.name] {
			continue
		}
		if err := w.db.Exec(fmt.Sprintf("alter table %s add column %s %s", table, col.name, col.def)); err != nil {
			return fmt.Errorf("migrate %s.%s: %w", table, col.name, err)
		}
	}
	return nil
}

func (w *Writer) Write(ctx context.Context, ev Event) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	if ev.ChatJID == "" || ev.MsgID == "" || ev.Timestamp == 0 {
		return Result{}, fmt.Errorf("event missing chat_jid, msg_id, or timestamp")
	}
	ev.SourcePK = w.sourcePKFor(ev)
	id := StableMessageID(ev.ChatJID, ev.MsgID, ev.SenderJID, ev.Timestamp, ev.SourcePK)
	now := w.now().Unix()
	thash := TextHash(ev.Text, ev.MediaTitle)
	var res Result
	res.ID = id
	var runID string
	err := w.db.WithTx(func() error {
		existing, err := w.db.QueryTx("select text_hash from whatsapp_messages where id=?", id)
		if err != nil {
			return err
		}
		if len(existing) == 0 {
			res.Inserted = true
		} else if existing[0]["text_hash"] != thash {
			res.Updated = true
		}
		mediaHydrationStatus := nullEmpty(ev.MediaHydrationStatus)
		mediaHydrationAttempts := ev.MediaHydrationAttempts
		if mediaHydrationStatus == nil {
			mediaHydrationAttempts = 0
		} else if len(existing) != 0 {
			res.Updated = true
		}
		if err := w.db.ExecTx(
			`insert into whatsapp_messages(
			  id,source_pk,chat_jid,chat_label,msg_id,sender_jid,sender_name,ts,from_me,text,raw_type,
			  message_type,media_type,media_title,media_path,media_url,media_size,text_hash,ingested_at,deleted_or_missing,
			  media_hydration_status,media_hydration_attempts
			) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,?,?)
			on conflict(id) do update set
			  sender_name=excluded.sender_name,
			  text=excluded.text,
			  message_type=excluded.message_type,
			  media_type=excluded.media_type,
			  media_title=excluded.media_title,
			  media_path=excluded.media_path,
			  media_url=excluded.media_url,
			  media_size=excluded.media_size,
			  text_hash=excluded.text_hash,
			  ingested_at=excluded.ingested_at,
			  deleted_or_missing=0,
			  media_hydration_status=case
			    when excluded.media_hydration_status is not null then excluded.media_hydration_status
			    else whatsapp_messages.media_hydration_status
			  end,
			  media_hydration_attempts=case
			    when excluded.media_hydration_status is not null then whatsapp_messages.media_hydration_attempts+excluded.media_hydration_attempts
			    else whatsapp_messages.media_hydration_attempts
			  end`,
			id, ev.SourcePK, ev.ChatJID, ev.ChatLabel, ev.MsgID, nullEmpty(ev.SenderJID),
			nullEmpty(ev.SenderName), ev.Timestamp, ev.FromMe, nullEmpty(ev.Text), ev.RawType,
			nullEmpty(ev.MessageType), nullEmpty(ev.MediaType), nullEmpty(ev.MediaTitle),
			nullEmpty(ev.MediaPath), nullEmpty(ev.MediaURL), nullZero(ev.MediaSize), thash, now,
			mediaHydrationStatus, mediaHydrationAttempts,
		); err != nil {
			return err
		}
		if err := w.refreshFTSTx(id, ev); err != nil {
			return err
		}
		runID, err = w.recordRun(id, now, res, len(ev.MediaBytes) > 0)
		return err
	})
	if err != nil {
		return Result{}, err
	}
	if len(ev.MediaBytes) > 0 {
		path, err := w.writeMedia(ev)
		if err != nil {
			if markErr := w.markMediaFailure(id, runID); markErr != nil {
				return res, fmt.Errorf("write media: %w; mark media failure: %v", err, markErr)
			}
			return res, err
		}
		res.MediaPath = path
		if !res.Inserted {
			res.Updated = true
		}
		if err := w.markMediaOK(id, runID, path); err != nil {
			return res, err
		}
	}
	return res, nil
}

func (w *Writer) refreshFTSTx(id string, ev Event) error {
	if w.ftsUnavailable {
		return nil
	}
	if err := w.db.ExecTx("delete from whatsapp_messages_fts where id=?", id); err != nil {
		return w.handleFTSError(err)
	}
	if err := w.db.ExecTx(
		"insert into whatsapp_messages_fts(id,text,chat_label,sender_name,media_title) values(?,?,?,?,?)",
		id, ev.Text, ev.ChatLabel, ev.SenderName, ev.MediaTitle,
	); err != nil {
		return w.handleFTSError(err)
	}
	return nil
}

func (w *Writer) handleFTSError(err error) error {
	if isFTSUnavailable(err) {
		w.ftsUnavailable = true
		return nil
	}
	return err
}

func isFTSUnavailable(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "no such module: fts5") ||
		strings.Contains(msg, "no such table: whatsapp_messages_fts")
}

func (w *Writer) sourcePKFor(ev Event) int64 {
	if ev.SourcePK != 0 {
		return ev.SourcePK
	}
	rows, err := w.db.Query(
		`select id, source_pk from whatsapp_messages
		  where chat_jid=? and msg_id=? and coalesce(sender_jid,'')=? and ts=?
		  order by source_pk desc limit 1`,
		ev.ChatJID, ev.MsgID, ev.SenderJID, ev.Timestamp,
	)
	if err == nil && len(rows) == 1 {
		if pk, ok := rows[0]["source_pk"].(int64); ok {
			return pk
		}
	}
	return 0
}

func (w *Writer) recordRun(id string, now int64, res Result, mediaPending bool) (string, error) {
	inserted, updated, skipped := 0, 0, 0
	switch {
	case res.Inserted:
		inserted = 1
	case res.Updated:
		updated = 1
	default:
		skipped = 1
	}
	runID := StableRunID(id, now)
	status := "ok"
	if mediaPending {
		status = "pending"
	}
	return runID, w.db.ExecTx(
		`insert into whatsapp_ingest_runs(
		  run_id,started_at,finished_at,mode,dry_run,
		  allowed_group_count,scanned_messages,inserted_messages,updated_messages,skipped_messages,
		  media_copied,status
		) values(?,?,?,?,?,?,?,?,?,?,?,?)
		on conflict(run_id) do update set
		  finished_at=excluded.finished_at,
		  scanned_messages=whatsapp_ingest_runs.scanned_messages+excluded.scanned_messages,
		  inserted_messages=whatsapp_ingest_runs.inserted_messages+excluded.inserted_messages,
		  updated_messages=whatsapp_ingest_runs.updated_messages+excluded.updated_messages,
		  skipped_messages=whatsapp_ingest_runs.skipped_messages+excluded.skipped_messages,
		  status=excluded.status`,
		runID, now, now, "whatsmeow-live", 0,
		w.allowedGroupCount, 1, inserted, updated, skipped, 0, status,
	)
}

func (w *Writer) writeMedia(ev Event) (string, error) {
	ext := cleanExt(ev.MediaExt)
	if ext == "" {
		ext = extForType(ev.MessageType)
	}
	dir := filepath.Join(w.mediaRoot, safePathPart(ev.ChatJID))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	final := filepath.Join(dir, safePathPart(ev.MsgID)+ext)
	tmp := final + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return "", err
	}
	if _, err = f.Write(ev.MediaBytes); err != nil {
		_ = f.Close()
		return "", err
	}
	if err = f.Sync(); err != nil {
		_ = f.Close()
		return "", err
	}
	if err = f.Close(); err != nil {
		return "", err
	}
	if err = os.Rename(tmp, final); err != nil {
		return "", err
	}
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return final, nil
}

func (w *Writer) markMediaOK(id, runID, path string) error {
	now := w.now().Unix()
	return w.db.WithTx(func() error {
		if err := w.db.ExecTx(
			`update whatsapp_messages
		    set media_local_path=?, media_hydrated_at=?, media_hydration_status='ok',
		        media_hydration_attempts=media_hydration_attempts+1, media_surfaced_at=null
		  where id=?`,
			path, now, id,
		); err != nil {
			return err
		}
		return w.db.ExecTx(
			`update whatsapp_ingest_runs
			    set status='ok', media_copied=media_copied+1
			  where run_id=?`,
			runID,
		)
	})
}

func (w *Writer) markMediaFailure(id, runID string) error {
	return w.db.WithTx(func() error {
		if err := w.db.ExecTx(
			`update whatsapp_messages
			    set media_hydration_status='failed',
			        media_hydration_attempts=media_hydration_attempts+1
			  where id=?`,
			id,
		); err != nil {
			return err
		}
		return w.db.ExecTx(
			`update whatsapp_ingest_runs
			    set status='ok'
			  where run_id=?`,
			runID,
		)
	})
}

func StableMessageID(chatJID, msgID, senderJID string, ts, sourcePK int64) string {
	parts := []string{chatJID, msgID, senderJID, fmt.Sprint(ts), fmt.Sprint(sourcePK)}
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(sum[:])
}

func TextHash(text, mediaTitle string) string {
	sum := sha256.Sum256([]byte(text + "\x00" + mediaTitle))
	return hex.EncodeToString(sum[:])
}

func StableRunID(messageID string, now int64) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%d\x00%s", now, messageID)))
	return hex.EncodeToString(sum[:16])
}

func nullEmpty(v string) any {
	if v == "" {
		return nil
	}
	return v
}

func nullZero(v int64) any {
	if v == 0 {
		return nil
	}
	return v
}

func retentionValue(v *int) any {
	if v == nil {
		return nil
	}
	return *v
}

var unsafePath = regexp.MustCompile(`[^A-Za-z0-9@._-]+`)

func safePathPart(v string) string {
	v = strings.Trim(unsafePath.ReplaceAllString(v, "_"), "._")
	if v == "" {
		return "unknown"
	}
	return v
}

func cleanExt(ext string) string {
	if ext == "" {
		return ""
	}
	if !strings.HasPrefix(ext, ".") {
		ext = "." + ext
	}
	if strings.ContainsAny(ext, `/\`) || len(ext) > 12 {
		return ""
	}
	return strings.ToLower(ext)
}

func extForType(t string) string {
	switch strings.ToLower(t) {
	case "image":
		return ".jpg"
	case "video":
		return ".mp4"
	case "audio", "ptt":
		return ".ogg"
	case "sticker":
		return ".webp"
	default:
		return ".bin"
	}
}
