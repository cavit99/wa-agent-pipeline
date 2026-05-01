package unseen

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	_ "github.com/mattn/go-sqlite3"
)

const (
	DefaultOverlap       int64 = 1800
	IngestMargin         int64 = 60
	HydratedSnippetChars       = 600
)

var (
	skipTypes = map[string]bool{
		"reaction": true, "sticker": true, "system": true, "group_event": true,
		"type_46": true, "type_66": true, "ptt": true,
	}
	mediaTypes = map[string]bool{"image": true, "document": true, "video": true, "audio": true}
)

type Options struct {
	DBPath     string
	ConfigPath string
	Tenant     string
	StatePath  string
	SinceHours *float64
	NoAdvance  bool
	Now        func() time.Time
	Stdout     io.Writer
	Stderr     io.Writer
}

type configFile struct {
	Enabled bool                         `json:"enabled"`
	Groups  []map[string]json.RawMessage `json:"groups"`
}

type messageRow struct {
	ID                   string
	ChatJID              string
	ChatLabel            string
	SenderName           sql.NullString
	TS                   int64
	MessageType          sql.NullString
	MediaType            sql.NullString
	MediaTitle           sql.NullString
	MediaPath            sql.NullString
	MediaLocalPath       sql.NullString
	MediaSize            sql.NullInt64
	MediaExtractedText   sql.NullString
	MediaExtractedBy     sql.NullString
	MediaHydratedAt      sql.NullInt64
	MediaHydrationStatus sql.NullString
	MediaSurfacedAt      sql.NullInt64
	Text                 string
	Display              string
	NeedsSurfaceMark     bool
}

func DefaultStatePath(root, tenant string) string {
	return filepath.Join(root, "state", "whatsapp_seen_ts."+tenant)
}

func TenantGroupJIDs(configPath, tenant string) ([]string, error) {
	b, err := os.ReadFile(configPath)
	if err != nil {
		return nil, err
	}
	var cfg configFile
	if err := json.Unmarshal(b, &cfg); err != nil {
		return nil, err
	}
	if !cfg.Enabled {
		return nil, nil
	}
	var jids []string
	for _, group := range cfg.Groups {
		tenants, err := groupTenants(group)
		if err != nil {
			return nil, err
		}
		jid := strings.TrimSpace(rawString(group["jid"], ""))
		if jid == "" {
			continue
		}
		for _, groupTenant := range tenants {
			if groupTenant == tenant {
				jids = append(jids, jid)
				break
			}
		}
	}
	return jids, nil
}

func Run(ctx context.Context, opts Options) error {
	if opts.DBPath == "" {
		return fmt.Errorf("missing db path")
	}
	db, err := sql.Open("sqlite3", opts.DBPath)
	if err != nil {
		return err
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, "pragma busy_timeout = 5000"); err != nil {
		return err
	}
	return runWithDB(ctx, db, opts)
}

func runWithDB(ctx context.Context, db *sql.DB, opts Options) error {
	if opts.Tenant == "" {
		return fmt.Errorf("unseen requires --tenant <name>")
	}
	if opts.ConfigPath == "" {
		return fmt.Errorf("missing config path")
	}
	if opts.StatePath == "" {
		return fmt.Errorf("missing state path")
	}
	out := opts.Stdout
	if out == nil {
		out = os.Stdout
	}
	errOut := opts.Stderr
	if errOut == nil {
		errOut = os.Stderr
	}
	nowFn := opts.Now
	if nowFn == nil {
		nowFn = time.Now
	}
	now := nowFn().Unix()
	allowedJIDs, err := TenantGroupJIDs(opts.ConfigPath, opts.Tenant)
	if err != nil {
		return err
	}
	cutoff, err := cutoffFor(now, opts.StatePath, opts.SinceHours)
	if err != nil {
		return err
	}
	rows, err := fetchRows(ctx, db, allowedJIDs, cutoff)
	if err != nil {
		return err
	}
	msgs := make([]messageRow, 0, len(rows))
	for _, row := range rows {
		mtype := strings.ToLower(nullString(row.MessageType))
		if skipTypes[mtype] {
			continue
		}
		row.Display = displayFor(row)
		hydratedAt := nullInt(row.MediaHydratedAt)
		row.NeedsSurfaceMark = hydratedAt != 0 && !row.MediaSurfacedAt.Valid
		msgs = append(msgs, row)
	}
	writeOutput(out, cutoff, msgs)
	if opts.NoAdvance {
		return nil
	}
	ingestTS, ok, err := latestOKIngestFinishedAt(ctx, db)
	if err != nil {
		return err
	}
	if !ok {
		fmt.Fprintln(errOut, "# WARNING: no successful ingest run on record; cursor unchanged.")
		return nil
	}
	maxAdvance := ingestTS - IngestMargin
	priorCursor, err := priorCursor(opts.StatePath)
	if err != nil {
		return err
	}
	if maxAdvance <= priorCursor {
		fmt.Fprintf(errOut, "# WARNING: latest OK ingest (%d) is not newer than cursor (%d); cursor unchanged.\n", ingestTS, priorCursor)
	} else {
		newCursor := maxAdvance
		if now < newCursor {
			newCursor = now
		}
		if err := atomicWriteText(opts.StatePath, strconv.FormatInt(newCursor, 10)); err != nil {
			return err
		}
	}
	return markSurfaced(ctx, db, msgs, now)
}

func groupTenants(group map[string]json.RawMessage) ([]string, error) {
	raw, ok := group["tenants"]
	if !ok {
		return nil, invalidTenantsError(group)
	}
	var values []any
	if err := json.Unmarshal(raw, &values); err != nil {
		return nil, invalidTenantsError(group)
	}
	if len(values) == 0 {
		return nil, invalidTenantsError(group)
	}
	out := make([]string, 0, len(values))
	for _, value := range values {
		tenant, ok := value.(string)
		if !ok {
			return nil, invalidTenantsError(group)
		}
		tenant = strings.TrimSpace(tenant)
		if tenant == "" {
			return nil, invalidTenantsError(group)
		}
		out = append(out, tenant)
	}
	return out, nil
}

func invalidTenantsError(group map[string]json.RawMessage) error {
	return fmt.Errorf("invalid or missing tenants for group %s: expected non-empty list of strings", rawString(group["jid"], "<missing jid>"))
}

func rawString(raw json.RawMessage, fallback string) string {
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	return fallback
}

func cutoffFor(now int64, statePath string, sinceHours *float64) (int64, error) {
	if sinceHours != nil {
		if *sinceHours == 0 {
			return 0, nil
		}
		return int64(float64(now) - *sinceHours*3600), nil
	}
	b, err := os.ReadFile(statePath)
	if err == nil {
		prior, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
		if err != nil {
			return now - 24*3600, nil
		}
		cutoff := prior - DefaultOverlap
		if cutoff < 0 {
			return 0, nil
		}
		return cutoff, nil
	}
	if os.IsNotExist(err) {
		return now - 24*3600, nil
	}
	return 0, err
}

func priorCursor(statePath string) (int64, error) {
	b, err := os.ReadFile(statePath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	prior, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
	if err != nil {
		return 0, nil
	}
	return prior, nil
}

func fetchRows(ctx context.Context, db *sql.DB, allowedJIDs []string, cutoff int64) ([]messageRow, error) {
	tenantClause := " and 0 = 1"
	args := make([]any, 0, len(allowedJIDs)+1)
	if len(allowedJIDs) > 0 {
		placeholders := strings.TrimRight(strings.Repeat("?,", len(allowedJIDs)), ",")
		tenantClause = " and chat_jid in (" + placeholders + ")"
		for _, jid := range allowedJIDs {
			args = append(args, jid)
		}
	}
	args = append(args, cutoff)
	query := `select id, chat_jid, chat_label, sender_name, ts, message_type,
                  media_type, media_title, media_path, media_local_path,
                  media_size, media_extracted_text, media_extracted_by,
                  media_hydrated_at, media_hydration_status, media_surfaced_at,
                  coalesce(text,'') as text
             from whatsapp_messages
            where from_me = 0
              ` + tenantClause + `
              and (
                ts >= ?
                or (media_hydrated_at is not null and media_surfaced_at is null)
              )
            order by chat_label, ts`
	sqlRows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer sqlRows.Close()
	var out []messageRow
	for sqlRows.Next() {
		var row messageRow
		if err := sqlRows.Scan(
			&row.ID, &row.ChatJID, &row.ChatLabel, &row.SenderName, &row.TS, &row.MessageType,
			&row.MediaType, &row.MediaTitle, &row.MediaPath, &row.MediaLocalPath,
			&row.MediaSize, &row.MediaExtractedText, &row.MediaExtractedBy,
			&row.MediaHydratedAt, &row.MediaHydrationStatus, &row.MediaSurfacedAt,
			&row.Text,
		); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, sqlRows.Err()
}

func displayFor(row messageRow) string {
	mtype := strings.ToLower(nullString(row.MessageType))
	text := strings.TrimSpace(row.Text)
	var display string
	switch {
	case text != "":
		display = text
	case mediaTypes[mtype]:
		display = mediaPlaceholder(row)
	default:
		if mtype == "" {
			mtype = "unknown"
		}
		display = "[" + mtype + "]"
	}
	hydratedAt := nullInt(row.MediaHydratedAt)
	if hydratedAt == 0 || row.MediaSurfacedAt.Valid {
		return display
	}
	extracted := strings.TrimSpace(nullString(row.MediaExtractedText))
	extractedBy := strings.TrimSpace(nullString(row.MediaExtractedBy))
	localPath := nullString(row.MediaLocalPath)
	if extracted != "" {
		snippet, truncated := truncateRunes(extracted, HydratedSnippetChars)
		snippet = strings.ReplaceAll(snippet, "\n", " / ")
		if truncated {
			snippet += "…"
		}
		tag := "extracted"
		if extractedBy != "" {
			tag = fmt.Sprintf("extracted (%s)", extractedBy)
		}
		return fmt.Sprintf("%s → %s: %s", display, tag, snippet)
	}
	if localPath != "" {
		return fmt.Sprintf("%s → file: %s (id: %s; open with `read` if action-relevant)", display, localPath, row.ID)
	}
	return display
}

func mediaPlaceholder(row messageRow) string {
	mtype := strings.ToLower(nullString(row.MessageType))
	if mtype == "" {
		mtype = "media"
	}
	parts := []string{mtype}
	title := strings.TrimSpace(nullString(row.MediaTitle))
	if title != "" && strings.Contains(title, ".") && utf8.RuneCountInString(title) <= 200 && !strings.Contains(title, "/") && !strings.Contains(title, "+") {
		parts = append(parts, title)
	} else if mediaPath := nullString(row.MediaPath); mediaPath != "" {
		if ext := filepath.Ext(mediaPath); ext != "" {
			parts = append(parts, "*"+ext)
		}
	}
	if size := nullInt(row.MediaSize); size != 0 {
		parts = append(parts, humanSize(size))
	}
	if len(parts) > 2 {
		return fmt.Sprintf("[%s: %s · %s]", parts[0], parts[1], parts[2])
	}
	if len(parts) == 2 {
		return fmt.Sprintf("[%s: %s]", parts[0], parts[1])
	}
	return "[" + parts[0] + "]"
}

func humanSize(n int64) string {
	const KB, MB, GB = int64(1024), int64(1024 * 1024), int64(1024 * 1024 * 1024)
	switch {
	case n < MB:
		return fmt.Sprintf("%.1fKB", float64(n)/float64(KB))
	case n < GB:
		return fmt.Sprintf("%.1fMB", float64(n)/float64(MB))
	default:
		return fmt.Sprintf("%.1fGB", float64(n)/float64(GB))
	}
}

func writeOutput(out io.Writer, cutoff int64, msgs []messageRow) {
	byGroup := make(map[string][]messageRow)
	var groupOrder []string
	for _, msg := range msgs {
		if _, ok := byGroup[msg.ChatLabel]; !ok {
			groupOrder = append(groupOrder, msg.ChatLabel)
		}
		byGroup[msg.ChatLabel] = append(byGroup[msg.ChatLabel], msg)
	}
	fmt.Fprintf(out, "# Unseen parent-group messages since %s\n", time.Unix(cutoff, 0).UTC().Format("2006-01-02 15:04 UTC"))
	fmt.Fprintf(out, "# %d messages across %d group(s)\n", len(msgs), len(groupOrder))
	fmt.Fprintln(out)
	if len(msgs) == 0 {
		fmt.Fprintln(out, "(no new messages)")
	}
	for _, label := range groupOrder {
		gmsgs := byGroup[label]
		fmt.Fprintf(out, "## %s — jid: `%s`  (%d messages)\n", label, gmsgs[0].ChatJID, len(gmsgs))
		fmt.Fprintln(out)
		for _, msg := range gmsgs {
			display := strings.ReplaceAll(msg.Display, "\n", " / ")
			display = strings.ReplaceAll(display, "\r", " ")
			sender := nullString(msg.SenderName)
			if sender == "" {
				sender = "?"
			}
			fmt.Fprintf(out, "- [%s] [%s] %s\n", time.Unix(msg.TS, 0).UTC().Format("Mon 02 Jan 2006 15:04"), sender, display)
		}
		fmt.Fprintln(out)
	}
}

func latestOKIngestFinishedAt(ctx context.Context, db *sql.DB) (int64, bool, error) {
	var ts sql.NullInt64
	// status='ok' means message text persisted; media failures are per-message.
	err := db.QueryRowContext(ctx, `select max(finished_at) from whatsapp_ingest_runs
            where status = 'ok' and dry_run = 0 and finished_at is not null`).Scan(&ts)
	if err != nil {
		return 0, false, err
	}
	if !ts.Valid {
		return 0, false, nil
	}
	return ts.Int64, true, nil
}

func markSurfaced(ctx context.Context, db *sql.DB, msgs []messageRow, now int64) error {
	ids := make([]string, 0)
	for _, msg := range msgs {
		if msg.NeedsSurfaceMark {
			ids = append(ids, msg.ID)
		}
	}
	if len(ids) == 0 {
		return nil
	}
	placeholders := strings.TrimRight(strings.Repeat("?,", len(ids)), ",")
	args := make([]any, 0, len(ids)+1)
	args = append(args, now)
	for _, id := range ids {
		args = append(args, id)
	}
	_, err := db.ExecContext(ctx, "update whatsapp_messages set media_surfaced_at = ? where id in ("+placeholders+")", args...)
	return err
}

func atomicWriteText(path, text string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	tmp := filepath.Join(filepath.Dir(path), "."+filepath.Base(path)+".tmp."+strconv.Itoa(os.Getpid()))
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmp)
		}
	}()
	if _, err := io.WriteString(f, text); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	cleanup = false
	if dir, err := os.Open(filepath.Dir(path)); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
	return nil
}

func nullString(value sql.NullString) string {
	if !value.Valid {
		return ""
	}
	return value.String
}

func nullInt(value sql.NullInt64) int64 {
	if !value.Valid {
		return 0
	}
	return value.Int64
}

func truncateRunes(value string, limit int) (string, bool) {
	runes := []rune(value)
	if len(runes) <= limit {
		return value, false
	}
	return string(runes[:limit]), true
}
