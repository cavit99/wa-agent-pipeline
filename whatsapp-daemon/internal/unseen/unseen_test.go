package unseen

import (
	"bytes"
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"openclaw/whatsapp-daemon/internal/writer"
)

func TestTenantGroupJIDsFiltersByTenant(t *testing.T) {
	config := writeConfig(t, `{
		"enabled": true,
		"groups": [
			{"jid": "family@g.us", "label": "family", "tenants": ["family"]},
			{"jid": "main@g.us", "label": "main", "tenants": ["main"]},
			{"jid": "shared@g.us", "label": "shared", "tenants": ["family", "main"]}
		]
	}`)
	got, err := TenantGroupJIDs(config, "family")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(got, ",") != "family@g.us,shared@g.us" {
		t.Fatalf("family jids = %#v", got)
	}
	got, err = TenantGroupJIDs(config, "main")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(got, ",") != "main@g.us,shared@g.us" {
		t.Fatalf("main jids = %#v", got)
	}
}

func TestTenantGroupJIDsRejectsMissingTenants(t *testing.T) {
	config := writeConfig(t, `{"enabled": true, "groups": [{"jid": "x@g.us", "label": "x"}]}`)
	_, err := TenantGroupJIDs(config, "family")
	if err == nil {
		t.Fatal("expected missing tenants error")
	}
	want := "invalid or missing tenants for group x@g.us: expected non-empty list of strings"
	if err.Error() != want {
		t.Fatalf("error = %q, want %q", err.Error(), want)
	}
}

func TestTenantGroupJIDsDisabledReturnsEmpty(t *testing.T) {
	config := writeConfig(t, `{"enabled": false, "groups": [{"jid": "x@g.us", "label": "x"}]}`)
	got, err := TenantGroupJIDs(config, "family")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("disabled config jids = %#v, want empty", got)
	}
}

func TestRunFormatsMessagesAdvancesCursorAndMarksHydratedRows(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(schemaWithoutFTS()); err != nil {
		t.Fatal(err)
	}
	insertGroup(t, db, "family@g.us", "Family Chat")
	insertGroup(t, db, "shared@g.us", "Shared Chat")
	insertGroup(t, db, "main@g.us", "Main Chat")
	insertMessage(t, db, messageFixture{
		ID:                 "old-image",
		SourcePK:           1,
		ChatJID:            "family@g.us",
		ChatLabel:          "Family Chat",
		MsgID:              "old-image",
		SenderName:         "Mia",
		TS:                 1777620600,
		MessageType:        "image",
		MediaPath:          "/cache/photo.jpg",
		MediaExtractedText: "Schedule\nchanged",
		MediaExtractedBy:   "ocr",
		MediaHydratedAt:    1777625900,
	})
	insertMessage(t, db, messageFixture{
		ID:          "new-text",
		SourcePK:    2,
		ChatJID:     "family@g.us",
		ChatLabel:   "Family Chat",
		MsgID:       "new-text",
		SenderName:  "Alex",
		TS:          1777623120,
		Text:        "Hello\nfamily",
		MessageType: "text",
	})
	insertMessage(t, db, messageFixture{
		ID:          "shared-doc",
		SourcePK:    3,
		ChatJID:     "shared@g.us",
		ChatLabel:   "Shared Chat",
		MsgID:       "shared-doc",
		TS:          1777623600,
		MessageType: "document",
		MediaTitle:  "agenda.pdf",
		MediaSize:   2516582,
	})
	insertMessage(t, db, messageFixture{
		ID:          "skip-sticker",
		SourcePK:    4,
		ChatJID:     "family@g.us",
		ChatLabel:   "Family Chat",
		MsgID:       "skip-sticker",
		SenderName:  "Sam",
		TS:          1777623900,
		MessageType: "sticker",
	})
	insertMessage(t, db, messageFixture{
		ID:          "other-tenant",
		SourcePK:    5,
		ChatJID:     "main@g.us",
		ChatLabel:   "Main Chat",
		MsgID:       "other-tenant",
		SenderName:  "Pat",
		TS:          1777624200,
		Text:        "not visible",
		MessageType: "text",
	})
	if _, err := db.Exec(`insert into whatsapp_ingest_runs(
		run_id, started_at, finished_at, mode, dry_run, allowed_group_count, status
	) values(?, ?, ?, ?, ?, ?, ?)`, "run-1", 1777625800, 1777625940, "test", 0, 2, "ok"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`insert into whatsapp_ingest_runs(
		run_id, started_at, finished_at, mode, dry_run, allowed_group_count, status
	) values(?, ?, ?, ?, ?, ?, ?)`, "run-pending", 1777625900, 1777625990, "test", 0, 2, "pending"); err != nil {
		t.Fatal(err)
	}
	config := writeConfig(t, `{
		"enabled": true,
		"groups": [
			{"jid": "family@g.us", "label": "Family Chat", "tenants": ["family"]},
			{"jid": "shared@g.us", "label": "Shared Chat", "tenants": ["family", "main"]},
			{"jid": "main@g.us", "label": "Main Chat", "tenants": ["main"]}
		]
	}`)
	statePath := filepath.Join(t.TempDir(), "state", "whatsapp_seen_ts.family")
	since := 1.0
	var out, errOut bytes.Buffer
	err = runWithDB(context.Background(), db, Options{
		ConfigPath: config,
		Tenant:     "family",
		StatePath:  statePath,
		SinceHours: &since,
		Now:        func() time.Time { return time.Unix(1777626000, 0) },
		Stdout:     &out,
		Stderr:     &errOut,
	})
	if err != nil {
		t.Fatal(err)
	}
	if errOut.String() != "" {
		t.Fatalf("stderr = %q", errOut.String())
	}
	want := "# Unseen parent-group messages since 2026-05-01 08:00 UTC\n" +
		"# 3 messages across 2 group(s)\n" +
		"\n" +
		"## Family Chat — jid: `family@g.us`  (2 messages)\n" +
		"\n" +
		"- [Fri 01 May 2026 07:30] [Mia] [image: *.jpg] → extracted (ocr): Schedule / changed\n" +
		"- [Fri 01 May 2026 08:12] [Alex] Hello / family\n" +
		"\n" +
		"## Shared Chat — jid: `shared@g.us`  (1 messages)\n" +
		"\n" +
		"- [Fri 01 May 2026 08:20] [?] [document: agenda.pdf · 2.4MB]\n" +
		"\n"
	if out.String() != want {
		t.Fatalf("stdout mismatch\nwant:\n%s\ngot:\n%s", want, out.String())
	}
	state, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(state)) != "1777625880" {
		t.Fatalf("state cursor = %q", string(state))
	}
	var surfaced sql.NullInt64
	if err := db.QueryRow("select media_surfaced_at from whatsapp_messages where id='old-image'").Scan(&surfaced); err != nil {
		t.Fatal(err)
	}
	if !surfaced.Valid || surfaced.Int64 != 1777626000 {
		t.Fatalf("media_surfaced_at = %#v", surfaced)
	}
}

type messageFixture struct {
	ID                 string
	SourcePK           int64
	ChatJID            string
	ChatLabel          string
	MsgID              string
	SenderName         string
	TS                 int64
	Text               string
	MessageType        string
	MediaTitle         string
	MediaPath          string
	MediaSize          int64
	MediaExtractedText string
	MediaExtractedBy   string
	MediaHydratedAt    int64
}

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "whatsapp_groups.json")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// schemaWithoutFTS strips the FTS5 virtual table because some go-sqlite3 builds
// are compiled without FTS5 support; the unseen logic does not query the FTS table.
func schemaWithoutFTS() string {
	return strings.ReplaceAll(writer.Schema, `create virtual table if not exists whatsapp_messages_fts
using fts5(id unindexed, text, chat_label, sender_name, media_title);
`, "")
}

func insertGroup(t *testing.T, db *sql.DB, jid, label string) {
	t.Helper()
	_, err := db.Exec(`insert into whatsapp_groups(
		jid, label, ingest_text, ingest_media_metadata, copy_media, index_embeddings, updated_at
	) values(?, ?, 1, 1, 0, 0, ?)`, jid, label, 1777620000)
	if err != nil {
		t.Fatal(err)
	}
}

func insertMessage(t *testing.T, db *sql.DB, msg messageFixture) {
	t.Helper()
	fromMe := 0
	_, err := db.Exec(`insert into whatsapp_messages(
		id, source_pk, chat_jid, chat_label, msg_id, sender_name, ts, from_me, text, raw_type,
		message_type, media_title, media_path, media_size, text_hash, ingested_at,
		media_extracted_text, media_extracted_by, media_hydrated_at
	) values(?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		msg.ID, msg.SourcePK, msg.ChatJID, msg.ChatLabel, msg.MsgID, nullEmpty(msg.SenderName),
		msg.TS, fromMe, nullEmpty(msg.Text), nullEmpty(msg.MessageType), nullEmpty(msg.MediaTitle),
		nullEmpty(msg.MediaPath), nullZero(msg.MediaSize), msg.ID+"-hash", 1777620000,
		nullEmpty(msg.MediaExtractedText), nullEmpty(msg.MediaExtractedBy), nullZero(msg.MediaHydratedAt),
	)
	if err != nil {
		t.Fatal(err)
	}
}

func nullEmpty(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func nullZero(value int64) any {
	if value == 0 {
		return nil
	}
	return value
}
