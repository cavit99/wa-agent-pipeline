package writer

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"openclaw/whatsapp-daemon/internal/config"
	"openclaw/whatsapp-daemon/internal/sqlite"
)

func newTestWriter(t *testing.T) *Writer {
	t.Helper()
	dir := t.TempDir()
	w, err := New(filepath.Join(dir, "openclaw.db"), filepath.Join(dir, "media"))
	if err != nil {
		t.Fatal(err)
	}
	w.now = func() time.Time { return time.Unix(1714470000, 0) }
	t.Cleanup(func() { _ = w.Close() })
	if err := w.EnsureGroups(context.Background(), []config.Group{{
		JID: "120@g.us", Label: "test", IngestText: true, IngestMediaMetadata: true, CopyMedia: true,
	}}); err != nil {
		t.Fatal(err)
	}
	return w
}

func TestHashEquivalenceWithPython(t *testing.T) {
	if got := TextHash("", ""); got != "6e340b9cffb37a989ca544e6bb780a2c78901d3fb33738768511a30617afa01d" {
		t.Fatalf("empty hash mismatch: %s", got)
	}
	if got := TextHash("hello", "file.pdf"); got != "8e25f8cd321fd812805b85792f69253858974006645ed90f72e6c54c654c8031" {
		t.Fatalf("text/media hash mismatch: %s", got)
	}
	if got := TextHash("emoji \U0001f9ea", "r\u00e9sum\u00e9.pdf"); got != "c5b333bb5d60737fdd384738e544d9e7f54bef1726c8b08eb824cc5b67d96bc5" {
		t.Fatalf("unicode hash mismatch: %s", got)
	}
	if got := StableMessageID("120@g.us", "ABC", "", 1714470000, 0); got != "701dabdb1a966ef898d5c69572791ea9906a1d037e50aef89f6806a88d494ccf" {
		t.Fatalf("stable id mismatch: %s", got)
	}
}

func TestWriteIsIdempotentAndRefreshesFTS(t *testing.T) {
	w := newTestWriter(t)
	ev := Event{ChatJID: "120@g.us", ChatLabel: "test", MsgID: "ABC", SenderJID: "447@g.us", SenderName: "anon", Timestamp: 1714470000, Text: "hello", RawType: 1, MessageType: "text"}
	first, err := w.Write(context.Background(), ev)
	if err != nil {
		t.Fatal(err)
	}
	second, err := w.Write(context.Background(), ev)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != second.ID {
		t.Fatalf("id changed: %s vs %s", first.ID, second.ID)
	}
	rows, err := w.db.Query("select count(*) as n from whatsapp_messages where chat_jid=?", "120@g.us")
	if err != nil {
		t.Fatal(err)
	}
	if rows[0]["n"].(int64) != 1 {
		t.Fatalf("duplicate message rows: %#v", rows)
	}
	rows, err = w.db.Query("select text from whatsapp_messages_fts where id=?", first.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0]["text"] != "hello" {
		t.Fatalf("fts not refreshed: %#v", rows)
	}
}

func TestNaturalKeyReusesExistingSourcePK(t *testing.T) {
	w := newTestWriter(t)
	oldID := StableMessageID("120@g.us", "ABC", "447@g.us", 1714470000, 42)
	if err := w.db.Exec(
		`insert into whatsapp_messages(
		  id,source_pk,chat_jid,chat_label,msg_id,sender_jid,sender_name,ts,from_me,text,raw_type,
		  message_type,text_hash,ingested_at,deleted_or_missing
		) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,0)`,
		oldID, 42, "120@g.us", "test", "ABC", "447@g.us", "anon", 1714470000, false, "old", 1, "text", TextHash("old", ""), 1714460000,
	); err != nil {
		t.Fatal(err)
	}
	res, err := w.Write(context.Background(), Event{ChatJID: "120@g.us", ChatLabel: "test", MsgID: "ABC", SenderJID: "447@g.us", SenderName: "anon", Timestamp: 1714470000, Text: "new", RawType: 1, MessageType: "text"})
	if err != nil {
		t.Fatal(err)
	}
	if res.ID != oldID {
		t.Fatalf("did not reuse old id: got %s want %s", res.ID, oldID)
	}
	rows, _ := w.db.Query("select count(*) as n, max(source_pk) as pk, max(text) as text from whatsapp_messages")
	if rows[0]["n"].(int64) != 1 || rows[0]["pk"].(int64) != 42 || rows[0]["text"] != "new" {
		t.Fatalf("unexpected natural-key result: %#v", rows)
	}
}

func TestMediaWriteIsAtomicBeforeDBPathUpdate(t *testing.T) {
	w := newTestWriter(t)
	res, err := w.Write(context.Background(), Event{
		ChatJID: "120@g.us", ChatLabel: "test", MsgID: "IMG1", SenderJID: "447@g.us",
		SenderName: "anon", Timestamp: 1714470000, RawType: 1, MessageType: "image",
		MediaType: "image", MediaTitle: "photo.jpg", MediaSize: 4, MediaBytes: []byte("data"), MediaExt: ".jpg",
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.MediaPath == "" {
		t.Fatal("media path not returned")
	}
	if _, err := os.Stat(res.MediaPath + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("tmp file still exists or stat failed: %v", err)
	}
	body, err := os.ReadFile(res.MediaPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(body, []byte("data")) {
		t.Fatalf("media body mismatch: %q", body)
	}
	rows, _ := w.db.Query("select media_local_path, media_hydration_status, media_hydration_attempts from whatsapp_messages where id=?", res.ID)
	if rows[0]["media_local_path"] != res.MediaPath || rows[0]["media_hydration_status"] != "ok" || rows[0]["media_hydration_attempts"].(int64) != 1 {
		t.Fatalf("db media fields wrong: %#v", rows)
	}
	rows, _ = w.db.Query("select media_copied, status from whatsapp_ingest_runs where run_id=?", StableRunID(res.ID, 1714470000))
	if rows[0]["media_copied"].(int64) != 1 || rows[0]["status"] != "ok" {
		t.Fatalf("run media accounting wrong: %#v", rows)
	}
}

func TestMediaFailureDoesNotPointAtPartialFile(t *testing.T) {
	dir := t.TempDir()
	mediaRoot := filepath.Join(dir, "not-a-dir")
	if err := os.WriteFile(mediaRoot, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	w, err := New(filepath.Join(dir, "openclaw.db"), mediaRoot)
	if err != nil {
		t.Fatal(err)
	}
	w.now = func() time.Time { return time.Unix(1714470000, 0) }
	t.Cleanup(func() { _ = w.Close() })
	if err := w.EnsureGroups(context.Background(), []config.Group{{JID: "120@g.us", Label: "test", IngestText: true, IngestMediaMetadata: true, CopyMedia: true}}); err != nil {
		t.Fatal(err)
	}
	res, err := w.Write(context.Background(), Event{ChatJID: "120@g.us", ChatLabel: "test", MsgID: "IMG2", SenderJID: "447@g.us", Timestamp: 1714470000, RawType: 1, MessageType: "image", MediaBytes: []byte("data"), MediaExt: ".jpg"})
	if err == nil {
		t.Fatal("expected media write failure")
	}
	rows, _ := w.db.Query("select media_local_path, media_hydration_status, media_hydration_attempts from whatsapp_messages where id=?", res.ID)
	if rows[0]["media_local_path"] != nil || rows[0]["media_hydration_status"] != "failed" || rows[0]["media_hydration_attempts"].(int64) != 1 {
		t.Fatalf("db points at failed media: %#v", rows)
	}
	rows, _ = w.db.Query("select status, finished_at, media_copied from whatsapp_ingest_runs where run_id=?", StableRunID(res.ID, 1714470000))
	if rows[0]["status"] != "ok" || rows[0]["finished_at"].(int64) != 1714470000 || rows[0]["media_copied"].(int64) != 0 {
		t.Fatalf("media failure run should remain cursor-eligible without copied media: %#v", rows)
	}
}

func TestWriteRecordsExplicitMediaHydrationFailure(t *testing.T) {
	w := newTestWriter(t)
	ev := Event{
		ChatJID: "120@g.us", ChatLabel: "test", MsgID: "IMG3", SenderJID: "447@g.us",
		Timestamp: 1714470000, RawType: 1, MessageType: "image", MediaType: "image",
		MediaHydrationStatus: "failed: expired", MediaHydrationAttempts: 1,
	}
	res, err := w.Write(context.Background(), ev)
	if err != nil {
		t.Fatal(err)
	}
	rows, _ := w.db.Query("select media_hydration_status, media_hydration_attempts from whatsapp_messages where id=?", res.ID)
	if rows[0]["media_hydration_status"] != "failed: expired" || rows[0]["media_hydration_attempts"].(int64) != 1 {
		t.Fatalf("explicit media failure not recorded: %#v", rows)
	}

	ev.MediaHydrationStatus = ""
	ev.MediaHydrationAttempts = 0
	if _, err := w.Write(context.Background(), ev); err != nil {
		t.Fatal(err)
	}
	rows, _ = w.db.Query("select media_hydration_status, media_hydration_attempts from whatsapp_messages where id=?", res.ID)
	if rows[0]["media_hydration_status"] != "failed: expired" || rows[0]["media_hydration_attempts"].(int64) != 1 {
		t.Fatalf("empty media status should preserve existing failure: %#v", rows)
	}
}

func TestWriteCountsExplicitMediaHydrationFailureAsUpdate(t *testing.T) {
	w := newTestWriter(t)
	ev := Event{
		ChatJID: "120@g.us", ChatLabel: "test", MsgID: "IMG4", SenderJID: "447@g.us",
		Timestamp: 1714470000, RawType: 1, MessageType: "image", MediaType: "image",
	}
	first, err := w.Write(context.Background(), ev)
	if err != nil {
		t.Fatal(err)
	}
	if !first.Inserted || first.Updated {
		t.Fatalf("unexpected first write result: %#v", first)
	}

	ev.MediaHydrationStatus = "failed: expired"
	ev.MediaHydrationAttempts = 1
	second, err := w.Write(context.Background(), ev)
	if err != nil {
		t.Fatal(err)
	}
	if second.Inserted || !second.Updated {
		t.Fatalf("hydration failure should count as an update: %#v", second)
	}

	rows, _ := w.db.Query("select media_hydration_status, media_hydration_attempts from whatsapp_messages where id=?", first.ID)
	if rows[0]["media_hydration_status"] != "failed: expired" || rows[0]["media_hydration_attempts"].(int64) != 1 {
		t.Fatalf("explicit media failure not recorded on existing row: %#v", rows)
	}
}

func TestWritePersistsWhenFTSUnavailable(t *testing.T) {
	w := newTestWriter(t)
	w.ftsUnavailable = true
	res, err := w.Write(context.Background(), Event{
		ChatJID: "120@g.us", ChatLabel: "test", MsgID: "NOFTS1", SenderJID: "447@g.us",
		Timestamp: 1714470000, Text: "hello without fts", RawType: 1, MessageType: "text",
	})
	if err != nil {
		t.Fatal(err)
	}
	rows, _ := w.db.Query("select text from whatsapp_messages where id=?", res.ID)
	if len(rows) != 1 || rows[0]["text"] != "hello without fts" {
		t.Fatalf("message did not persist without fts: %#v", rows)
	}
	rows, _ = w.db.Query("select count(*) as n from whatsapp_messages_fts where id=?", res.ID)
	if rows[0]["n"].(int64) != 0 {
		t.Fatalf("fts should have been skipped: %#v", rows)
	}
}

func TestIsFTSUnavailable(t *testing.T) {
	for _, err := range []error{
		sqliteErr("no such module: fts5"),
		sqliteErr("no such table: whatsapp_messages_fts"),
	} {
		if !isFTSUnavailable(err) {
			t.Fatalf("expected unavailable fts error: %v", err)
		}
	}
	if isFTSUnavailable(sqliteErr("database is locked")) {
		t.Fatal("non-fts error should not be ignored")
	}
}

type sqliteErr string

func (e sqliteErr) Error() string { return string(e) }

func TestNewMigratesExistingHydrationColumns(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "openclaw.db")
	db, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.ExecScript(`
create table whatsapp_groups (
  jid text primary key,
  label text not null,
  updated_at integer not null
);

create table whatsapp_messages (
  id text primary key,
  source_pk integer not null,
  chat_jid text not null,
  chat_label text not null,
  msg_id text not null,
  sender_jid text,
  sender_name text,
  ts integer not null,
  from_me integer not null,
  text text,
  raw_type integer not null,
  message_type text,
  media_type text,
  media_title text,
  media_path text,
  text_hash text not null,
  ingested_at integer not null,
  foreign key(chat_jid) references whatsapp_groups(jid)
);
`); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()

	w, err := New(dbPath, filepath.Join(dir, "media"))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	rows, err := w.db.Query("pragma table_info(whatsapp_messages)")
	if err != nil {
		t.Fatal(err)
	}
	have := map[string]bool{}
	for _, row := range rows {
		have[row["name"].(string)] = true
	}
	for _, name := range []string{
		"media_url", "media_size", "deleted_or_missing", "media_local_path",
		"media_extracted_text", "media_extracted_by", "media_hydrated_at",
		"media_hydration_status", "media_hydration_attempts", "media_surfaced_at",
	} {
		if !have[name] {
			t.Fatalf("missing migrated column %s", name)
		}
	}
	if _, err := w.db.Query("select count(*) as n from whatsapp_messages_fts"); err != nil {
		t.Fatalf("fts table was not created after migration: %v", err)
	}
}
