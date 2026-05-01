package sqlite

/*
#cgo darwin LDFLAGS: -lsqlite3
#cgo linux LDFLAGS: -lsqlite3
#include <stdlib.h>
#include <sqlite3.h>

static int bind_text_transient(sqlite3_stmt *stmt, int idx, const char *val, int n) {
	return sqlite3_bind_text(stmt, idx, val, n, SQLITE_TRANSIENT);
}

static int bind_blob_transient(sqlite3_stmt *stmt, int idx, const void *val, int n) {
	return sqlite3_bind_blob(stmt, idx, val, n, SQLITE_TRANSIENT);
}
*/
import "C"

import (
	"errors"
	"fmt"
	"sync"
	"unsafe"
)

type DB struct {
	mu sync.Mutex
	db *C.sqlite3
}

func Open(path string) (*DB, error) {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	var handle *C.sqlite3
	flags := C.SQLITE_OPEN_READWRITE | C.SQLITE_OPEN_CREATE | C.SQLITE_OPEN_FULLMUTEX
	if rc := C.sqlite3_open_v2(cpath, &handle, C.int(flags), nil); rc != C.SQLITE_OK {
		msg := C.GoString(C.sqlite3_errmsg(handle))
		if handle != nil {
			C.sqlite3_close(handle)
		}
		return nil, errors.New(msg)
	}
	db := &DB{db: handle}
	if err := db.Exec("pragma busy_timeout = 5000"); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.db == nil {
		return nil
	}
	if rc := C.sqlite3_close(db.db); rc != C.SQLITE_OK {
		return errors.New(C.GoString(C.sqlite3_errmsg(db.db)))
	}
	db.db = nil
	return nil
}

func (db *DB) ExecScript(script string) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.execScript(script)
}

func (db *DB) Exec(query string, args ...any) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	_, err := db.query(query, args...)
	return err
}

func (db *DB) Query(query string, args ...any) ([]map[string]any, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.query(query, args...)
}

func (db *DB) WithTx(fn func() error) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := db.execScript("begin immediate"); err != nil {
		return err
	}
	if err := fn(); err != nil {
		_ = db.execScript("rollback")
		return err
	}
	return db.execScript("commit")
}

func (db *DB) ExecTx(query string, args ...any) error {
	_, err := db.query(query, args...)
	return err
}

func (db *DB) QueryTx(query string, args ...any) ([]map[string]any, error) {
	return db.query(query, args...)
}

func (db *DB) execScript(script string) error {
	csql := C.CString(script)
	defer C.free(unsafe.Pointer(csql))
	var errmsg *C.char
	if rc := C.sqlite3_exec(db.db, csql, nil, nil, &errmsg); rc != C.SQLITE_OK {
		defer C.sqlite3_free(unsafe.Pointer(errmsg))
		return errors.New(C.GoString(errmsg))
	}
	return nil
}

func (db *DB) query(query string, args ...any) ([]map[string]any, error) {
	csql := C.CString(query)
	defer C.free(unsafe.Pointer(csql))
	var stmt *C.sqlite3_stmt
	if rc := C.sqlite3_prepare_v2(db.db, csql, -1, &stmt, nil); rc != C.SQLITE_OK {
		return nil, errors.New(C.GoString(C.sqlite3_errmsg(db.db)))
	}
	defer C.sqlite3_finalize(stmt)
	for i, arg := range args {
		if err := bind(stmt, i+1, arg); err != nil {
			return nil, err
		}
	}
	var out []map[string]any
	for {
		rc := C.sqlite3_step(stmt)
		switch rc {
		case C.SQLITE_ROW:
			n := int(C.sqlite3_column_count(stmt))
			row := make(map[string]any, n)
			for i := 0; i < n; i++ {
				name := C.GoString(C.sqlite3_column_name(stmt, C.int(i)))
				row[name] = column(stmt, i)
			}
			out = append(out, row)
		case C.SQLITE_DONE:
			return out, nil
		default:
			return nil, errors.New(C.GoString(C.sqlite3_errmsg(db.db)))
		}
	}
}

func bind(stmt *C.sqlite3_stmt, idx int, arg any) error {
	var rc C.int
	switch v := arg.(type) {
	case nil:
		rc = C.sqlite3_bind_null(stmt, C.int(idx))
	case bool:
		if v {
			rc = C.sqlite3_bind_int64(stmt, C.int(idx), 1)
		} else {
			rc = C.sqlite3_bind_int64(stmt, C.int(idx), 0)
		}
	case int:
		rc = C.sqlite3_bind_int64(stmt, C.int(idx), C.sqlite3_int64(v))
	case int64:
		rc = C.sqlite3_bind_int64(stmt, C.int(idx), C.sqlite3_int64(v))
	case string:
		cs := C.CString(v)
		defer C.free(unsafe.Pointer(cs))
		rc = C.bind_text_transient(stmt, C.int(idx), cs, C.int(len(v)))
	case []byte:
		if len(v) == 0 {
			rc = C.bind_blob_transient(stmt, C.int(idx), nil, 0)
		} else {
			rc = C.bind_blob_transient(stmt, C.int(idx), unsafe.Pointer(&v[0]), C.int(len(v)))
		}
	default:
		return fmt.Errorf("unsupported sqlite bind type %T", arg)
	}
	if rc != C.SQLITE_OK {
		return fmt.Errorf("sqlite bind %d failed: %d", idx, int(rc))
	}
	return nil
}

func column(stmt *C.sqlite3_stmt, idx int) any {
	i := C.int(idx)
	switch C.sqlite3_column_type(stmt, i) {
	case C.SQLITE_NULL:
		return nil
	case C.SQLITE_INTEGER:
		return int64(C.sqlite3_column_int64(stmt, i))
	case C.SQLITE_FLOAT:
		return float64(C.sqlite3_column_double(stmt, i))
	case C.SQLITE_BLOB:
		n := C.sqlite3_column_bytes(stmt, i)
		ptr := C.sqlite3_column_blob(stmt, i)
		return C.GoBytes(ptr, n)
	default:
		n := C.sqlite3_column_bytes(stmt, i)
		ptr := C.sqlite3_column_text(stmt, i)
		return C.GoStringN((*C.char)(unsafe.Pointer(ptr)), n)
	}
}
