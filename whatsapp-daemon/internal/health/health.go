package health

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Logger struct {
	mu sync.Mutex
	l  *log.Logger
	f  *os.File
}

func NewLogger(path string) (*Logger, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	return &Logger{l: log.New(f, "", log.LstdFlags|log.LUTC), f: f}, nil
}

func (l *Logger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f == nil {
		return nil
	}
	err := l.f.Close()
	l.f = nil
	return err
}

func (l *Logger) Printf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.l != nil {
		l.l.Printf(format, args...)
	}
}

type Status struct {
	path string
	mu   sync.Mutex

	Connected     bool  `json:"connected"`
	LastMessageAt int64 `json:"last_message_at"`
	QueueDepth    int   `json:"queue_depth"`
	IPCListening  bool  `json:"ipc_listening,omitempty"`

	mediaFailures []time.Time
}

func NewStatus(path string) *Status {
	return &Status{path: path}
}

func (s *Status) SetConnected(v bool) {
	s.mu.Lock()
	s.Connected = v
	s.mu.Unlock()
}

func (s *Status) SetQueueDepth(n int) {
	s.mu.Lock()
	s.QueueDepth = n
	s.mu.Unlock()
}

func (s *Status) SetIPCListening(v bool) {
	s.mu.Lock()
	s.IPCListening = v
	s.mu.Unlock()
}

func (s *Status) MessageAt(t time.Time) {
	s.mu.Lock()
	s.LastMessageAt = t.Unix()
	s.mu.Unlock()
}

func (s *Status) MediaFailure(t time.Time) {
	s.mu.Lock()
	s.mediaFailures = append(s.mediaFailures, t)
	s.mu.Unlock()
}

func (s *Status) Write() error {
	s.mu.Lock()
	s.pruneLocked(time.Now().Add(-5 * time.Minute))
	body := struct {
		LastMessageAt   int64 `json:"last_message_at"`
		Connected       bool  `json:"connected"`
		QueueDepth      int   `json:"queue_depth"`
		IPCListening    bool  `json:"ipc_listening,omitempty"`
		MediaFailures5m int   `json:"media_failures_5m"`
		UpdatedAt       int64 `json:"updated_at"`
	}{s.LastMessageAt, s.Connected, s.QueueDepth, s.IPCListening, len(s.mediaFailures), time.Now().Unix()}
	s.mu.Unlock()
	b, err := json.MarshalIndent(body, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, append(b, '\n'), 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

func (s *Status) Run(stop <-chan struct{}, interval time.Duration, logger *Logger) {
	if interval <= 0 {
		interval = time.Minute
	}
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-stop:
			return
		case <-tick.C:
			if err := s.Write(); err != nil && logger != nil {
				logger.Printf("health_write_failed error=%q", err.Error())
			}
		}
	}
}

func (s *Status) pruneLocked(cutoff time.Time) {
	keep := s.mediaFailures[:0]
	for _, t := range s.mediaFailures {
		if t.After(cutoff) {
			keep = append(keep, t)
		}
	}
	s.mediaFailures = keep
}

func LogConnection(l *Logger, state string) {
	if l != nil {
		l.Printf("connection_state state=%s", state)
	}
}

func LogMediaFailure(l *Logger, chatJID, msgID string, err error) {
	if l != nil {
		l.Printf("media_download_failed chat_jid=%s msg_id=%s error=%q", chatJID, msgID, fmt.Sprint(err))
	}
}
