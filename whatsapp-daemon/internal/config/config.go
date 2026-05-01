package config

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"openclaw/whatsapp-daemon/internal/paths"
)

type Group struct {
	JID                 string   `json:"jid"`
	Label               string   `json:"label"`
	Tenants             []string `json:"tenants"`
	Purpose             string   `json:"purpose"`
	IngestText          bool     `json:"ingest_text"`
	IngestMediaMetadata bool     `json:"ingest_media_metadata"`
	CopyMedia           bool     `json:"copy_media"`
	IndexEmbeddings     bool     `json:"index_embeddings"`
	RetentionDays       *int     `json:"retention_days"`
}

type fileConfig struct {
	Enabled bool       `json:"enabled"`
	Groups  []rawGroup `json:"groups"`
}

type rawGroup struct {
	JID                 string    `json:"jid"`
	Label               string    `json:"label"`
	Tenants             *[]string `json:"tenants"`
	Purpose             string    `json:"purpose"`
	IngestText          *bool     `json:"ingest_text"`
	IngestMediaMetadata *bool     `json:"ingest_media_metadata"`
	CopyMedia           *bool     `json:"copy_media"`
	IndexEmbeddings     *bool     `json:"index_embeddings"`
	RetentionDays       *int      `json:"retention_days"`
}

type Change struct {
	Added []Group
}

type Manager struct {
	path    string
	mu      sync.RWMutex
	groups  map[string]Group
	modTime time.Time
	subs    []chan Change
}

func New(path string) (*Manager, error) {
	m := &Manager{path: path, groups: map[string]Group{}}
	if err := m.Reload(); err != nil {
		return nil, err
	}
	return m, nil
}

func Load(path string) ([]Group, time.Time, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, time.Time{}, err
	}
	var fc fileConfig
	if err := json.Unmarshal(b, &fc); err != nil {
		return nil, time.Time{}, err
	}
	out := make([]Group, 0, len(fc.Groups))
	seenJID := map[string]bool{}
	seenLabel := map[string]bool{}
	if !fc.Enabled {
		fc.Groups = nil
	}
	for _, raw := range fc.Groups {
		jid := strings.TrimSpace(raw.JID)
		label := strings.TrimSpace(raw.Label)
		if jid == "" || label == "" {
			return nil, time.Time{}, fmt.Errorf("every group needs jid and label")
		}
		if !strings.HasSuffix(jid, "@g.us") {
			return nil, time.Time{}, fmt.Errorf("refusing non-group JID: %s", jid)
		}
		if seenJID[jid] {
			return nil, time.Time{}, fmt.Errorf("duplicate group jid: %s", jid)
		}
		if seenLabel[label] {
			return nil, time.Time{}, fmt.Errorf("duplicate group label: %s", label)
		}
		if raw.Tenants == nil {
			return nil, time.Time{}, fmt.Errorf("group %s: tenants must be set explicitly (e.g. [\"family\"])", jid)
		}
		tenants, err := normalizeTenants(*raw.Tenants)
		if err != nil {
			return nil, time.Time{}, fmt.Errorf("group %s tenants: %w", jid, err)
		}
		seenJID[jid] = true
		seenLabel[label] = true
		out = append(out, Group{
			JID:                 jid,
			Label:               label,
			Tenants:             tenants,
			Purpose:             raw.Purpose,
			IngestText:          boolDefault(raw.IngestText, true),
			IngestMediaMetadata: boolDefault(raw.IngestMediaMetadata, true),
			CopyMedia:           boolDefault(raw.CopyMedia, false),
			IndexEmbeddings:     boolDefault(raw.IndexEmbeddings, false),
			RetentionDays:       raw.RetentionDays,
		})
	}
	st, err := os.Stat(path)
	if err != nil {
		return nil, time.Time{}, err
	}
	return out, st.ModTime(), nil
}

func (m *Manager) Reload() error {
	groups, mod, err := Load(m.path)
	if err != nil {
		return err
	}
	next := make(map[string]Group, len(groups))
	for _, g := range groups {
		next[g.JID] = g
	}
	m.mu.Lock()
	var added []Group
	changed := len(next) != len(m.groups)
	for jid, g := range next {
		old, ok := m.groups[jid]
		if !ok {
			added = append(added, g)
			changed = true
		} else if !sameGroup(old, g) {
			changed = true
		}
	}
	m.groups = next
	m.modTime = mod
	subs := append([]chan Change(nil), m.subs...)
	m.mu.Unlock()
	if changed {
		ch := Change{Added: added}
		for _, sub := range subs {
			select {
			case sub <- ch:
			default:
			}
		}
	}
	return nil
}

func (m *Manager) Watch(ctx context.Context, interval time.Duration, onError func(error)) {
	if interval <= 0 {
		interval = 2 * time.Second
	}
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			st, err := os.Stat(m.path)
			if err != nil {
				onError(err)
				continue
			}
			m.mu.RLock()
			changed := st.ModTime().After(m.modTime)
			m.mu.RUnlock()
			if changed {
				if err := m.Reload(); err != nil {
					onError(err)
				}
			}
		}
	}
}

func (m *Manager) Subscribe() <-chan Change {
	ch := make(chan Change, 4)
	m.mu.Lock()
	m.subs = append(m.subs, ch)
	m.mu.Unlock()
	return ch
}

func (m *Manager) IsAllowed(jid string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.groups[jid]
	return ok
}

func (m *Manager) Group(jid string) (Group, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	g, ok := m.groups[jid]
	return g, ok
}

func (m *Manager) Groups() []Group {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]Group, 0, len(m.groups))
	for _, g := range m.groups {
		out = append(out, g)
	}
	return out
}

func (m *Manager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.groups)
}

func DefaultPath(home string) string {
	return filepath.Join(paths.Root(home), "config", "whatsapp_groups.json")
}

func boolDefault(v *bool, fallback bool) bool {
	if v == nil {
		return fallback
	}
	return *v
}

func normalizeTenants(values []string) ([]string, error) {
	if len(values) == 0 {
		return nil, fmt.Errorf("must not be empty")
	}
	out := make([]string, 0, len(values))
	seen := map[string]bool{}
	for _, value := range values {
		tenant := strings.TrimSpace(value)
		if tenant == "" {
			return nil, fmt.Errorf("contains empty tenant")
		}
		if seen[tenant] {
			return nil, fmt.Errorf("duplicate tenant %q", tenant)
		}
		seen[tenant] = true
		out = append(out, tenant)
	}
	return out, nil
}

func (g Group) HasTenant(tenant string) bool {
	for _, t := range g.Tenants {
		if t == tenant {
			return true
		}
	}
	return false
}

func sameGroup(a, b Group) bool {
	if a.JID != b.JID || a.Label != b.Label || a.Purpose != b.Purpose ||
		a.IngestText != b.IngestText || a.IngestMediaMetadata != b.IngestMediaMetadata ||
		a.CopyMedia != b.CopyMedia || a.IndexEmbeddings != b.IndexEmbeddings {
		return false
	}
	if !sameStrings(a.Tenants, b.Tenants) {
		return false
	}
	if a.RetentionDays == nil || b.RetentionDays == nil {
		return a.RetentionDays == nil && b.RetentionDays == nil
	}
	return *a.RetentionDays == *b.RetentionDays
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
