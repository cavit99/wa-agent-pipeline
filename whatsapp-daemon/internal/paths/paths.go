package paths

import (
	"os"
	"path/filepath"
	"strings"
)

const (
	EnvRoot       = "WA_AGENT_PIPELINE_HOME"
	DefaultDBName = "wa_pipeline.db"
)

func Root(home string) string {
	if value := strings.TrimSpace(os.Getenv(EnvRoot)); value != "" {
		return cleanRoot(value, home)
	}
	return filepath.Join(home, "wa-agent-pipeline")
}

func DefaultDB(root string) string {
	return filepath.Join(root, "db", DefaultDBName)
}

func cleanRoot(value, home string) string {
	if value == "~" {
		return home
	}
	if strings.HasPrefix(value, "~/") {
		return filepath.Join(home, strings.TrimPrefix(value, "~/"))
	}
	return filepath.Clean(value)
}
