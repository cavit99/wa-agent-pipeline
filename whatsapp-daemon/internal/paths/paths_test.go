package paths

import (
	"path/filepath"
	"testing"
)

func TestRootDefaultsToHomeWhatsappPipeline(t *testing.T) {
	t.Setenv(EnvRoot, "")
	got := Root("/Users/alice")
	want := filepath.Join("/Users/alice", "wa-agent-pipeline")
	if got != want {
		t.Fatalf("Root() = %q, want %q", got, want)
	}
}

func TestRootUsesEnvOverride(t *testing.T) {
	t.Setenv(EnvRoot, "/opt/wa-agent-pipeline")
	if got := Root("/Users/alice"); got != "/opt/wa-agent-pipeline" {
		t.Fatalf("Root() = %q", got)
	}
}

func TestRootExpandsTildeOverride(t *testing.T) {
	t.Setenv(EnvRoot, "~/wa-agent-pipeline")
	got := Root("/Users/alice")
	want := filepath.Join("/Users/alice", "wa-agent-pipeline")
	if got != want {
		t.Fatalf("Root() = %q, want %q", got, want)
	}
}
