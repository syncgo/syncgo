package state

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoad_MissingFile(t *testing.T) {
	s, err := Load(filepath.Join(t.TempDir(), "missing.json"))
	if err != nil {
		t.Fatalf("Load on missing file should not fail; error: %v", err)
	}
	if s != nil {
		t.Fatalf("Load on missing file should return nil state, got %+v", s)
	}
}

func TestSaveAndLoad_Roundtrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")

	want := &State{SlotName: "sync_slot", ConsistentPoint: "0/16B3748"}
	if err := Save(path, want); err != nil {
		t.Fatalf("Save failed; error: %v", err)
	}

	got, err := Load(path)
	if err != nil {
		t.Fatalf("Load failed; error: %v", err)
	}
	if got == nil {
		t.Fatal("Load returned nil state after Save")
	}
	if got.SlotName != want.SlotName || got.ConsistentPoint != want.ConsistentPoint {
		t.Fatalf("roundtrip mismatch: got %+v, want %+v", got, want)
	}
}

func TestSave_CreatesMissingDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sub", "dir", "state.json")

	if err := Save(path, &State{SlotName: "s", ConsistentPoint: "0/1"}); err != nil {
		t.Fatalf("Save into missing directory failed; error: %v", err)
	}

	s, err := Load(path)
	if err != nil || s == nil {
		t.Fatalf("Load after Save failed; state: %+v, error: %v", s, err)
	}
}

func TestSave_OverwritesExisting(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")

	if err := Save(path, &State{SlotName: "s", ConsistentPoint: "0/1"}); err != nil {
		t.Fatalf("first Save failed; error: %v", err)
	}
	if err := Save(path, &State{SlotName: "s", ConsistentPoint: "0/2"}); err != nil {
		t.Fatalf("second Save failed; error: %v", err)
	}

	s, err := Load(path)
	if err != nil {
		t.Fatalf("Load failed; error: %v", err)
	}
	if s.ConsistentPoint != "0/2" {
		t.Fatalf("expected updated consistent point 0/2, got %q", s.ConsistentPoint)
	}
}

func TestLoad_InvalidJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	if err := os.WriteFile(path, []byte("{not json"), 0o644); err != nil {
		t.Fatalf("failed to write test file; error: %v", err)
	}

	if _, err := Load(path); err == nil {
		t.Fatal("Load on invalid JSON should fail")
	}
}

func TestPathFromConfig(t *testing.T) {
	tests := []struct {
		configPath string
		want       string
	}{
		{"config.yaml", "syncgo_state.json"},
		{"/etc/syncgo/config.yaml", "/etc/syncgo/syncgo_state.json"},
		{"e2e/config.yaml", "e2e/syncgo_state.json"},
	}

	for _, tt := range tests {
		if got := PathFromConfig(tt.configPath); got != tt.want {
			t.Errorf("PathFromConfig(%q) = %q, want %q", tt.configPath, got, tt.want)
		}
	}
}
