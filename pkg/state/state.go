// Package state persists syncgo's runtime state (e.g. the replication slot's
// consistent point) in a JSON file, so the run command can resume the
// replication stream exactly where the prepare command left off.
package state

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// defaultFileName is the state file name used when the config path has no
// explicit directory or when a directory is given without a file name.
const defaultFileName = "syncgo_state.json"

// State is the persisted runtime state.
type State struct {
	// SlotName is the replication slot the consistent point belongs to.
	SlotName string `json:"slot_name"`
	// ConsistentPoint is the LSN at which the slot was created. The initial
	// data copy reads the database exactly at this point, so the replication
	// stream must start here to neither lose nor duplicate changes.
	ConsistentPoint string `json:"consistent_point"`
}

// PathFromConfig derives the state file path from the config file path: the
// state file lives next to the config file.
func PathFromConfig(configPath string) string {
	dir := filepath.Dir(configPath)
	if dir == "." || dir == "" {
		return defaultFileName
	}

	return filepath.Join(dir, defaultFileName)
}

// Load reads the state file. Returns (nil, nil) when the file does not
// exist — an absent state file is a normal situation for a fresh setup.
func Load(path string) (*State, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to read state file %q: %w", path, err)
	}

	var s State
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("failed to parse state file %q: %w", path, err)
	}

	return &s, nil
}

// Save writes the state file atomically: the file is written to a temporary
// file next to the target and then renamed, so a crash mid-write never
// leaves a truncated state file behind.
func Save(path string, s *State) error {
	if dir := filepath.Dir(path); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("failed to create state directory %q: %w", dir, err)
		}
	}

	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal state: %w", err)
	}

	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("failed to write state file %q: %w", tmp, err)
	}

	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("failed to rename state file %q: %w", path, err)
	}

	return nil
}
