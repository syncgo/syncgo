package config

import (
	"os"
	"strings"
	"testing"
	"time"
)

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name        string
		config      Config
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid config with elasticsearch",
			config: Config{
				PostgreSQL:   PostgreSQLConfig{SlotName: "test", PublicationName: "pglogrepl_demo"},
				SearchEngine: SearchEngineConfig{Name: elasticsearch, Address: "http://localhost:9200"},
				Batcher:      BatcherConfig{Size: 100},
			},
			expectError: false,
		},
		{
			name: "valid config with opensearch",
			config: Config{
				PostgreSQL:   PostgreSQLConfig{SlotName: "test", PublicationName: "pglogrepl_demo"},
				SearchEngine: SearchEngineConfig{Name: opensearch, Address: "http://localhost:9300"},
				Batcher:      BatcherConfig{Size: 100},
			},
			expectError: false,
		},
		{
			name: "invalid config - opensearch with no addresses",
			config: Config{
				PostgreSQL:   PostgreSQLConfig{SlotName: "test", PublicationName: "pglogrepl_demo"},
				SearchEngine: SearchEngineConfig{Name: elasticsearch, Address: ""},
				Batcher:      BatcherConfig{Size: 100},
			},
			expectError: true,
			errorMsg:    "address must be configured",
		},
		{
			name: "no search engine name",
			config: Config{
				PostgreSQL:   PostgreSQLConfig{SlotName: "test", PublicationName: "pglogrepl_demo"},
				SearchEngine: SearchEngineConfig{Address: ""},
				Batcher:      BatcherConfig{Size: 100},
			},
			expectError: true,
			errorMsg:    `search engine name should be "elasticsearh" or "opensearch"`,
		},
		{
			name: "wrong search engine name",
			config: Config{
				PostgreSQL:   PostgreSQLConfig{SlotName: "test", PublicationName: "pglogrepl_demo"},
				SearchEngine: SearchEngineConfig{Name: "test"},
				Batcher:      BatcherConfig{Size: 100},
			},
			expectError: true,
			errorMsg:    `search engine name should be "elasticsearh" or "opensearch"`,
		},
		{
			name: "grpc for elastic",
			config: Config{
				PostgreSQL:   PostgreSQLConfig{SlotName: "test", PublicationName: "pglogrepl_demo"},
				SearchEngine: SearchEngineConfig{Name: elasticsearch, Address: "test", GRPC: &OpenSearchGRPCConfig{Host: ""}},
				Batcher:      BatcherConfig{Size: 100},
			},
			expectError: true,
			errorMsg:    `setup grpc only for opensearch`,
		},
		{
			name: "batcher size zero defaults to 100",
			config: Config{
				PostgreSQL:   PostgreSQLConfig{SlotName: "test", PublicationName: "pglogrepl_demo"},
				SearchEngine: SearchEngineConfig{Name: elasticsearch, Address: "http://localhost:9200"},
				Batcher:      BatcherConfig{Size: 0},
			},
			expectError: false,
		},
		{
			name: "batcher size negative defaults to 100",
			config: Config{
				PostgreSQL:   PostgreSQLConfig{SlotName: "test", PublicationName: "pglogrepl_demo"},
				SearchEngine: SearchEngineConfig{Name: elasticsearch, Address: "http://localhost:9200"},
				Batcher:      BatcherConfig{Size: -1},
			},
			expectError: false,
		},
		{
			name: "batcher flush interval zero defaults to 50ms",
			config: Config{
				PostgreSQL:   PostgreSQLConfig{SlotName: "test", PublicationName: "pglogrepl_demo"},
				SearchEngine: SearchEngineConfig{Name: elasticsearch, Address: "http://localhost:9200"},
				Batcher:      BatcherConfig{Size: 100, FlushInterval: 0},
			},
			expectError: false,
		},
		{
			name: "missing slot_name",
			config: Config{
				PostgreSQL:   PostgreSQLConfig{PublicationName: "pglogrepl_demo"},
				SearchEngine: SearchEngineConfig{Name: elasticsearch, Address: "http://localhost:9200"},
				Batcher:      BatcherConfig{Size: 100},
			},
			expectError: true,
			errorMsg:    "postgresql.slot_name is required",
		},
		{
			name: "missing publication_name",
			config: Config{
				PostgreSQL:   PostgreSQLConfig{SlotName: "test"},
				SearchEngine: SearchEngineConfig{Name: elasticsearch, Address: "http://localhost:9200"},
				Batcher:      BatcherConfig{Size: 100},
			},
			expectError: true,
			errorMsg:    "postgresql.publication_name is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got nil")
					return
				}
				if err.Error() != tt.errorMsg {
					t.Errorf("expected error message %q, got %q", tt.errorMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestConfig_BatcherDefaults(t *testing.T) {
	cfg := &Config{
		PostgreSQL:   PostgreSQLConfig{SlotName: "wasd", PublicationName: "wasd"},
		SearchEngine: SearchEngineConfig{Name: elasticsearch, Address: "http://localhost:9200"},
		Batcher:      BatcherConfig{Size: 0, FlushInterval: 0},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Batcher.Size != 100 {
		t.Errorf("expected default size 100, got %d", cfg.Batcher.Size)
	}
	if cfg.Batcher.FlushInterval != 50*time.Millisecond {
		t.Errorf("expected default flush_interval 50ms, got %v", cfg.Batcher.FlushInterval)
	}
}

func TestLoadFromYAML(t *testing.T) {
	tests := []struct {
		name        string
		yamlContent string
		path        string
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid config with elasticsearch",
			yamlContent: `
postgresql:
  user: testuser
  password: testpass
  host: localhost
  port: "5432"
  database: testdb
  slot_name: test
  publication_name: pglogrepl_demo
search_engine:
  name: elasticsearch
  address: http://localhost:9200
  username: admin
  password: secret
batcher:
  size: 100
`,
			expectError: false,
		},
		{
			name: "valid config with opensearch",
			yamlContent: `
postgresql:
  user: pguser
  password: pgpass
  host: db.example.com
  port: "5432"
  database: mydb
  slot_name: test
  publication_name: pglogrepl_demo
search_engine:
  name: opensearch
  address: http://localhost:9300
  username: opensearch
  password: opensearch123
  api_key: test-api-key
batcher:
  size: 50
`,
			expectError: false,
		},
		{
			name: "invalid YAML syntax",
			yamlContent: `
postgresql:
  user: testuser
  password: testpass
  host: localhost
invalid: yaml: [syntax
`,
			expectError: true,
			errorMsg:    "failed to parse YAML",
		},
		{
			name: "invalid config - both elasticsearch and opensearch",
			yamlContent: `
postgresql:
  user: testuser
  password: testpass
  host: localhost
  port: "5432"
  database: testdb
elasticsearch:
  address: http://localhost:9200
opensearch:
  address: http://localhost:9300
`,
			expectError: true,
			errorMsg:    "config validation failed",
		},
		{
			name: "invalid config - neither elasticsearch nor opensearch",
			yamlContent: `
postgresql:
  user: testuser
  password: testpass
  host: localhost
  port: "5432"
  database: testdb
`,
			expectError: true,
			errorMsg:    "config validation failed",
		},
		{
			name:        "file not found",
			path:        "nonexistent-config_opensearch.yaml",
			expectError: true,
			errorMsg:    "config file not found",
		},
		{
			name: "empty path uses default",
			yamlContent: `
postgresql:
  user: testuser
  password: testpass
  host: localhost
  port: "5432"
  database: testdb
  slot_name: test
  publication_name: pglogrepl_demo
search_engine:
  name: elasticsearch
  address: http://localhost:9200
  username: admin
  password: secret
batcher:
  size: 100
`,
			path:        "",
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var testPath string
			var cleanup func()

			if tt.path == "" && tt.yamlContent != "" {
				if tt.name == "empty path uses default" {
					testPath = defaultConfigPath
					err := os.WriteFile(testPath, []byte(tt.yamlContent), 0644)
					if err != nil {
						t.Fatalf("failed to create test file: %v", err)
					}
					cleanup = func() {
						if err := os.Remove(testPath); err != nil {
							t.Fatalf("failed to remove test file: %v", err)
						}
					}
				} else {
					tmpFile, err := os.CreateTemp("", "test-config-*.yaml")
					if err != nil {
						t.Fatalf("failed to create temp file: %v", err)
					}
					testPath = tmpFile.Name()
					if err := tmpFile.Close(); err != nil {
						t.Fatalf("failed to close temp file: %v", err)
					}

					err = os.WriteFile(testPath, []byte(tt.yamlContent), 0644)
					if err != nil {
						if err := os.Remove(testPath); err != nil {
							t.Fatalf("failed to remove test file: %v", err)
						}
						t.Fatalf("failed to write test file: %v", err)
					}
					cleanup = func() {
						if err := os.Remove(testPath); err != nil {
							t.Fatalf("failed to remove test file: %v", err)
						}
					}
				}
			} else if tt.path != "" {
				testPath = tt.path
				cleanup = func() {}
			} else {
				t.Fatal("test case must have either path or yamlContent")
			}

			defer cleanup()

			cfg, err := LoadFromYAML(testPath)

			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got nil")
					return
				}
				if tt.errorMsg != "" && !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("expected error message to contain %q, got %q", tt.errorMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
					return
				}
				if cfg == nil {
					t.Errorf("expected config but got nil")
					return
				}
			}
		})
	}
}
