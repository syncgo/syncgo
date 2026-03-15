package config

import (
	"os"
	"strings"
	"testing"
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
				SearchEngine: SearchEngineConfig{Name: elasticsearch, Addresses: []string{"http://localhost:9200"}},
			},
			expectError: false,
		},
		{
			name: "valid config with opensearch",
			config: Config{
				SearchEngine: SearchEngineConfig{Name: opensearch, Addresses: []string{"http://localhost:9300"}},
			},
			expectError: false,
		},
		{
			name: "invalid config - opensearch with no addresses",
			config: Config{
				SearchEngine: SearchEngineConfig{Name: elasticsearch, Addresses: nil},
			},
			expectError: true,
			errorMsg:    "must be configured at least one address",
		},
		{
			name: "no search engine name",
			config: Config{
				SearchEngine: SearchEngineConfig{Addresses: nil},
			},
			expectError: true,
			errorMsg:    `search engine name should be "elasticsearh" or "opensearch"`,
		},
		{
			name: "wrong search engine name",
			config: Config{
				SearchEngine: SearchEngineConfig{Name: "test"},
			},
			expectError: true,
			errorMsg:    `search engine name should be "elasticsearh" or "opensearch"`,
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
search_engine:
  name: elasticsearh
  addresses:
    - http://localhost:9200
  username: admin
  password: secret
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
search_engine:
  name: opensearch
  addresses:
    - http://localhost:9300
    - http://localhost:9301
  username: opensearch
  password: opensearch123
  api_key: test-api-key
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
  addresses:
    - http://localhost:9200
opensearch:
  addresses:
    - http://localhost:9300
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
search_engine:
  name: elasticsearh
  addresses:
    - http://localhost:9200
  username: admin
  password: secret
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
