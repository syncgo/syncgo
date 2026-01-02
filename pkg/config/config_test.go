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
			name: "valid config with IsElastic true",
			config: Config{
				IsElastic:    true,
				IsOpenSearch: false,
			},
			expectError: false,
		},
		{
			name: "valid config with IsOpenSearch true",
			config: Config{
				IsElastic:    false,
				IsOpenSearch: true,
			},
			expectError: false,
		},
		{
			name: "invalid config - both true",
			config: Config{
				IsElastic:    true,
				IsOpenSearch: true,
			},
			expectError: true,
			errorMsg:    "both IS_ELASTIC and IS_OPENSEARCH cannot be true at the same time",
		},
		{
			name: "invalid config - both false",
			config: Config{
				IsElastic:    false,
				IsOpenSearch: false,
			},
			expectError: true,
			errorMsg:    "either IS_ELASTIC or IS_OPENSEARCH must be true",
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
			name: "valid config with IsElastic",
			yamlContent: `
postgresql:
  user: testuser
  password: testpass
  host: localhost
  port: "5432"
  database: testdb
search:
  addresses:
    - http://localhost:9200
  username: admin
  password: secret
is_elastic: true
is_opensearch: false
`,
			expectError: false,
		},
		{
			name: "valid config with IsOpenSearch",
			yamlContent: `
postgresql:
  user: pguser
  password: pgpass
  host: db.example.com
  port: "5432"
  database: mydb
search:
  addresses:
    - http://localhost:9300
    - http://localhost:9301
  username: opensearch
  password: opensearch123
  api_key: test-api-key
is_elastic: false
is_opensearch: true
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
			name: "invalid config - both flags true",
			yamlContent: `
postgresql:
  user: testuser
  password: testpass
  host: localhost
  port: "5432"
  database: testdb
search:
  addresses:
    - http://localhost:9200
  username: admin
  password: secret
is_elastic: true
is_opensearch: true
`,
			expectError: true,
			errorMsg:    "config validation failed",
		},
		{
			name: "invalid config - both flags false",
			yamlContent: `
postgresql:
  user: testuser
  password: testpass
  host: localhost
  port: "5432"
  database: testdb
search:
  addresses:
    - http://localhost:9200
  username: admin
  password: secret
is_elastic: false
is_opensearch: false
`,
			expectError: true,
			errorMsg:    "config validation failed",
		},
		{
			name:        "file not found",
			path:        "nonexistent-config.yaml",
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
search:
  addresses:
    - http://localhost:9200
  username: admin
  password: secret
is_elastic: true
is_opensearch: false
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
						os.Remove(testPath)
					}
				} else {
					tmpFile, err := os.CreateTemp("", "test-config-*.yaml")
					if err != nil {
						t.Fatalf("failed to create temp file: %v", err)
					}
					testPath = tmpFile.Name()
					tmpFile.Close()

					err = os.WriteFile(testPath, []byte(tt.yamlContent), 0644)
					if err != nil {
						os.Remove(testPath)
						t.Fatalf("failed to write test file: %v", err)
					}
					cleanup = func() {
						os.Remove(testPath)
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
