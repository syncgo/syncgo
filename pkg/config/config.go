package config

import (
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

const defaultConfigPath = "config.yaml"

// Config holds the complete application configuration
type Config struct {
	PostgreSQL   PostgreSQLConfig `yaml:"postgresql"`
	Search       SearchConfig     `yaml:"search"`
	IsElastic    bool             `yaml:"is_elastic"`
	IsOpenSearch bool             `yaml:"is_opensearch"`
}

// PostgreSQLConfig holds PostgreSQL connection configuration
type PostgreSQLConfig struct {
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Host     string `yaml:"host"`
	Port     string `yaml:"port"`
	Database string `yaml:"database"`
}

// SearchConfig holds Elasticsearch/OpenSearch connection configuration
// This is a unified config since ES and OpenSearch use the same parameters
type SearchConfig struct {
	Addresses []string `yaml:"addresses"`
	Username  string   `yaml:"username"`
	Password  string   `yaml:"password"`
	APIKey    string   `yaml:"api_key"`
	CloudID   string   `yaml:"cloud_id"`
	Index     string   `yaml:"index"`
}

// Validate checks that the configuration is valid
func (c *Config) Validate() error {
	if c.IsElastic && c.IsOpenSearch {
		return fmt.Errorf("both IS_ELASTIC and IS_OPENSEARCH cannot be true at the same time")
	}

	if !c.IsElastic && !c.IsOpenSearch {
		return fmt.Errorf("either IS_ELASTIC or IS_OPENSEARCH must be true")
	}

	return nil
}

// LoadFromYAML loads configuration from a YAML file
func LoadFromYAML(configPath string) (*Config, error) {
	if configPath == "" {
		configPath = defaultConfigPath
	}

	// Normalize the path (resolve relative paths, clean up separators)
	configPath = filepath.Clean(configPath)

	if _, err := os.Stat(configPath); err != nil {
		return nil, fmt.Errorf("config file not found: %s", configPath)
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse YAML: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	return &cfg, nil
}
