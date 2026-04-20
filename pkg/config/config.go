package config

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/romanchechyotkin/syncgo/pkg/gzip"
	"gopkg.in/yaml.v3"
)

const (
	defaultConfigPath = "config.yaml"

	elasticsearch = "elasticsearch"
	opensearch    = "opensearch"

	defaultBatcherSize          = 100
	defaultBatcherFlushInterval = 50 * time.Millisecond
)

// Config holds the complete application configuration
type Config struct {
	PostgreSQL   PostgreSQLConfig   `yaml:"postgresql"`
	SearchEngine SearchEngineConfig `yaml:"search_engine"`
	Metrics      MetricsConfig      `yaml:"metrics"`
	Batcher      BatcherConfig      `yaml:"batcher"`
}

// OpenSearchGRPCConfig holds gRPC connection settings for OpenSearch.
type OpenSearchGRPCConfig struct {
	Host string `yaml:"host"`
	Port int    `yaml:"port"`
}

// MetricsConfig holds configuration for the Prometheus metrics HTTP server
type MetricsConfig struct {
	// Port is the TCP port to expose /metrics on (e.g. 9090). If 0, metrics server is disabled.
	Port int `yaml:"port"`
}

// BatcherConfig holds configuration for the internal batcher
type BatcherConfig struct {
	// Size is the maximum number of items buffered before a flush is triggered.
	Size int `yaml:"size"`
	// FlushInterval is how long the batcher waits without any flush before
	// sending committed items automatically. 0 disables the timer-based flush.
	FlushInterval time.Duration `yaml:"flush_interval"`
}

// PostgreSQLConfig holds PostgreSQL connection configuration
type PostgreSQLConfig struct {
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Host     string `yaml:"host"`
	Port     string `yaml:"port"`
	Database string `yaml:"database"`
}

// SearchConfig holds connection configuration for Elasticsearch or OpenSearch (HTTP).
type SearchEngineConfig struct {
	Name              string                    `yaml:"name"`
	Address           string                    `yaml:"address"`
	Username          string                    `yaml:"username"`
	Password          string                    `yaml:"password"`
	APIKey            string                    `yaml:"api_key"`
	CloudID           string                    `yaml:"cloud_id"`
	Index             string                    `yaml:"index"`
	ConnectionTimeout time.Duration             `yaml:"connection_timeout"`
	GzipCompression   gzip.GzipCompressionLevel `yaml:"gzip_compression"`
	TLS               *SearchTLSConfig          `yaml:"tls"`
	KeepAlive         *SearchKeepAliveConfig    `yaml:"keep_alive"`
	GRPC              *OpenSearchGRPCConfig     `yaml:"grpc,omitempty"`
}

type SearchTLSConfig struct {
	CACert             string `yaml:"ca_cert"`
	InsecureSkipVerify bool   `yaml:"insecure_skip_verify"`
}

type SearchKeepAliveConfig struct {
	MaxConnDuration     time.Duration `yaml:"max_conn_duration"`
	MaxIdleConnDuration time.Duration `yaml:"max_idle_conn_duration"`
}

// Validate checks that the configuration is valid
func (c *Config) Validate() error {
	name := c.SearchEngine.Name

	if name != elasticsearch && name != opensearch {
		return fmt.Errorf(`search engine name should be "elasticsearh" or "opensearch"`)
	}

	if len(c.SearchEngine.Address) == 0 {
		return fmt.Errorf("address must be configured")
	}

	if c.SearchEngine.GRPC != nil && c.SearchEngine.Name != opensearch {
		return fmt.Errorf("setup grpc only for opensearch")
	}

	if c.Batcher.Size <= 0 {
		c.Batcher.Size = defaultBatcherSize
	}

	if c.Batcher.FlushInterval <= 0 {
		c.Batcher.FlushInterval = defaultBatcherFlushInterval
	}

	return nil
}

// UseGRPC returns true if OpenSearch is configured with gRPC (host and port set).
func (c *SearchEngineConfig) UseGRPC() bool {
	return c != nil && c.Name == opensearch && c.GRPC != nil && c.GRPC.Host != "" && c.GRPC.Port > 0
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
