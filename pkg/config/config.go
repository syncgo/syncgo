package config

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/syncgo/syncgo/pkg/gzip"

	"gopkg.in/yaml.v3"
)

const (
	defaultConfigPath = "/etc/syncgo/config.yaml"

	elasticsearch = "elasticsearch"
	opensearch    = "opensearch"

	defaultBatcherSize          = 100
	defaultBatcherFlushInterval = 50 * time.Millisecond

	defaultFlushMaxRetries   = 3
	defaultFlushRetryTimeout = time.Second
)

type Config struct {
	PostgreSQL   PostgreSQLConfig   `yaml:"postgresql"`
	SearchEngine SearchEngineConfig `yaml:"search_engine"`
	Metrics      MetricsConfig      `yaml:"metrics"`
	Batcher      BatcherConfig      `yaml:"batcher"`
}

type OpenSearchGRPCConfig struct {
	Host string `yaml:"host"`
	Port int    `yaml:"port"`
}

type MetricsConfig struct {
	Port int `yaml:"port"`
}

type BatcherConfig struct {
	Size              int           `yaml:"size"`
	FlushInterval     time.Duration `yaml:"flush_interval"`
	FlushMaxRetries   int           `yaml:"flush_max_retries"`
	FlushRetryTimeout time.Duration `yaml:"flush_retry_timeout"`
}

type PostgreSQLConfig struct {
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Host     string `yaml:"host"`
	Port     string `yaml:"port"`
	Database string `yaml:"database"`

	PublicationName string `yaml:"publication_name"`
	SlotName        string `yaml:"slot_name"`
	IDColumn        string `yaml:"id_column"`
}

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

	if c.Batcher.FlushMaxRetries <= 0 {
		c.Batcher.FlushMaxRetries = defaultFlushMaxRetries
	}

	if c.Batcher.FlushRetryTimeout <= 0 {
		c.Batcher.FlushRetryTimeout = defaultFlushRetryTimeout
	}

	if c.PostgreSQL.SlotName == "" {
		return fmt.Errorf("postgresql.slot_name is required")
	}

	if c.PostgreSQL.PublicationName == "" {
		return fmt.Errorf("postgresql.publication_name is required")
	}

	if c.PostgreSQL.IDColumn == "" {
		c.PostgreSQL.IDColumn = "id"
	}

	return nil
}

func (c *SearchEngineConfig) UseGRPC() bool {
	return c != nil && c.Name == opensearch && c.GRPC != nil && c.GRPC.Host != "" && c.GRPC.Port > 0
}

func LoadFromYAML(configPath string) (*Config, error) {
	if configPath == "" {
		configPath = defaultConfigPath
	}

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
