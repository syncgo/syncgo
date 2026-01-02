package elasticsearch

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	// Skip if no Elasticsearch available (for CI, we'll use service containers)
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Get test server address from environment or use default
	esAddr := os.Getenv("ELASTICSEARCH_ADDRESS")
	if esAddr == "" {
		esAddr = "http://localhost:9200"
	}

	esUser := os.Getenv("ELASTICSEARCH_USER")
	if esUser == "" {
		esUser = "admin"
	}

	esPassword := os.Getenv("ELASTICSEARCH_PASSWORD")
	if esPassword == "" {
		esPassword = "Es123456"
	}

	tests := []struct {
		name    string
		config  Config
		wantErr bool
	}{
		{
			name: "valid connection with username and password",
			config: Config{
				Addresses: []string{esAddr},
				Username:  esUser,
				Password:  esPassword,
			},
			wantErr: false,
		},
		{
			name: "valid connection without auth (if security disabled)",
			config: Config{
				Addresses: []string{esAddr},
			},
			wantErr: false,
		},
		{
			name: "invalid address",
			config: Config{
				Addresses: []string{"http://invalid-host:9200"},
				Username:  esUser,
				Password:  esPassword,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := New(ctx, tt.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("New() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if client != nil {
				// Client doesn't have explicit Close, but we can verify it's not nil
				_ = client
			}
		})
	}
}

func TestNew_WithAPIKey(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	esAddr := os.Getenv("ELASTICSEARCH_ADDRESS")
	if esAddr == "" {
		esAddr = "http://localhost:9200"
	}

	apiKey := os.Getenv("ELASTICSEARCH_API_KEY")
	if apiKey == "" {
		t.Skip("ELASTICSEARCH_API_KEY not set, skipping API key test")
	}

	config := Config{
		Addresses: []string{esAddr},
		APIKey:    apiKey,
	}

	client, err := New(ctx, config)
	if err != nil {
		t.Fatalf("New() with API key error = %v", err)
	}
	if client == nil {
		t.Error("New() returned nil client")
	}
}

func TestNew_WithCloudID(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cloudID := os.Getenv("ELASTICSEARCH_CLOUD_ID")
	if cloudID == "" {
		t.Skip("ELASTICSEARCH_CLOUD_ID not set, skipping Cloud ID test")
	}

	config := Config{
		CloudID: cloudID,
	}

	client, err := New(ctx, config)
	if err != nil {
		t.Fatalf("New() with CloudID error = %v", err)
	}
	if client == nil {
		t.Error("New() returned nil client")
	}
}
