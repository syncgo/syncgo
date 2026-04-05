package search_engine_client

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/romanchechyotkin/syncgo/pkg/search_engine_client/mocks"

	"go.uber.org/mock/gomock"
)

func TestNew(t *testing.T) {
	// Skip if no OpenSearch available (for CI, we'll use service containers)
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Get test server address from environment or use default
	osAddr := os.Getenv("OPENSEARCH_ADDRESS")
	if osAddr == "" {
		osAddr = "http://localhost:9300"
	}

	osUser := os.Getenv("OPENSEARCH_USER")
	if osUser == "" {
		osUser = "admin"
	}

	osPassword := os.Getenv("OPENSEARCH_PASSWORD")
	if osPassword == "" {
		osPassword = "Op3nS3arch!"
	}

	tests := []struct {
		name    string
		config  Config
		wantErr bool
	}{
		{
			name: "valid connection with username and password",
			config: Config{
				Address:  osAddr,
				Username: osUser,
				Password: osPassword,
			},
			wantErr: false,
		},
		{
			name: "valid connection without auth (if security disabled)",
			config: Config{
				Address: osAddr,
			},
			wantErr: false,
		},
		{
			name: "invalid address",
			config: Config{
				Address:  "http://invalid-host:9300",
				Username: osUser,
				Password: osPassword,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			monitoring := mocks.NewMockMonitoring(ctrl)

			client, err := New(ctx, tt.config, monitoring)
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

	osAddr := os.Getenv("OPENSEARCH_ADDRESS")
	if osAddr == "" {
		osAddr = "http://localhost:9300"
	}

	apiKey := os.Getenv("OPENSEARCH_API_KEY")
	if apiKey == "" {
		t.Skip("OPENSEARCH_API_KEY not set, skipping API key test")
	}

	config := Config{
		Address: osAddr,
		APIKey:  apiKey,
	}
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	monitoring := mocks.NewMockMonitoring(ctrl)

	client, err := New(ctx, config, monitoring)
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

	cloudID := os.Getenv("OPENSEARCH_CLOUD_ID")
	if cloudID == "" {
		t.Skip("OPENSEARCH_CLOUD_ID not set, skipping Cloud ID test")
	}

	config := Config{
		CloudID: cloudID,
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	monitoring := mocks.NewMockMonitoring(ctrl)

	client, err := New(ctx, config, monitoring)
	if err != nil {
		t.Fatalf("New() with CloudID error = %v", err)
	}
	if client == nil {
		t.Error("New() returned nil client")
	}
}
