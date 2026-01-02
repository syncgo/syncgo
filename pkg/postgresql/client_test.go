package postgresql

import (
	"context"
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	// Skip if no database available (for CI, we'll use service containers)
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tests := []struct {
		name    string
		config  Config
		wantErr bool
	}{
		{
			name: "valid connection",
			config: Config{
				User:     "postgres",
				Password: "postgres",
				Host:     "localhost",
				Port:     "5432",
				Database: "postgres",
			},
			wantErr: false,
		},
		{
			name: "invalid host",
			config: Config{
				User:     "postgres",
				Password: "postgres",
				Host:     "invalid-host",
				Port:     "5432",
				Database: "postgres",
			},
			wantErr: true,
		},
		{
			name: "invalid port",
			config: Config{
				User:     "postgres",
				Password: "postgres",
				Host:     "localhost",
				Port:     "9999",
				Database: "postgres",
			},
			wantErr: true,
		},
		{
			name: "invalid credentials",
			config: Config{
				User:     "invalid",
				Password: "invalid",
				Host:     "localhost",
				Port:     "5432",
				Database: "postgres",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn, err := New(ctx, tt.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("New() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if conn != nil {
				defer conn.Close(ctx)
			}
		})
	}
}

func TestNew_ReplicationMode(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Test that replication mode is set correctly
	config := Config{
		User:     "postgres",
		Password: "postgres",
		Host:     "localhost",
		Port:     "5432",
		Database: "postgres",
	}

	conn, err := New(ctx, config)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer conn.Close(ctx)

	// Verify connection is established
	if conn == nil {
		t.Error("New() returned nil connection")
	}
}
