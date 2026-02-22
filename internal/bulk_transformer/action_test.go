package bulk_transformer

import (
	"bytes"
	"testing"
)

func Test_Data_Bytes(t *testing.T) {
	tests := []struct {
		name     string
		data     Data
		expected string
	}{
		{
			name: "Index action with body",
			data: Data{
				Action: Index,
				Body:   []byte(`{"field1":"value1"}`),
				ID:     "1",
			},
			expected: "{\"index\":{\"_id\":\"1\"}}\n{\"field1\":\"value1\"}\n",
		},
		{
			name: "Create action with body",
			data: Data{
				Action: Create,
				Body:   []byte(`{"field1":"value3"}`),
				ID:     "3",
			},
			expected: "{\"create\":{\"_id\":\"3\"}}\n{\"field1\":\"value3\"}\n",
		},
		{
			name: "Delete action without body",
			data: Data{
				Action: Delete,
				Body:   nil,
				ID:     "2",
			},
			expected: "{\"delete\":{\"_id\":\"2\"}}\n\n",
		},
		{
			name: "Update action with wrapped body",
			data: Data{
				Action: Update,
				Body:   []byte(`{"field2":"value2"}`),
				ID:     "1",
			},
			expected: "{\"update\":{\"_id\":\"1\"}}\n{\"doc\":{\"field2\":\"value2\"}}\n",
		},
		{
			name: "Index action with empty body",
			data: Data{
				Action: Index,
				Body:   []byte{},
				ID:     "123",
			},
			expected: "{\"index\":{\"_id\":\"123\"}}\n\n",
		},
		{
			name: "Update action with complex body",
			data: Data{
				Action: Update,
				Body:   []byte(`{"name":"John","age":30,"active":true}`),
				ID:     "456",
			},
			expected: "{\"update\":{\"_id\":\"456\"}}\n{\"doc\":{\"name\":\"John\",\"age\":30,\"active\":true}}\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.data.Bytes()

			if string(result) != tt.expected {
				t.Errorf("Data.Bytes() = %q, want %q", string(result), tt.expected)
			}
		})
	}
}

func Test_WriteMetadataBody(t *testing.T) {
	tests := []struct {
		name     string
		action   Action
		id       string
		expected string
	}{
		{
			name:     "Index action metadata",
			action:   Index,
			id:       "1",
			expected: "{\"index\":{\"_id\":\"1\"}}\n",
		},
		{
			name:     "Create action metadata",
			action:   Create,
			id:       "3",
			expected: "{\"create\":{\"_id\":\"3\"}}\n",
		},
		{
			name:     "Delete action metadata",
			action:   Delete,
			id:       "2",
			expected: "{\"delete\":{\"_id\":\"2\"}}\n",
		},
		{
			name:     "Update action metadata",
			action:   Update,
			id:       "1",
			expected: "{\"update\":{\"_id\":\"1\"}}\n",
		},
		{
			name:     "Index with long ID",
			action:   Index,
			id:       "abc-123-def-456",
			expected: "{\"index\":{\"_id\":\"abc-123-def-456\"}}\n",
		},
		{
			name:     "Delete with numeric ID",
			action:   Delete,
			id:       "999999",
			expected: "{\"delete\":{\"_id\":\"999999\"}}\n",
		},
		{
			name:     "Update with UUID",
			action:   Update,
			id:       "550e8400-e29b-41d4-a716-446655440000",
			expected: "{\"update\":{\"_id\":\"550e8400-e29b-41d4-a716-446655440000\"}}\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes.NewBuffer(nil)
			writeMetadataBody(buf, tt.action, tt.id)

			result := buf.String()
			if result != tt.expected {
				t.Errorf("writeMetadataBody() = %q, want %q", result, tt.expected)
			}
		})
	}
}
