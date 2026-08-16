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

func Test_DataPayload_Bytes(t *testing.T) {
	tests := []struct {
		name     string
		payload  DataPayload
		expected string
	}{
		{
			name:     "empty payload",
			payload:  DataPayload{},
			expected: "",
		},
		{
			name: "single item",
			payload: DataPayload{
				{Action: Index, ID: "1", Body: []byte(`{"a":1}`)},
			},
			expected: "{\"index\":{\"_id\":\"1\"}}\n{\"a\":1}\n",
		},
		{
			name: "multiple items concatenated in order",
			payload: DataPayload{
				{Action: Create, ID: "1", Body: []byte(`{"a":1}`)},
				{Action: Delete, ID: "2"},
				{Action: Update, ID: "3", Body: []byte(`{"b":2}`)},
			},
			expected: "{\"create\":{\"_id\":\"1\"}}\n{\"a\":1}\n" +
				"{\"delete\":{\"_id\":\"2\"}}\n\n" +
				"{\"update\":{\"_id\":\"3\"}}\n{\"doc\":{\"b\":2}}\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.payload.Bytes()

			if len(tt.payload) == 0 {
				if result != nil {
					t.Errorf("DataPayload.Bytes() = %q, want nil for empty payload", result)
				}
				return
			}

			if string(result) != tt.expected {
				t.Errorf("DataPayload.Bytes() = %q, want %q", string(result), tt.expected)
			}
		})
	}
}

func Test_DataPayload_ToBulkRequest(t *testing.T) {
	t.Run("empty payload with default index", func(t *testing.T) {
		req := DataPayload{}.ToBulkRequest("my-index")
		if req.GetIndex() != "my-index" {
			t.Errorf("req.Index = %q, want %q", req.GetIndex(), "my-index")
		}
		if len(req.BulkRequestBody) != 0 {
			t.Errorf("expected no request bodies, got %d", len(req.BulkRequestBody))
		}
	})

	t.Run("empty defaultIndex leaves Index unset", func(t *testing.T) {
		req := DataPayload{}.ToBulkRequest("")
		if req.Index != nil {
			t.Errorf("req.Index = %v, want nil", req.Index)
		}
	})

	t.Run("index action carries body, index, and id", func(t *testing.T) {
		req := DataPayload{
			{Action: Index, ID: "1", Body: []byte(`{"field":"value"}`)},
		}.ToBulkRequest("my-index")

		if len(req.BulkRequestBody) != 1 {
			t.Fatalf("expected 1 body, got %d", len(req.BulkRequestBody))
		}
		body := req.BulkRequestBody[0]
		idxOp := body.OperationContainer.GetIndex()
		if idxOp == nil {
			t.Fatal("expected Index operation container")
		}
		if idxOp.GetXId() != "1" {
			t.Errorf("XId = %q, want %q", idxOp.GetXId(), "1")
		}
		if idxOp.GetXIndex() != "my-index" {
			t.Errorf("XIndex = %q, want %q", idxOp.GetXIndex(), "my-index")
		}
		if string(body.Object) != `{"field":"value"}` {
			t.Errorf("Object = %s, want %s", body.Object, `{"field":"value"}`)
		}
	})

	t.Run("create action carries body, index, and id", func(t *testing.T) {
		req := DataPayload{
			{Action: Create, ID: "3", Body: []byte(`{"field":"value3"}`)},
		}.ToBulkRequest("my-index")

		body := req.BulkRequestBody[0]
		createOp := body.OperationContainer.GetCreate()
		if createOp == nil {
			t.Fatal("expected Create operation container")
		}
		if createOp.GetXId() != "3" {
			t.Errorf("XId = %q, want %q", createOp.GetXId(), "3")
		}
		if createOp.GetXIndex() != "my-index" {
			t.Errorf("XIndex = %q, want %q", createOp.GetXIndex(), "my-index")
		}
		if string(body.Object) != `{"field":"value3"}` {
			t.Errorf("Object = %s, want %s", body.Object, `{"field":"value3"}`)
		}
	})

	t.Run("update action wraps body in doc envelope", func(t *testing.T) {
		req := DataPayload{
			{Action: Update, ID: "1", Body: []byte(`{"field2":"value2"}`)},
		}.ToBulkRequest("my-index")

		body := req.BulkRequestBody[0]
		updOp := body.OperationContainer.GetUpdate()
		if updOp == nil {
			t.Fatal("expected Update operation container")
		}
		if updOp.GetXId() != "1" {
			t.Errorf("XId = %q, want %q", updOp.GetXId(), "1")
		}
		if string(body.Object) != `{"doc":{"field2":"value2"}}` {
			t.Errorf("Object = %s, want %s", body.Object, `{"doc":{"field2":"value2"}}`)
		}
	})

	t.Run("delete action has no body", func(t *testing.T) {
		req := DataPayload{
			{Action: Delete, ID: "2"},
		}.ToBulkRequest("my-index")

		body := req.BulkRequestBody[0]
		delOp := body.OperationContainer.GetDelete()
		if delOp == nil {
			t.Fatal("expected Delete operation container")
		}
		if delOp.GetXId() != "2" {
			t.Errorf("XId = %q, want %q", delOp.GetXId(), "2")
		}
		if delOp.GetXIndex() != "my-index" {
			t.Errorf("XIndex = %q, want %q", delOp.GetXIndex(), "my-index")
		}
		if body.Object != nil {
			t.Errorf("Object = %s, want nil", body.Object)
		}
	})

	t.Run("empty id omits XId on operation", func(t *testing.T) {
		req := DataPayload{
			{Action: Index, Body: []byte(`{"a":1}`)},
		}.ToBulkRequest("my-index")

		idxOp := req.BulkRequestBody[0].OperationContainer.GetIndex()
		if idxOp.XId != nil {
			t.Errorf("XId = %v, want nil for empty ID", idxOp.XId)
		}
	})

	t.Run("empty defaultIndex omits XIndex on operation", func(t *testing.T) {
		req := DataPayload{
			{Action: Index, ID: "1", Body: []byte(`{"a":1}`)},
		}.ToBulkRequest("")

		idxOp := req.BulkRequestBody[0].OperationContainer.GetIndex()
		if idxOp.XIndex != nil {
			t.Errorf("XIndex = %v, want nil for empty defaultIndex", idxOp.XIndex)
		}
	})

	t.Run("preserves order across multiple actions", func(t *testing.T) {
		req := DataPayload{
			{Action: Create, ID: "1", Body: []byte(`{}`)},
			{Action: Delete, ID: "2"},
			{Action: Update, ID: "3", Body: []byte(`{}`)},
		}.ToBulkRequest("my-index")

		if len(req.BulkRequestBody) != 3 {
			t.Fatalf("expected 3 bodies, got %d", len(req.BulkRequestBody))
		}
		if req.BulkRequestBody[0].OperationContainer.GetCreate() == nil {
			t.Error("expected body[0] to be Create")
		}
		if req.BulkRequestBody[1].OperationContainer.GetDelete() == nil {
			t.Error("expected body[1] to be Delete")
		}
		if req.BulkRequestBody[2].OperationContainer.GetUpdate() == nil {
			t.Error("expected body[2] to be Update")
		}
	})

	t.Run("unknown action is skipped", func(t *testing.T) {
		req := DataPayload{
			{Action: Action(99), ID: "1", Body: []byte(`{}`)},
		}.ToBulkRequest("my-index")

		if len(req.BulkRequestBody) != 0 {
			t.Errorf("expected unknown action to be skipped, got %d bodies", len(req.BulkRequestBody))
		}
	})
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
