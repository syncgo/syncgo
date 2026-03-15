package search_engine_client

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/romanchechyotkin/syncgo/internal/pb/opensearchpb"
)

// ndjsonToBulkRequest parses NDJSON bulk payload (action line, optional source line, ...)
// and builds a BulkRequest for the gRPC API. Supports index, create, update, delete actions.
func ndjsonToBulkRequest(data []byte, defaultIndex string) (*opensearchpb.BulkRequest, error) {
	req := &opensearchpb.BulkRequest{}
	if defaultIndex != "" {
		req.Index = &defaultIndex
	}
	lines := bytes.Split(data, []byte{'\n'})
	var body []*opensearchpb.BulkRequestBody
	i := 0
	for i < len(lines) {
		line := bytes.TrimSpace(lines[i])
		i++
		if len(line) == 0 {
			continue
		}
		var action map[string]json.RawMessage
		if err := json.Unmarshal(line, &action); err != nil {
			return nil, fmt.Errorf("invalid action line: %w", err)
		}
		var docLine []byte
		if i < len(lines) {
			next := bytes.TrimSpace(lines[i])
			if len(next) > 0 && next[0] == '{' {
				docLine = next
				i++
			}
		}
		bodyPart, err := parseBulkAction(action, docLine, defaultIndex)
		if err != nil {
			return nil, err
		}
		if bodyPart != nil {
			body = append(body, bodyPart)
		}
	}
	req.BulkRequestBody = body
	return req, nil
}

func parseBulkAction(action map[string]json.RawMessage, doc []byte, defaultIndex string) (*opensearchpb.BulkRequestBody, error) {
	for op, payload := range action {
		switch op {
		case "index":
			idxOp, err := parseIndexOp(payload, defaultIndex)
			if err != nil {
				return nil, err
			}
			return &opensearchpb.BulkRequestBody{
				OperationContainer: &opensearchpb.OperationContainer{
					OperationContainer: &opensearchpb.OperationContainer_Index{Index: idxOp},
				},
				Object: ptrBytes(doc),
			}, nil
		case "create":
			writeOp, err := parseCreateOp(payload, defaultIndex)
			if err != nil {
				return nil, err
			}
			return &opensearchpb.BulkRequestBody{
				OperationContainer: &opensearchpb.OperationContainer{
					OperationContainer: &opensearchpb.OperationContainer_Create{Create: writeOp},
				},
				Object: ptrBytes(doc),
			}, nil
		case "update":
			updOp, err := parseUpdateOp(payload, defaultIndex)
			if err != nil {
				return nil, err
			}
			return &opensearchpb.BulkRequestBody{
				OperationContainer: &opensearchpb.OperationContainer{
					OperationContainer: &opensearchpb.OperationContainer_Update{Update: updOp},
				},
				Object: ptrBytes(doc),
			}, nil
		case "delete":
			delOp, err := parseDeleteOp(payload, defaultIndex)
			if err != nil {
				return nil, err
			}
			return &opensearchpb.BulkRequestBody{
				OperationContainer: &opensearchpb.OperationContainer{
					OperationContainer: &opensearchpb.OperationContainer_Delete{Delete: delOp},
				},
			}, nil
		default:
			return nil, fmt.Errorf("unsupported bulk action: %s", op)
		}
	}
	return nil, fmt.Errorf("empty bulk action")
}

func parseIndexOp(payload json.RawMessage, defaultIndex string) (*opensearchpb.IndexOperation, error) {
	var op struct {
		Index   string `json:"_index"`
		ID      string `json:"_id"`
		Routing string `json:"routing"`
	}
	_ = json.Unmarshal(payload, &op)
	out := &opensearchpb.IndexOperation{}
	if op.Index != "" {
		out.XIndex = &op.Index
	} else if defaultIndex != "" {
		out.XIndex = &defaultIndex
	}
	if op.ID != "" {
		out.XId = &op.ID
	}
	if op.Routing != "" {
		out.Routing = &op.Routing
	}
	return out, nil
}

func parseCreateOp(payload json.RawMessage, defaultIndex string) (*opensearchpb.WriteOperation, error) {
	var op struct {
		Index string `json:"_index"`
		ID    string `json:"_id"`
	}
	_ = json.Unmarshal(payload, &op)
	out := &opensearchpb.WriteOperation{}
	if op.Index != "" {
		out.XIndex = &op.Index
	} else if defaultIndex != "" {
		out.XIndex = &defaultIndex
	}
	if op.ID != "" {
		out.XId = &op.ID
	}
	return out, nil
}

func parseUpdateOp(payload json.RawMessage, defaultIndex string) (*opensearchpb.UpdateOperation, error) {
	var op struct {
		Index string `json:"_index"`
		ID    string `json:"_id"`
	}
	_ = json.Unmarshal(payload, &op)
	out := &opensearchpb.UpdateOperation{}
	if op.ID != "" {
		out.XId = &op.ID
	}
	if op.Index != "" {
		out.XIndex = &op.Index
	} else if defaultIndex != "" {
		out.XIndex = &defaultIndex
	}
	return out, nil
}

func parseDeleteOp(payload json.RawMessage, defaultIndex string) (*opensearchpb.DeleteOperation, error) {
	var op struct {
		Index string `json:"_index"`
		ID    string `json:"_id"`
	}
	_ = json.Unmarshal(payload, &op)
	out := &opensearchpb.DeleteOperation{}
	if op.ID != "" {
		out.XId = &op.ID
	}
	if op.Index != "" {
		out.XIndex = &op.Index
	} else if defaultIndex != "" {
		out.XIndex = &defaultIndex
	}
	return out, nil
}

func ptrBytes(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	return b
}
