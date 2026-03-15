package bulk_transformer

import (
	"bytes"

	"github.com/romanchechyotkin/syncgo/internal/pb/opensearchpb"
)

type Action byte

const (
	Index = iota
	Create
	Delete
	Update
)

type DataPayload []Data

type Data struct {
	ID     string
	Action Action
	Body   []byte
}

// index action
// { "index" : { "_index" : "test", "_id" : "1" } }
// { "field1" : "value1" }

// create action
// { "create" : { "_index" : "test", "_id" : "3" } }
// { "field1" : "value3" }

// delete action
// { "delete" : { "_index" : "test", "_id" : "2" } }

// update
// { "update" : {"_id" : "1", "_index" : "test"} }
// { "doc" : {"field2" : "value2"} }

func (d Data) Bytes() []byte {
	var buf = bytes.NewBuffer(nil)

	writeMetadataBody(buf, d.Action, d.ID)

	switch d.Action {
	case Index, Create:
		buf.Write(d.Body)
	case Update:
		buf.WriteString(`{"doc":`)
		buf.Write(d.Body)
		buf.WriteString(`}`)
	default:
	}

	buf.WriteByte('\n')

	return buf.Bytes()
}

// Bytes builds an NDJSON payload suitable for the HTTP Bulk API.
func (dp DataPayload) Bytes() []byte {
	if len(dp) == 0 {
		return nil
	}

	var buf = bytes.NewBuffer(nil)
	for _, d := range dp {
		buf.Write(d.Bytes())
	}

	return buf.Bytes()
}

// ToBulkRequest builds a gRPC BulkRequest for the given DataPayload.
// The provided defaultIndex is used when the protocol requires an index
// and none is encoded in the payload (which matches how the HTTP bulk
// endpoint uses a default index in the URL).
func (dp DataPayload) ToBulkRequest(defaultIndex string) *opensearchpb.BulkRequest {
	req := &opensearchpb.BulkRequest{}
	if defaultIndex != "" {
		req.Index = &defaultIndex
	}

	if len(dp) == 0 {
		return req
	}

	body := make([]*opensearchpb.BulkRequestBody, 0, len(dp))

	for _, d := range dp {
		var (
			opContainer *opensearchpb.OperationContainer
			doc         []byte
		)

		switch d.Action {
		case Index:
			idxOp := &opensearchpb.IndexOperation{}
			if defaultIndex != "" {
				idxOp.XIndex = &defaultIndex
			}
			if d.ID != "" {
				idxOp.XId = &d.ID
			}
			opContainer = &opensearchpb.OperationContainer{
				OperationContainer: &opensearchpb.OperationContainer_Index{Index: idxOp},
			}
			doc = d.Body
		case Create:
			writeOp := &opensearchpb.WriteOperation{}
			if defaultIndex != "" {
				writeOp.XIndex = &defaultIndex
			}
			if d.ID != "" {
				writeOp.XId = &d.ID
			}
			opContainer = &opensearchpb.OperationContainer{
				OperationContainer: &opensearchpb.OperationContainer_Create{Create: writeOp},
			}
			doc = d.Body
		case Update:
			updOp := &opensearchpb.UpdateOperation{}
			if defaultIndex != "" {
				updOp.XIndex = &defaultIndex
			}
			if d.ID != "" {
				updOp.XId = &d.ID
			}
			opContainer = &opensearchpb.OperationContainer{
				OperationContainer: &opensearchpb.OperationContainer_Update{Update: updOp},
			}

			buf := bytes.NewBuffer(nil)
			buf.WriteString(`{"doc":`)
			buf.Write(d.Body)
			buf.WriteString(`}`)
			doc = buf.Bytes()
		case Delete:
			delOp := &opensearchpb.DeleteOperation{}
			if defaultIndex != "" {
				delOp.XIndex = &defaultIndex
			}
			if d.ID != "" {
				delOp.XId = &d.ID
			}
			opContainer = &opensearchpb.OperationContainer{
				OperationContainer: &opensearchpb.OperationContainer_Delete{Delete: delOp},
			}
		default:
			continue
		}

		brb := &opensearchpb.BulkRequestBody{
			OperationContainer: opContainer,
		}
		if len(doc) > 0 {
			brb.Object = doc
		}

		body = append(body, brb)
	}

	req.BulkRequestBody = body
	return req
}

func writeMetadataBody(buf *bytes.Buffer, action Action, id string) {
	switch action {
	case Index:
		buf.WriteString(`{"index":{"_id":"`)
		buf.WriteString(id)
		buf.WriteString(`"}}`)
		buf.WriteByte('\n')
	case Create:
		buf.WriteString(`{"create":{"_id":"`)
		buf.WriteString(id)
		buf.WriteString(`"}}`)
		buf.WriteByte('\n')
	case Delete:
		buf.WriteString(`{"delete":{"_id":"`)
		buf.WriteString(id)
		buf.WriteString(`"}}`)
		buf.WriteByte('\n')
	case Update:
		buf.WriteString(`{"update":{"_id":"`)
		buf.WriteString(id)
		buf.WriteString(`"}}`)
		buf.WriteByte('\n')
	default:
	}
}
