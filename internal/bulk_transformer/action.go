package bulk_transformer

import "bytes"

type Action byte

const (
	Index = iota
	Create
	Delete
	Update
)

type Data struct {
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

func (d Data) Bytes(id string) []byte {
	var buf = bytes.NewBuffer(nil)

	writeMetadataBody(buf, d.Action, id)

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
