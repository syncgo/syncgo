package plugins

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/syncgo/syncgo/internal/bulk_transformer"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
)

// Message byte IDs from the PostgreSQL logical replication protocol; pglogrepl
// does not export these constants.
const (
	BeginByteID    = 'B'
	InsertByteID   = 'I'
	UpdateByteID   = 'U'
	DeleteByteID   = 'D'
	RelationByteID = 'R'
)

// encodeXLogMessage serializes a logical replication message the way
// pgoutput would send it over the wire (pglogrepl messages have no public
// encoder, so we build the binary form manually).
func encodeXLogMessage(t *testing.T, msg pglogrepl.Message) []byte {
	t.Helper()

	switch m := msg.(type) {
	case *pglogrepl.InsertMessage:
		buf := &bytes.Buffer{}
		buf.WriteByte(byte(InsertByteID))
		binary.Write(buf, binary.BigEndian, m.RelationID)
		buf.WriteByte('N')
		buf.Write(encodeTupleData(t, m.Tuple))
		return buf.Bytes()

	case *pglogrepl.UpdateMessage:
		buf := &bytes.Buffer{}
		buf.WriteByte(byte(UpdateByteID))
		binary.Write(buf, binary.BigEndian, m.RelationID)
		if m.OldTuple != nil {
			buf.WriteByte('O')
			buf.Write(encodeTupleData(t, m.OldTuple))
		}
		buf.WriteByte('N')
		buf.Write(encodeTupleData(t, m.NewTuple))
		return buf.Bytes()

	case *pglogrepl.DeleteMessage:
		buf := &bytes.Buffer{}
		buf.WriteByte(byte(DeleteByteID))
		binary.Write(buf, binary.BigEndian, m.RelationID)
		buf.WriteByte('K')
		buf.Write(encodeTupleData(t, m.OldTuple))
		return buf.Bytes()

	default:
		t.Fatalf("unsupported message type %T", msg)
		return nil
	}
}

// encodeTupleData serializes a TupleData in the pgoutput wire format.
func encodeTupleData(t *testing.T, tuple *pglogrepl.TupleData) []byte {
	t.Helper()

	buf := &bytes.Buffer{}
	binary.Write(buf, binary.BigEndian, uint16(len(tuple.Columns)))
	for _, col := range tuple.Columns {
		switch col.DataType {
		case 'n', 'u':
			buf.WriteByte(col.DataType)
		case 't':
			buf.WriteByte('t')
			binary.Write(buf, binary.BigEndian, uint32(len(col.Data)))
			buf.Write(col.Data)
		default:
			t.Fatalf("unsupported tuple column type %q", col.DataType)
		}
	}
	return buf.Bytes()
}

func buildTestRelation(relationID uint32) *pglogrepl.RelationMessage {
	return &pglogrepl.RelationMessage{
		RelationID:   relationID,
		RelationName: "users",
		Namespace:    "public",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", DataType: pgtype.Int4OID},
			{Name: "name", DataType: pgtype.TextOID},
		},
	}
}

// encodeRelationMessage serializes a RelationMessage in the pgoutput wire
// format.
func encodeRelationMessage(t *testing.T, rel *pglogrepl.RelationMessage) []byte {
	t.Helper()

	buf := &bytes.Buffer{}
	buf.WriteByte(byte(RelationByteID))
	binary.Write(buf, binary.BigEndian, rel.RelationID)
	writeString := func(s string) {
		buf.WriteString(s)
		buf.WriteByte(0)
	}
	writeString(rel.Namespace)
	writeString(rel.RelationName)
	buf.WriteByte(0) // replica identity 'default'
	binary.Write(buf, binary.BigEndian, uint16(len(rel.Columns)))
	for _, col := range rel.Columns {
		buf.WriteByte(0) // flags
		writeString(col.Name)
		binary.Write(buf, binary.BigEndian, col.DataType)
		binary.Write(buf, binary.BigEndian, int32(-1)) // typmod, use -1
	}
	return buf.Bytes()
}

func TestNewPlugin(t *testing.T) {
	tests := []struct {
		name     string
		plugin   string
		expected string
		wantErr  bool
	}{
		{name: "pgoutput", plugin: "pgoutput", expected: "pgoutput"},
		{name: "empty defaults to pgoutput", plugin: "", expected: "pgoutput"},
		{name: "unknown", plugin: "test_decoding", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := New(tt.plugin, "pub", "id")
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error for plugin %q", tt.plugin)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if p.Name() != tt.expected {
				t.Errorf("Name() = %q, want %q", p.Name(), tt.expected)
			}
		})
	}
}

func TestPgOutputDecode(t *testing.T) {
	p := newPgOutputPlugin("pub", "id")

	t.Run("begin and commit", func(t *testing.T) {
		// BeginMessage payload: FinalLSN (8) + CommitTime (8) + XID (4).
		payload := make([]byte, 20)
		events, err := p.Decode(append([]byte{byte(BeginByteID)}, payload...))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(events) != 1 || events[0].Kind != EventBegin {
			t.Fatalf("events = %+v, want [EventBegin]", events)
		}
	})

	t.Run("relation is cached", func(t *testing.T) {
		rel := buildTestRelation(1)
		// encodeRelationMessage already includes the message type byte.
		events, err := p.Decode(encodeRelationMessage(t, rel))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(events) != 0 {
			t.Fatalf("events = %+v, want none", events)
		}
		if _, ok := p.relations[1]; !ok {
			t.Fatal("relation was not cached")
		}
	})

	t.Run("insert", func(t *testing.T) {
		p := newPgOutputPlugin("pub", "id")
		p.relations[1] = buildTestRelation(1)

		tuple := &pglogrepl.TupleData{
			Columns: []*pglogrepl.TupleDataColumn{
				{DataType: 't', Data: []byte("42")},
				{DataType: 't', Data: []byte("foo")},
			},
		}
		msg := &pglogrepl.InsertMessage{RelationID: 1, Tuple: tuple}
		events, err := p.Decode(encodeXLogMessage(t, msg))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(events) != 1 || events[0].Kind != EventChange {
			t.Fatalf("events = %+v, want one EventChange", events)
		}
		change := events[0].Change
		if change.ID != "42" || change.Action != bulk_transformer.Index {
			t.Fatalf("change = %+v, want id=42 action=index", change)
		}
	})

	t.Run("unknown relation is skipped", func(t *testing.T) {
		p := newPgOutputPlugin("pub", "id")
		// One null column so the message satisfies the minimum wire length.
		msg := &pglogrepl.InsertMessage{RelationID: 99, Tuple: &pglogrepl.TupleData{
			Columns: []*pglogrepl.TupleDataColumn{{DataType: 'n'}},
		}}
		events, err := p.Decode(encodeXLogMessage(t, msg))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(events) != 0 {
			t.Fatalf("events = %+v, want none", events)
		}
	})
}
