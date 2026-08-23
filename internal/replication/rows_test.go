package replication

import (
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
)

func TestTupleToValues(t *testing.T) {
	rel := &pglogrepl.RelationMessage{
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", DataType: pgtype.Int4OID},
			{Name: "unchanged_toast", DataType: pgtype.Int4OID},
			{Name: "name", DataType: pgtype.TextOID},
			{Name: "bad_text", DataType: pgtype.Int4OID},
			{Name: "bad_binary", DataType: pgtype.Int4OID},
		},
	}
	typeMap := pgtype.NewMap()

	tuple := &pglogrepl.TupleData{
		Columns: []*pglogrepl.TupleDataColumn{
			{DataType: 't', Data: []byte("42")},
			{DataType: 'u'},
			{DataType: 'n'},
			{DataType: 't', Data: []byte("not-a-number")},
			{DataType: 'b', Data: []byte{0x00}},
		},
	}

	values := tupleToValues(rel, tuple, typeMap)

	if got, ok := values["id"]; !ok || got != int32(42) {
		t.Errorf("id = %v (ok=%v), want int32(42)", got, ok)
	}
	if _, ok := values["unchanged_toast"]; ok {
		t.Errorf("unchanged TOAST column should be omitted, got %v", values["unchanged_toast"])
	}
	if got, ok := values["name"]; !ok || got != nil {
		t.Errorf("name = %v (ok=%v), want nil", got, ok)
	}
	if _, ok := values["bad_text"]; ok {
		t.Errorf("bad_text should be omitted on decode error, got %v", values["bad_text"])
	}
	if _, ok := values["bad_binary"]; ok {
		t.Errorf("bad_binary should be omitted on decode error, got %v", values["bad_binary"])
	}
}

func TestTupleToValues_NilTuple(t *testing.T) {
	rel := &pglogrepl.RelationMessage{}
	typeMap := pgtype.NewMap()

	if got := tupleToValues(rel, nil, typeMap); got != nil {
		t.Errorf("tupleToValues(nil tuple) = %v, want nil", got)
	}
}

func TestDecodeTextColumnData(t *testing.T) {
	typeMap := pgtype.NewMap()

	t.Run("known OID decodes value", func(t *testing.T) {
		val, err := decodeTextColumnData(typeMap, []byte("42"), pgtype.Int4OID)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if val != int32(42) {
			t.Errorf("val = %v, want int32(42)", val)
		}
	})

	t.Run("unknown OID falls back to raw string", func(t *testing.T) {
		val, err := decodeTextColumnData(typeMap, []byte("hello"), 999999999)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if val != "hello" {
			t.Errorf("val = %v, want %q", val, "hello")
		}
	})

	t.Run("known OID decode error is propagated", func(t *testing.T) {
		_, err := decodeTextColumnData(typeMap, []byte("not-a-number"), pgtype.Int4OID)
		if err == nil {
			t.Error("expected decode error for invalid int4 text")
		}
	})
}

func TestDecodeBinaryColumnData(t *testing.T) {
	typeMap := pgtype.NewMap()

	t.Run("known OID decodes value", func(t *testing.T) {
		val, err := decodeBinaryColumnData(typeMap, []byte{0x00, 0x00, 0x00, 0x2a}, pgtype.Int4OID)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if val != int32(42) {
			t.Errorf("val = %v, want int32(42)", val)
		}
	})

	t.Run("unknown OID falls back to raw bytes", func(t *testing.T) {
		data := []byte{0x01, 0x02, 0x03}
		val, err := decodeBinaryColumnData(typeMap, data, 999999999)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		gotBytes, ok := val.([]byte)
		if !ok {
			t.Fatalf("val is %T, want []byte", val)
		}
		if string(gotBytes) != string(data) {
			t.Errorf("val = %v, want %v", gotBytes, data)
		}
	})

	t.Run("known OID decode error is propagated", func(t *testing.T) {
		_, err := decodeBinaryColumnData(typeMap, []byte{0x00, 0x01}, pgtype.Int4OID)
		if err == nil {
			t.Error("expected decode error for truncated int4 binary")
		}
	})
}
