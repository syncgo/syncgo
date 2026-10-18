package plugins

import (
	"testing"

	"github.com/syncgo/syncgo/internal/bulk_transformer"
)

func TestDocumentID(t *testing.T) {
	id, ok := documentID("id", map[string]any{"id": 42, "name": "foo"})
	if !ok || id != "42" {
		t.Fatalf("documentID = (%q, %v), want (42, true)", id, ok)
	}

	_, ok = documentID("id", map[string]any{"name": "foo"})
	if ok {
		t.Fatal("expected missing id column to fail")
	}
}

func TestValuesToData(t *testing.T) {
	item, ok := valuesToData("1", bulk_transformer.Index, map[string]any{"id": 1, "name": "a"})
	if !ok {
		t.Fatal("valuesToData failed")
	}
	if item.ID != "1" || item.Action != bulk_transformer.Index {
		t.Fatalf("unexpected item: %+v", item)
	}
	if string(item.Body) != `{"id":1,"name":"a"}` && string(item.Body) != `{"name":"a","id":1}` {
		t.Fatalf("unexpected body: %s", item.Body)
	}
}
