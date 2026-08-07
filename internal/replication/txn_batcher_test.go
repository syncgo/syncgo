package replication

import (
	"testing"

	"github.com/syncgo/syncgo/internal/bulk_transformer"
)

type mockRowBatcher struct {
	added     []bulk_transformer.Data
	commitN   int
	rollbackN int
}

func (m *mockRowBatcher) Add(item bulk_transformer.Data) {
	m.added = append(m.added, item)
}

func (m *mockRowBatcher) CommitN(n int) {
	m.commitN += n
}

func (m *mockRowBatcher) RollbackN(n int) {
	m.rollbackN += n
}

func TestTxnTracker_BeginAddCommit(t *testing.T) {
	var txn txnTracker
	b := &mockRowBatcher{}

	txn.begin()
	txn.recordAdd()
	txn.recordAdd()
	txn.commit(b)

	if len(b.added) != 0 {
		t.Fatalf("tracker should not add via commit, got %d added", len(b.added))
	}
	if b.commitN != 2 {
		t.Fatalf("CommitN = %d, want 2", b.commitN)
	}
	if txn.adds != 0 {
		t.Fatalf("txn.adds = %d after commit, want 0", txn.adds)
	}
}

func TestTxnTracker_BeginAddRollback(t *testing.T) {
	var txn txnTracker
	b := &mockRowBatcher{}

	txn.begin()
	txn.recordAdd()
	txn.recordAdd()
	txn.rollback(b)

	if b.rollbackN != 2 {
		t.Fatalf("RollbackN = %d, want 2", b.rollbackN)
	}
	if b.commitN != 0 {
		t.Fatalf("CommitN = %d, want 0", b.commitN)
	}
}

func TestTxnTracker_CommitWithNilBatcher(t *testing.T) {
	var txn txnTracker
	txn.recordAdd()
	txn.commit(nil)
	if txn.adds != 0 {
		t.Fatalf("txn.adds = %d, want 0", txn.adds)
	}
}

func TestLogicalReplicationConn_EnqueueAndCommit(t *testing.T) {
	b := &mockRowBatcher{}
	c := &LogicalReplicationConn{
		batcher:  b,
		idColumn: "id",
	}

	c.txn.begin()
	c.enqueueRow(bulk_transformer.Data{ID: "1", Action: bulk_transformer.Index, Body: []byte(`{"id":1}`)})
	c.enqueueRow(bulk_transformer.Data{ID: "2", Action: bulk_transformer.Index, Body: []byte(`{"id":2}`)})
	c.txn.commit(c.batcher)

	if len(b.added) != 2 {
		t.Fatalf("added = %d, want 2", len(b.added))
	}
	if b.commitN != 2 {
		t.Fatalf("CommitN = %d, want 2", b.commitN)
	}
}

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
