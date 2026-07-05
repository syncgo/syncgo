package replication

// txnTracker counts rows added during the current transaction (or stream segment).
// pgoutput only emits DML for committed transactions; StreamAbortMessageV2 rolls back
// an in-progress streamed transaction.
type txnTracker struct {
	adds int
}

func (t *txnTracker) begin() {
	t.adds = 0
}

func (t *txnTracker) recordAdd() {
	t.adds++
}

func (t *txnTracker) commit(b RowBatcher) {
	if b == nil || t.adds == 0 {
		t.adds = 0
		return
	}
	b.CommitN(t.adds)
	t.adds = 0
}

func (t *txnTracker) rollback(b RowBatcher) {
	if b == nil || t.adds == 0 {
		t.adds = 0
		return
	}
	b.RollbackN(t.adds)
	t.adds = 0
}
