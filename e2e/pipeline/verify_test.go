//go:build e2e

package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"testing"
	"time"
)

// verifySync polls the search engine until the document count matches the
// expected surviving row count, then checks every surviving row's document
// content field by field, and confirms every deleted row's document is gone.
func verifySync(t *testing.T, sc *searchClient, rows []testRow, deleted map[int]bool) {
	t.Helper()
	ctx := context.Background()

	want := 0
	for _, r := range rows {
		if !deleted[r.ID] {
			want++
		}
	}

	ok := waitFor(90*time.Second, 500*time.Millisecond, func() (bool, error) {
		if err := sc.refresh(ctx); err != nil {
			return false, err
		}
		n, err := sc.count(ctx)
		if err != nil {
			return false, err
		}
		return n == want, nil
	})
	if !ok {
		n, _ := sc.count(ctx)
		t.Fatalf("e2e; pipeline; expected %d documents in index %q, got %d after timeout", want, sc.index, n)
	}

	for _, r := range rows {
		if deleted[r.ID] {
			verifyDeleted(t, sc, r.ID)
			continue
		}
		verifyDocument(t, sc, r)
	}
}

func verifyDeleted(t *testing.T, sc *searchClient, id int) {
	t.Helper()
	ctx := context.Background()

	gone := waitFor(20*time.Second, 500*time.Millisecond, func() (bool, error) {
		_, found, err := sc.getSource(ctx, strconv.Itoa(id))
		if err != nil {
			return false, err
		}
		return !found, nil
	})
	if !gone {
		t.Errorf("e2e; pipeline; expected document id=%d to be deleted from index %q, but it still exists", id, sc.index)
	}
}

func verifyDocument(t *testing.T, sc *searchClient, r testRow) {
	t.Helper()
	ctx := context.Background()

	src, found, err := sc.getSource(ctx, strconv.Itoa(r.ID))
	if err != nil {
		t.Errorf("e2e; pipeline; failed to fetch document id=%d: %v", r.ID, err)
		return
	}
	if !found {
		t.Errorf("e2e; pipeline; expected document id=%d to exist in index %q, but it was not found", r.ID, sc.index)
		return
	}

	assertField(t, r.ID, src, "name", r.Name)
	assertField(t, r.ID, src, "email", r.Email)
	assertField(t, r.ID, src, "age", r.Age)
	assertField(t, r.ID, src, "active", r.Active)
	assertField(t, r.ID, src, "score", r.Score)

	if r.Bio == nil {
		if v, ok := src["bio"]; ok && v != nil {
			t.Errorf("e2e; pipeline; document id=%d expected bio to be null, got %v", r.ID, v)
		}
	} else {
		assertField(t, r.ID, src, "bio", *r.Bio)
	}

	assertMetaField(t, r.ID, src, r.Meta)
}

// assertField compares via string representation so that JSON-decoded
// numeric/boolean types on the search-engine side line up with the Go native
// types used to build the expected row, without a type-by-type switch.
func assertField(t *testing.T, id int, src map[string]any, field string, want any) {
	t.Helper()

	got, ok := src[field]
	if !ok {
		t.Errorf("e2e; pipeline; document id=%d missing field %q", id, field)
		return
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("e2e; pipeline; document id=%d field %q mismatch: want %v, got %v", id, field, want, got)
	}
}

// assertMetaField compares nested JSON by round-tripping the expected value
// through the same JSON encode/decode both sides go through, normalizing away
// Go-side int/float64 differences.
func assertMetaField(t *testing.T, id int, src map[string]any, want map[string]any) {
	t.Helper()

	wantJSON, err := json.Marshal(want)
	if err != nil {
		t.Errorf("e2e; pipeline; document id=%d failed to marshal expected meta: %v", id, err)
		return
	}
	var wantNorm any
	if err := json.Unmarshal(wantJSON, &wantNorm); err != nil {
		t.Errorf("e2e; pipeline; document id=%d failed to normalize expected meta: %v", id, err)
		return
	}

	gotJSON, err := json.Marshal(src["meta"])
	if err != nil {
		t.Errorf("e2e; pipeline; document id=%d failed to marshal actual meta: %v", id, err)
		return
	}
	var gotNorm any
	if err := json.Unmarshal(gotJSON, &gotNorm); err != nil {
		t.Errorf("e2e; pipeline; document id=%d failed to normalize actual meta: %v", id, err)
		return
	}

	if !reflect.DeepEqual(wantNorm, gotNorm) {
		t.Errorf("e2e; pipeline; document id=%d meta mismatch: want %s, got %s", id, wantJSON, gotJSON)
	}
}
