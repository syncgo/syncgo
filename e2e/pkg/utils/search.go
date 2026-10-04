//go:build e2e

package utils

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"
)

// AssertDocument polls the search engine until the document's "name" field
// matches expectedName, or fails the test after timeout.
func AssertDocument(t *testing.T, address, index, id, expectedName string) {
	t.Helper()

	timeout := 15 * time.Second
	deadline := time.Now().Add(timeout)

	var lastName string
	for time.Now().Before(deadline) {
		if source, ok := FetchDocument(t, address, index, id); ok {
			lastName, _ = source["name"].(string)
			if lastName == expectedName {
				return
			}
		}
		time.Sleep(300 * time.Millisecond)
	}

	t.Fatalf("e2e; sync; document %s has name %q, want %q after %s", id, lastName, expectedName, timeout)
}

// WaitForDocumentAbsent polls the search engine until the document is gone,
// or fails the test after timeout.
func WaitForDocumentAbsent(t *testing.T, address, index, id string, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, ok := FetchDocument(t, address, index, id); !ok {
			return
		}
		time.Sleep(300 * time.Millisecond)
	}

	t.Fatalf("e2e; sync; document %s was still present in %s/%s after %s", id, address, index, timeout)
}

// FetchDocument fetches a document's _source by id. The second return value
// is false if the document does not exist (HTTP 404).
func FetchDocument(t *testing.T, address, index, id string) (map[string]any, bool) {
	t.Helper()

	url := fmt.Sprintf("%s/%s/_doc/%s", address, index, id)
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal("e2e; sync; failed to fetch document; error: ", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, false
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("e2e; sync; unexpected status %d fetching document %s", resp.StatusCode, id)
	}

	var body struct {
		Source map[string]any `json:"_source"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal("e2e; sync; failed to decode document response; error: ", err)
	}

	return body.Source, true
}
