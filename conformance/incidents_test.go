//go:build integration

package conformance_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	sink "github.com/liran/sink-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestHotKeyMergeAmplification(t *testing.T) {
	for _, store := range searchBackends(t) {
		t.Run(store.driver, func(t *testing.T) {
			index := indexFor(t, store, "-1")
			proxy := proxyBackend(t, store)
			opts := serverOptions{backend: proxy.backend}
			server := startCandidate(t, opts)
			address := addressFor(t, index, "hot")
			operations := make([]sink.WriteOperation, 64)
			for i := range operations {
				operations[i] = merge(t, address, increment)
			}
			results := applied(t, writeAsync(t.Context(), server.client, sink.CompletionWaitUntilApplied, operations...), len(operations))
			// Exact backend work is the regression oracle, independent of machine speed.
			if reads, writes := proxy.count("/_mget", "hot"), proxy.count("/_bulk", "hot"); reads != 1 || writes != 1 {
				t.Fatalf("hot-key amplification: reads=%d writes=%d; want one snapshot and one conditional commit", reads, writes)
			}
			for _, result := range results {
				if !bytes.Equal(result.Revision.Bytes(), results[0].Revision.Bytes()) {
					t.Fatal("folded operations did not share the committed revision")
				}
			}
			assertCounter(t, server.client, address, 64)
		})
	}
}

func TestAppliedDoesNotInheritVisibleRefresh(t *testing.T) {
	for _, store := range searchBackends(t) {
		t.Run(store.driver, func(t *testing.T) {
			archive := indexFor(t, store, "-1")
			product := indexFor(t, store, "100ms")
			opts := serverOptions{backend: store, batchOps: 2, batchWait: 2000}
			server := startCandidate(t, opts)
			archived := addressFor(t, archive, "archive")
			visible := addressFor(t, product, "product")
			archivePut := put(t, archived, `{"counter":1}`, sink.WriteUpsert)
			productPut := put(t, visible, `{"counter":1}`, sink.WriteUpsert)
			first := writeAsync(t.Context(), server.client, sink.CompletionWaitUntilVisible, productPut)
			server.waitQueued(t, "Write")
			second := writeAsync(t.Context(), server.client, sink.CompletionWaitUntilApplied, archivePut)
			applied(t, second, 1)
			applied(t, first, 1)
			assertSearchCount(t, store, product, 1)
			assertCounter(t, server.client, archived, 1)
			// Delete has a separate batcher and must meet the same completion contract.
			done := make(chan error, 1)
			go func() {
				results, err := server.client.Delete(t.Context(), sink.CompletionWaitUntilVisible, visible)
				if err == nil && (len(results) != 1 || results[0].Status != sink.DeleteApplied) {
					err = fmt.Errorf("visible delete: %+v", results)
				}
				done <- err
			}()
			server.waitQueued(t, "Delete")
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			results, err := server.client.Delete(ctx, sink.CompletionWaitUntilApplied, archived)
			if err != nil || len(results) != 1 || results[0].Status != sink.DeleteApplied {
				t.Fatalf("applied delete inherited refresh wait: %+v, %v", results, err)
			}
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal("visible delete did not complete")
			}
			assertSearchCount(t, store, product, 0)
		})
	}
}

func TestCompletedDocumentReleasedBeforeSiblingRead(t *testing.T) {
	for _, store := range searchBackends(t) {
		t.Run(store.driver, func(t *testing.T) {
			index := indexFor(t, store, "-1")
			proxy := proxyBackend(t, store)
			opts := serverOptions{backend: proxy.backend, batchOps: 2}
			server := startCandidate(t, opts)
			gate := proxy.hold("/_mget", "slow", 1)
			t.Cleanup(gate.open)
			fast := addressFor(t, index, "fast")
			slow := addressFor(t, index, "slow")
			firstPut := put(t, fast, `{"counter":1}`, sink.WriteUpsert)
			first := writeAsync(t.Context(), server.client, sink.CompletionWaitUntilApplied, firstPut, merge(t, slow, increment))
			gate.wait(t)
			// Prove persistence before checking release; the whole original RPC
			// must remain pending until its slow operation finishes.
			call := httpCall{endpoint: store.endpoint, method: http.MethodGet, path: "/" + index + "/_doc/fast"}
			code, body := request(t, call)
			if code != http.StatusOK {
				t.Fatalf("fast document was not committed before slow read: %d %s", code, body)
			}
			select {
			case result := <-first:
				t.Fatalf("multi-operation RPC acknowledged incomplete work: %+v", result)
			default:
			}
			nextPut := put(t, fast, `{"counter":2}`, sink.WriteUpsert)
			applied(t, writeAsync(t.Context(), server.client, sink.CompletionWaitUntilApplied, nextPut), 1)
			gate.open()
			applied(t, first, 2)
			assertCounter(t, server.client, fast, 2)
			assertCounter(t, server.client, slow, 1)
		})
	}
}

func TestSuccessfulSiblingNotReplayedDuringConflict(t *testing.T) {
	for _, store := range searchBackends(t) {
		t.Run(store.driver, func(t *testing.T) {
			index := indexFor(t, store, "-1")
			proxy := proxyBackend(t, store)
			opts := serverOptions{backend: proxy.backend, batchOps: 2, batchWait: 2000}
			server := startCandidate(t, opts)
			fast, slow := addressFor(t, index, "fast"), addressFor(t, index, "slow")
			commit := proxy.hold("/_bulk", "slow", 1)
			retry := proxy.hold("/_mget", "slow", 2)
			t.Cleanup(commit.open)
			t.Cleanup(retry.open)
			first := writeAsync(t.Context(), server.client, sink.CompletionWaitUntilApplied, merge(t, fast, increment))
			server.waitQueued(t, "Write")
			second := writeAsync(t.Context(), server.client, sink.CompletionWaitUntilApplied, merge(t, slow, increment))
			commit.wait(t)
			// Create a real revision conflict after Sink's snapshot but before
			// its conditional commit. The retry must recompute from counter=10.
			call := httpCall{endpoint: store.endpoint, method: http.MethodPut, path: "/" + index + "/_doc/slow", body: []byte(`{"counter":10}`)}
			code, body := request(t, call)
			if code != http.StatusCreated {
				t.Fatalf("competing writer: %d %s", code, body)
			}
			commit.open()
			retry.wait(t)
			applied(t, first, 1)
			select {
			case result := <-second:
				t.Fatalf("conflicting operation acknowledged before retry: %+v", result)
			default:
			}
			retry.open()
			applied(t, second, 1)
			if fastWrites, slowWrites := proxy.count("/_bulk", "fast"), proxy.count("/_bulk", "slow"); fastWrites != 1 || slowWrites != 2 {
				t.Fatalf("retry scope: fast=%d slow=%d, want 1 and 2", fastWrites, slowWrites)
			}
			assertCounter(t, server.client, fast, 1)
			assertCounter(t, server.client, slow, 11)
		})
	}
}

func TestVisibleDatasetsCompleteIndependently(t *testing.T) {
	for _, store := range searchBackends(t) {
		t.Run(store.driver, func(t *testing.T) {
			slowIndex := indexFor(t, store, "-1")
			fastIndex := indexFor(t, store, "100ms")
			opts := serverOptions{backend: store, batchOps: 2, batchWait: 2000}
			server := startCandidate(t, opts)
			slowAddress, fastAddress := addressFor(t, slowIndex, "slow"), addressFor(t, fastIndex, "fast")
			slowPut := put(t, slowAddress, `{"counter":1}`, sink.WriteUpsert)
			fastPut := put(t, fastAddress, `{"counter":1}`, sink.WriteUpsert)
			slow := writeAsync(t.Context(), server.client, sink.CompletionWaitUntilVisible, slowPut)
			server.waitQueued(t, "Write")
			fast := writeAsync(t.Context(), server.client, sink.CompletionWaitUntilVisible, fastPut)
			applied(t, fast, 1)
			assertSearchCount(t, store, fastIndex, 1)
			// Real-time GET observes storage without satisfying the search wait.
			assertCounter(t, server.client, slowAddress, 1)
			select {
			case result := <-slow:
				t.Fatalf("visible acknowledged without refresh: %+v", result)
			default:
			}
			call := httpCall{endpoint: store.endpoint, method: http.MethodPost, path: "/" + slowIndex + "/_refresh"}
			code, body := request(t, call)
			if code != http.StatusOK {
				t.Fatalf("refresh: %d %s", code, body)
			}
			applied(t, slow, 1)
			assertSearchCount(t, store, slowIndex, 1)
		})
	}
}

func TestReadBudgetsBelongToOriginalRPC(t *testing.T) {
	for _, store := range searchBackends(t) {
		t.Run(store.driver, func(t *testing.T) {
			index := indexFor(t, store, "-1")
			proxy := proxyBackend(t, store)
			opts := serverOptions{backend: proxy.backend, batchOps: 2, batchWait: 2000, readBytes: 256}
			server := startCandidate(t, opts)
			a, b := addressFor(t, index, "a"), addressFor(t, index, "b")
			raw := `{"value":"` + strings.Repeat("x", 80) + `"}`
			applied(t, writeAsync(t.Context(), server.client, sink.CompletionWaitUntilApplied,
				put(t, a, raw, sink.WriteUpsert), put(t, b, raw, sink.WriteUpsert)), 2)
			control, err := server.client.Read(t.Context(), a)
			if err != nil || len(control) != 1 || control[0].Status != sink.ReadFound {
				t.Fatalf("single-RPC budget control failed: %+v, %v", control, err)
			}
			type outcome struct {
				results []sink.ReadResult
				err     error
			}
			done := make(chan outcome, 2)
			for i, address := range []sink.Address{a, b} {
				go func() {
					results, err := server.client.Read(t.Context(), address)
					result := outcome{results: results, err: err}
					done <- result
				}()
				if i == 0 {
					server.waitQueued(t, "Read")
				}
			}
			for range 2 {
				select {
				case result := <-done:
					if result.err != nil || len(result.results) != 1 || result.results[0].Status != sink.ReadFound {
						t.Fatalf("one valid read consumed another RPC's budget: %+v, %v", result.results, result.err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("independent read did not finish")
				}
			}
			// Repeated result documents still count twice within one RPC, even
			// if the backend snapshot is deduplicated. SDK retries are disabled.
			results, err := server.client.Read(t.Context(), a, a)
			if err != nil || len(results) != 2 || results[0].Status != sink.ReadFound || results[1].Status != sink.ReadFailed ||
				results[1].Failure == nil || results[1].Failure.Code != sink.FailureResourceExhausted {
				t.Fatalf("duplicate read escaped output budget: %+v, %v", results, err)
			}
		})
	}
}

func TestFormattedJSONBulkFraming(t *testing.T) {
	for _, store := range searchBackends(t) {
		t.Run(store.driver, func(t *testing.T) {
			index := indexFor(t, store, "-1")
			opts := serverOptions{backend: store}
			server := startCandidate(t, opts)
			a, b := addressFor(t, index, "a"), addressFor(t, index, "b")
			first := put(t, a, "{\n  \"counter\": 1,\n  \"text\": \"quote\\\" and line\\n and 品牌\"\n}\n", sink.WriteUpsert)
			second := put(t, b, `{"counter":2}`, sink.WriteUpsert)
			applied(t, writeAsync(t.Context(), server.client, sink.CompletionWaitUntilApplied, first, second), 2)
			assertCounter(t, server.client, a, 1)
			assertCounter(t, server.client, b, 2)
		})
	}
}

func TestReplaceRechecksExistenceAfterConflict(t *testing.T) {
	for _, store := range searchBackends(t) {
		for _, deleted := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/deleted-%t", store.driver, deleted), func(t *testing.T) {
				index := indexFor(t, store, "-1")
				call := httpCall{endpoint: store.endpoint, method: http.MethodPut, path: "/" + index + "/_doc/replace", body: []byte(`{"counter":0}`)}
				code, body := request(t, call)
				if code != http.StatusCreated {
					t.Fatalf("seed replacement: %d %s", code, body)
				}
				proxy := proxyBackend(t, store)
				opts := serverOptions{backend: proxy.backend, unbatched: true}
				server := startCandidate(t, opts)
				gate := proxy.hold("/_bulk", "replace", 1)
				t.Cleanup(gate.open)
				replaced := addressFor(t, index, "replace")
				sibling := addressFor(t, index, "sibling")
				result := writeAsync(t.Context(), server.client, sink.CompletionWaitUntilApplied,
					put(t, replaced, `{"counter":2}`, sink.WriteReplace), put(t, sibling, `{"counter":3}`, sink.WriteUpsert))
				gate.wait(t)
				if deleted {
					call.method, call.body = http.MethodDelete, nil
				} else {
					call.body = []byte(`{"counter":7}`)
				}
				code, body = request(t, call)
				if code != http.StatusOK {
					t.Fatalf("competing mutation: %d %s", code, body)
				}
				gate.open()
				select {
				case outcome := <-result:
					if outcome.err != nil || len(outcome.results) != 2 {
						t.Fatalf("replace response: %+v %v", outcome.results, outcome.err)
					}
					want := sink.WriteApplied
					if deleted {
						want = sink.WritePreconditionFailed
					}
					if outcome.results[0].Status != want || outcome.results[1].Status != sink.WriteApplied || outcome.results[1].Failure != nil {
						t.Fatalf("replace existence contract: %+v, want %s then applied", outcome.results, want)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("replace conflict retry did not terminate")
				}
				if proxy.count("/_bulk", "sibling") != 1 || proxy.count("/_mget", "replace") != 2 {
					t.Fatal("replace retry did not reread exactly once or replayed its successful sibling")
				}
				results, err := server.client.Read(t.Context(), replaced)
				if err != nil || len(results) != 1 {
					t.Fatalf("read replacement: %+v %v", results, err)
				}
				if deleted {
					if results[0].Status != sink.ReadNotFound {
						t.Fatalf("replace resurrected concurrently deleted record: %+v", results)
					}
				} else {
					assertCounter(t, server.client, replaced, 2)
				}
				assertCounter(t, server.client, sibling, 3)
			})
		}
	}
}

func TestQueuedCancellationDoesNotPoisonFollowingWrites(t *testing.T) {
	for _, store := range searchBackends(t) {
		t.Run(store.driver, func(t *testing.T) {
			index := indexFor(t, store, "-1")
			opts := serverOptions{backend: store, batchOps: 2, batchWait: 2000}
			server := startCandidate(t, opts)
			for iteration := range 8 {
				key := fmt.Sprintf("cancelled-%d", iteration)
				address := addressFor(t, index, key)
				ctx, cancel := context.WithCancel(t.Context())
				operation := put(t, address, `{"counter":999}`, sink.WriteUpsert)
				cancelled := writeAsync(ctx, server.client, sink.CompletionWaitUntilApplied, operation)
				server.waitQueued(t, "Write")
				cancel()
				select {
				case result := <-cancelled:
					if status.Code(result.err) != codes.Canceled {
						t.Fatalf("queued cancellation returned: %+v", result)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("cancelled caller did not return")
				}
				a, b := addressFor(t, index, "a"), addressFor(t, index, "b")
				applied(t, writeAsync(t.Context(), server.client, sink.CompletionWaitUntilApplied,
					put(t, a, `{"counter":1}`, sink.WriteUpsert), put(t, b, `{"counter":2}`, sink.WriteUpsert)), 2)
				// Cancellation was observed while queued, before any backend work.
				// Cancellation after dispatch intentionally has no rollback promise.
				call := httpCall{endpoint: store.endpoint, method: http.MethodGet, path: "/" + index + "/_doc/" + key}
				code, body := request(t, call)
				if code != http.StatusNotFound {
					t.Fatalf("queued cancelled write reached storage: %d %s", code, body)
				}
			}
		})
	}
}

func assertCounter(t *testing.T, client *sink.Client, address sink.Address, want int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	results, err := client.Read(ctx, address)
	if err != nil || len(results) != 1 || results[0].Status != sink.ReadFound {
		t.Fatalf("read %s: %+v, %v", address.Dataset(), results, err)
	}
	var document struct {
		Counter int `json:"counter"`
	}
	if err := results[0].Document.Decode(&document); err != nil {
		t.Fatal(err)
	}
	if document.Counter != want {
		t.Fatalf("persisted counter=%d, want %d", document.Counter, want)
	}
}

func assertSearchCount(t *testing.T, store backend, index string, want int) {
	t.Helper()
	call := httpCall{endpoint: store.endpoint, method: http.MethodGet, path: "/" + index + "/_count"}
	code, body := request(t, call)
	var result struct {
		Count int `json:"count"`
	}
	err := json.Unmarshal(body, &result)
	if code != http.StatusOK || err != nil || result.Count != want {
		t.Fatalf("acknowledged visibility: HTTP %d %s, want count=%d", code, body, want)
	}
}
