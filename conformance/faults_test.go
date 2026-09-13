//go:build integration

package conformance_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	sink "github.com/liran/sink-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Retries of an ambiguous mutation need business idempotence. This fixture
// carries one operation ID; it does not claim general server deduplication.
const idempotentIncrement = `return function(current, incoming)
  current = current or {counter = 0}
  if current.last_operation ~= "boundary-operation" then
    current.counter = current.counter + incoming.counter
    current.last_operation = "boundary-operation"
  end
  return current
end`

func TestSyncCrashBoundaries(t *testing.T) {
	for _, store := range searchBackends(t) {
		for _, phase := range []string{"before-commit", "after-commit", "after-acknowledgement"} {
			t.Run(store.driver+"/"+phase, func(t *testing.T) {
				index := indexFor(t, store, "-1")
				proxy := proxyBackend(t, store)
				opts := serverOptions{backend: proxy.backend}
				server := startCandidate(t, opts)
				address := addressFor(t, index, "crash")
				var gate *requestGate
				switch phase {
				case "before-commit":
					gate = proxy.hold("/_bulk", "crash", 1)
				case "after-commit":
					gate = proxy.holdResponse("/_bulk", "crash", false)
				}
				if gate != nil {
					t.Cleanup(gate.open)
				}
				operation := merge(t, address, idempotentIncrement)
				done := writeAsync(t.Context(), server.client, sink.CompletionWaitUntilApplied, operation)
				if gate == nil {
					applied(t, done, 1)
				} else {
					gate.wait(t)
					assertBackendCounter(t, store, index, phase != "before-commit")
					assertPending(t, done)
				}
				server.crash(t)
				if gate != nil {
					if phase == "before-commit" {
						gate.discard()
					} else {
						gate.open()
					}
					assertUnknownWrite(t, done)
				}
				restarted := startCandidate(t, opts)
				if phase == "before-commit" {
					assertAbsent(t, restarted.client, address)
				} else {
					assertCounter(t, restarted.client, address, 1)
				}
				applied(t, writeAsync(t.Context(), restarted.client, sink.CompletionWaitUntilApplied, operation), 1)
				assertCounter(t, restarted.client, address, 1)
				// Verify a new logical mutation still progresses after recovery.
				applied(t, writeAsync(t.Context(), restarted.client, sink.CompletionWaitUntilApplied, merge(t, address, increment)), 1)
				assertCounter(t, restarted.client, address, 2)
			})
		}
	}
}

func TestLostBackendResponseDoesNotReplayMutation(t *testing.T) {
	for _, store := range searchBackends(t) {
		for _, batchOps := range []int{1000, 1} {
			t.Run(fmt.Sprintf("%s/batch-ops=%d", store.driver, batchOps), func(t *testing.T) {
				index := indexFor(t, store, "-1")
				proxy := proxyBackend(t, store)
				opts := serverOptions{backend: proxy.backend, batchOps: batchOps}
				server := startCandidate(t, opts)
				address := addressFor(t, index, "crash")
				gate := proxy.holdResponse("/_bulk", "crash", true)
				t.Cleanup(gate.open)
				// A non-idempotent increment exposes an unsafe internal replay.
				done := writeAsync(t.Context(), server.client, sink.CompletionWaitUntilApplied, merge(t, address, increment))
				gate.wait(t)
				assertBackendCounter(t, store, index, true)
				assertPending(t, done)
				gate.open()
				assertUnknownWrite(t, done)
				assertCounter(t, server.client, address, 1)
				if writes := proxy.count("/_bulk", "crash"); writes != 1 {
					t.Fatalf("ambiguous non-idempotent mutation replayed internally: %d writes", writes)
				}
				applied(t, writeAsync(t.Context(), server.client, sink.CompletionWaitUntilApplied, merge(t, address, increment)), 1)
				assertCounter(t, server.client, address, 2)
			})
		}
	}
}

func TestCancellationAfterCommitRetainsState(t *testing.T) {
	for _, store := range searchBackends(t) {
		t.Run(store.driver, func(t *testing.T) {
			index := indexFor(t, store, "-1")
			proxy := proxyBackend(t, store)
			opts := serverOptions{backend: proxy.backend}
			server := startCandidate(t, opts)
			address := addressFor(t, index, "crash")
			gate := proxy.holdResponse("/_bulk", "crash", false)
			t.Cleanup(gate.open)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := writeAsync(ctx, server.client, sink.CompletionWaitUntilApplied, merge(t, address, increment))
			gate.wait(t)
			assertBackendCounter(t, store, index, true)
			cancel()
			assertUnknownWrite(t, done)
			gate.open()
			assertCounter(t, server.client, address, 1)
			applied(t, writeAsync(t.Context(), server.client, sink.CompletionWaitUntilApplied, merge(t, address, increment)), 1)
			assertCounter(t, server.client, address, 2)
		})
	}
}

func assertPending(t *testing.T, done <-chan writeOutcome) {
	t.Helper()
	select {
	case result := <-done:
		t.Fatalf("acknowledged before the held commit response: %+v", result)
	default:
	}
}

func assertUnknownWrite(t *testing.T, done <-chan writeOutcome) {
	t.Helper()
	select {
	case result := <-done:
		if result.err != nil {
			switch status.Code(result.err) {
			case codes.Unavailable, codes.Canceled, codes.DeadlineExceeded:
				return
			default:
				t.Fatalf("unexpected RPC error at ambiguous commit boundary: %v", result.err)
			}
		}
		if len(result.results) != 1 || result.results[0].Status != sink.WriteFailed || result.results[0].Failure == nil ||
			!result.results[0].Failure.Retryable || len(result.results[0].Revision.Bytes()) != 0 {
			t.Fatalf("ambiguous commit incorrectly acknowledged or permanently rejected: %+v", result)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("caller did not resolve after injected failure")
	}
}

func assertAbsent(t *testing.T, client *sink.Client, address sink.Address) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	results, err := client.Read(ctx, address)
	if err != nil || len(results) != 1 || results[0].Status != sink.ReadNotFound {
		t.Fatalf("expected absent record: %+v, %v", results, err)
	}
}

func assertBackendCounter(t *testing.T, store backend, index string, found bool) {
	t.Helper()
	call := httpCall{endpoint: store.endpoint, method: http.MethodGet, path: "/" + index + "/_doc/crash"}
	code, body := request(t, call)
	want := http.StatusNotFound
	if found {
		want = http.StatusOK
	}
	if code != want {
		t.Fatalf("fault boundary was not reached: backend HTTP %d %s, want %d", code, body, want)
	}
	if found {
		var document struct {
			Source struct {
				Counter int `json:"counter"`
			} `json:"_source"`
		}
		if err := json.Unmarshal(body, &document); err != nil || document.Source.Counter != 1 {
			t.Fatalf("commit boundary has unexpected persisted state: %s, %v", body, err)
		}
	}
}
