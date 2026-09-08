//go:build integration

package conformance_test

import (
	"net/http"
	"testing"

	sink "github.com/liran/sink-go"
)

func TestReturnedChainReleasesIndependentPut(t *testing.T) {
	for _, store := range searchBackends(t) {
		t.Run(store.driver, func(t *testing.T) {
			index := indexFor(t, store, "-1")
			proxy := proxyBackend(t, store)
			opts := serverOptions{backend: proxy.backend, batchOps: 3}
			server := startCandidate(t, opts)
			gate := proxy.hold("/_mget", "slow", 1)
			t.Cleanup(gate.open)
			fast, slow := addressFor(t, index, "fast"), addressFor(t, index, "slow")
			firstPut := put(t, fast, `{"counter":1}`, sink.WriteUpsert)
			returned := merge(t, slow, increment).WithReturnedDocument()
			first := writeAsync(t.Context(), server.client, sink.CompletionWaitUntilApplied, firstPut, returned, returned)
			gate.wait(t)
			call := httpCall{endpoint: store.endpoint, method: http.MethodGet, path: "/" + index + "/_doc/fast"}
			code, body := request(t, call)
			if code != http.StatusOK {
				t.Fatalf("independent Put blocked by returned chain: %d %s", code, body)
			}
			next := put(t, fast, `{"counter":2}`, sink.WriteUpsert)
			applied(t, writeAsync(t.Context(), server.client, sink.CompletionWaitUntilApplied, next), 1)
			gate.open()
			applied(t, first, 3)
			assertCounter(t, server.client, fast, 2)
			assertCounter(t, server.client, slow, 2)
		})
	}
}
