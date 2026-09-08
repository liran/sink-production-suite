//go:build integration

package conformance_test

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestRequestGateDiscardPreventsLateForwarding(t *testing.T) {
	var forwarded atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		forwarded.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(upstream.Close)
	store := backend{driver: "elasticsearch", endpoint: upstream.URL}
	proxy := proxyBackend(t, store)
	gate := proxy.hold("/_bulk", "held", 1)
	client := &http.Client{Timeout: 5 * time.Second}
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, proxy.backend.endpoint+"/_bulk", bytes.NewBufferString(`{"_id":"held"}`))
	if err != nil {
		t.Fatal(err)
	}
	type outcome struct {
		status int
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		response, err := client.Do(request)
		result := outcome{err: err}
		if response != nil {
			result.status = response.StatusCode
			response.Body.Close()
		}
		done <- result
	}()
	gate.wait(t)
	// Keep the connection alive to model delayed disconnect notification after
	// a candidate crash. The proxy must discard, not forward, the held write.
	gate.discard()
	result := <-done
	if result.err != nil || result.status != http.StatusServiceUnavailable || forwarded.Load() != 0 {
		t.Fatalf("discarded request reached backend: status=%d forwarded=%d error=%v", result.status, forwarded.Load(), result.err)
	}
	response, err := client.Get(proxy.backend.endpoint + "/recovered")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNoContent || forwarded.Load() != 1 {
		t.Fatal("discarded gate blocked subsequent requests")
	}
}
