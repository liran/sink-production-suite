//go:build integration

package conformance_test

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	sink "github.com/liran/sink-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSlowStoreSaturationIsBounded(t *testing.T) {
	rounds := 6
	if raw := os.Getenv("SINK_SATURATION_ROUNDS"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 2 || value > 1000 {
			t.Fatal("SINK_SATURATION_ROUNDS must be between 2 and 1000")
		}
		rounds = value
	}
	for _, store := range searchBackends(t) {
		t.Run(store.driver, func(t *testing.T) {
			index := indexFor(t, store, "-1")
			proxy := proxyBackend(t, store)
			opts := serverOptions{backend: proxy.backend, secondary: &store, capacity: 2, maxOps: 8, batchOps: 1, queued: 8}
			server := startCandidate(t, opts)
			healthy, err := sink.NewAddress("secondary", "catalog", index, sink.StringKey("healthy"))
			if err != nil {
				t.Fatal(err)
			}
			baseline := server.metricSnapshot(t)
			for round := range rounds {
				ctx, cancel := context.WithCancel(t.Context())
				t.Cleanup(cancel)
				outcomes := make(chan writeOutcome, 66)
				var gates []*requestGate
				var addresses []sink.Address
				payload := `{"value":"` + strings.Repeat("x", 32<<10) + `"}`
				for i := range 66 {
					key := fmt.Sprintf("slow-%d-%d", round, i)
					address := addressFor(t, index, key)
					addresses = append(addresses, address)
					var gate *requestGate
					if i < 2 {
						gate = proxy.hold("/_bulk", key, 1)
						gates = append(gates, gate)
						t.Cleanup(gate.open)
					}
					operation := put(t, address, payload, sink.WriteUpsert)
					go func() {
						results, err := server.client.Write(ctx, sink.CompletionWaitUntilApplied, operation)
						outcome := writeOutcome{results: results, err: err}
						outcomes <- outcome
					}()
					if gate != nil {
						gate.wait(t)
					}
				}
				// Two executions are held and eight callers fit the queue. Every
				// excess request must return overload promptly, before cancellation.
				deadline := time.NewTimer(5 * time.Second)
				for range 56 {
					select {
					case result := <-outcomes:
						if status.Code(result.err) != codes.ResourceExhausted {
							t.Fatalf("saturated queue did not reject excess work: %+v", result)
						}
					case <-deadline.C:
						t.Fatal("excess callers did not receive bounded overload responses")
					}
				}
				deadline.Stop()
				metrics := server.metricSnapshot(t)
				if metrics[`sink_batcher_queued_operations{method="Write"}`] != 8 || metrics["sink_in_flight_requests"] != 2 {
					t.Fatalf("saturation schedule not established: %+v", metrics)
				}
				// Exercise useful work repeatedly while the other store remains
				// saturated. Every RPC has its own deadline; readiness alone is not
				// evidence that the healthy store continues serving traffic.
				for sample := range 8 {
					started := time.Now()
					call, stop := context.WithTimeout(t.Context(), time.Second)
					operation := put(t, healthy, fmt.Sprintf(`{"counter":%d}`, round*8+sample), sink.WriteUpsert)
					results, err := server.client.Write(call, sink.CompletionWaitUntilApplied, operation)
					stop()
					if err != nil || len(results) != 1 || results[0].Status != sink.WriteApplied {
						t.Fatalf("healthy store stalled behind saturated store: %+v, %v", results, err)
					}
					t.Logf("round=%d healthy_write_latency=%s", round, time.Since(started))
					time.Sleep(100 * time.Millisecond)
				}
				cancel()
				for range 10 {
					select {
					case result := <-outcomes:
						if status.Code(result.err) != codes.Canceled {
							t.Fatalf("cancelled admitted/queued request: %+v", result)
						}
					case <-time.After(5 * time.Second):
						t.Fatal("cancelled work did not release callers")
					}
				}
				server.waitIdle(t)
				for _, gate := range gates {
					gate.open()
				}
				for i := 0; i < len(addresses); i += 8 {
					call, stop := context.WithTimeout(t.Context(), 5*time.Second)
					results, err := server.client.Read(call, addresses[i:min(i+8, len(addresses))]...)
					stop()
					if err != nil || len(results) != min(8, len(addresses)-i) {
						t.Fatalf("read cancelled work: %+v, %v", results, err)
					}
					for _, result := range results {
						if result.Status != sink.ReadNotFound {
							t.Fatalf("rejected/cancelled pre-commit request changed storage: %+v", result)
						}
					}
				}
				metrics = server.waitIdle(t)
				if metrics["go_goroutines"] > baseline["go_goroutines"]+80 ||
					metrics["go_memstats_heap_alloc_bytes"] > baseline["go_memstats_heap_alloc_bytes"]+64<<20 {
					t.Fatalf("resource growth after saturation: baseline=%+v current=%+v", baseline, metrics)
				}
				t.Logf("round=%d quiescent_resource_snapshot=%v", round, metrics)
			}
		})
	}
}

func (c *candidate) metricSnapshot(t *testing.T) map[string]float64 {
	t.Helper()
	call := httpCall{endpoint: c.metrics, method: http.MethodGet}
	code, body := request(t, call)
	if code != http.StatusOK {
		t.Fatalf("metrics: HTTP %d", code)
	}
	metrics := make(map[string]float64)
	for _, line := range strings.Split(string(body), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 || strings.HasPrefix(fields[0], "#") {
			continue
		}
		value, err := strconv.ParseFloat(fields[1], 64)
		if err != nil {
			t.Fatal(err)
		}
		metrics[fields[0]] = value
	}
	for _, name := range []string{"sink_in_flight_requests", "sink_in_flight_bytes", "go_goroutines", "go_memstats_heap_alloc_bytes"} {
		if _, ok := metrics[name]; !ok {
			t.Fatalf("required resource metric is missing: %s", name)
		}
	}
	selected := make(map[string]float64)
	for name, value := range metrics {
		if name == "go_goroutines" || name == "go_memstats_heap_alloc_bytes" ||
			strings.HasPrefix(name, "sink_in_flight_") || strings.HasPrefix(name, "sink_batcher_queued_") {
			selected[name] = value
		}
	}
	return selected
}

func (c *candidate) waitIdle(t *testing.T) map[string]float64 {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var metrics map[string]float64
	for time.Now().Before(deadline) {
		metrics = c.metricSnapshot(t)
		idle := true
		for name, value := range metrics {
			if strings.HasPrefix(name, "sink_") && value != 0 {
				idle = false
			}
		}
		if idle {
			return metrics
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("cancelled work retained execution/queue capacity: %+v", metrics)
	return nil
}
