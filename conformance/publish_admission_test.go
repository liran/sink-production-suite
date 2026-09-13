//go:build integration

package conformance_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	sink "github.com/liran/sink-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestPublishingSurvivesSynchronousSaturation(t *testing.T) {
	broker := startBroker(t)
	for _, store := range searchBackends(t) {
		for _, batchOps := range []int{1000, 1} {
			t.Run(fmt.Sprintf("%s/batch-ops=%d", store.driver, batchOps), func(t *testing.T) {
				index := indexFor(t, store, "-1")
				proxy := proxyBackend(t, store)
				topic := fmt.Sprintf("sink-publish-isolation-%d", time.Now().UnixNano())
				opts := serverOptions{backend: proxy.backend, broker: broker.address, topic: topic, capacity: 1, batchOps: batchOps}
				server := startCandidate(t, opts)
				blocked := addressFor(t, index, "blocked")
				gate := proxy.hold("/_bulk", "blocked", 1)
				t.Cleanup(gate.open)
				synchronous := writeAsync(t.Context(), server.client, sink.CompletionWaitUntilApplied, put(t, blocked, `{"counter":1}`, sink.WriteUpsert))
				gate.wait(t)
				if server.metricSnapshot(t)["sink_in_flight_requests"] != 1 {
					t.Fatal("synchronous store capacity was not occupied")
				}
				// No worker exists yet. Acceptance must come from real Kafka while
				// the same store's synchronous execution slot remains occupied.
				kept, removed := addressFor(t, index, "kept"), addressFor(t, index, "removed")
				for _, address := range []sink.Address{kept, removed} {
					ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
					operation := put(t, address, `{"counter":7}`, sink.WriteUpsert)
					results, err := server.client.Write(ctx, sink.CompletionReturnAfterAccepted, operation)
					cancel()
					if err != nil || len(results) != 1 || results[0].Status != sink.WriteAccepted || results[0].Failure != nil {
						t.Fatalf("synchronous saturation blocked Kafka publishing: %+v, %v", results, err)
					}
				}
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				deleted, err := server.client.Delete(ctx, sink.CompletionReturnAfterAccepted, removed)
				cancel()
				if err != nil || len(deleted) != 1 || deleted[0].Status != sink.DeleteAccepted || deleted[0].Failure != nil {
					t.Fatalf("synchronous saturation blocked Kafka delete: %+v, %v", deleted, err)
				}
				broker.assertEnd(t, topic, 3)
				select {
				case result := <-synchronous:
					t.Fatalf("storage gate released before publishing completed: %+v", result)
				default:
				}
				gate.open()
				applied(t, synchronous, 1)
				assertAbsent(t, server.client, kept)
				assertAbsent(t, server.client, removed)
				opts.worker, opts.backend = true, store
				startCandidate(t, opts)
				broker.waitCommitted(t, topic, 3)
				assertCounter(t, server.client, kept, 7)
				assertAbsent(t, server.client, removed)
				assertCounter(t, server.client, blocked, 1)
				broker.assertEnd(t, topic+".dlq", 0)
				server.waitIdle(t)
			})
		}
	}
}

func TestSynchronousWritesSurvivePublisherSaturation(t *testing.T) {
	broker := startBroker(t)
	for _, store := range searchBackends(t) {
		t.Run(store.driver, func(t *testing.T) {
			index := indexFor(t, store, "-1")
			topic := fmt.Sprintf("sink-broker-isolation-%d", time.Now().UnixNano())
			opts := serverOptions{backend: store, broker: broker.address, topic: topic, capacity: 1, batchOps: 1}
			server := startCandidate(t, opts)
			broker.docker(t, "pause")
			paused := true
			t.Cleanup(func() {
				if paused {
					broker.docker(t, "unpause")
				}
			})
			ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
			err := broker.client.Ping(ctx)
			cancel()
			if err == nil {
				t.Fatal("paused broker still answered requests")
			}
			first := addressFor(t, index, "first")
			pending := writeAsync(t.Context(), server.client, sink.CompletionReturnAfterAccepted, put(t, first, `{"counter":3}`, sink.WriteUpsert))
			deadline := time.Now().Add(5 * time.Second)
			for server.metricSnapshot(t)[`sink_admission_pool_requests{pool="publish"}`] != 1 {
				if time.Now().After(deadline) {
					t.Fatal("publish admission did not reach its configured store limit")
				}
				time.Sleep(10 * time.Millisecond)
			}
			select {
			case result := <-pending:
				t.Fatalf("publisher completed without a Kafka acknowledgement: %+v", result)
			default:
			}
			rejected := addressFor(t, index, "rejected")
			ctx, cancel = context.WithTimeout(t.Context(), time.Second)
			_, err = server.client.Write(ctx, sink.CompletionReturnAfterAccepted, put(t, rejected, `{"counter":99}`, sink.WriteUpsert))
			cancel()
			if status.Code(err) != codes.ResourceExhausted {
				t.Fatalf("saturated publisher did not reject excess write: %v", err)
			}
			ctx, cancel = context.WithTimeout(t.Context(), time.Second)
			_, err = server.client.Delete(ctx, sink.CompletionReturnAfterAccepted, first)
			cancel()
			if status.Code(err) != codes.ResourceExhausted {
				t.Fatalf("saturated publisher did not reject excess delete: %v", err)
			}
			healthy := addressFor(t, index, "healthy")
			applied(t, writeAsync(t.Context(), server.client, sink.CompletionWaitUntilApplied, put(t, healthy, `{"counter":5}`, sink.WriteUpsert)), 1)
			assertCounter(t, server.client, healthy, 5)
			broker.docker(t, "unpause")
			paused = false
			select {
			case result := <-pending:
				if result.err != nil || len(result.results) != 1 || result.results[0].Status != sink.WriteAccepted || result.results[0].Failure != nil {
					t.Fatalf("publisher did not recover after broker resumed: %+v", result)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("publisher did not recover after broker resumed")
			}
			next := addressFor(t, index, "next")
			accepted(t, server.client, put(t, next, `{"counter":8}`, sink.WriteUpsert))
			broker.assertEnd(t, topic, 2)
			opts.worker = true
			startCandidate(t, opts)
			broker.waitCommitted(t, topic, 2)
			assertCounter(t, server.client, first, 3)
			assertCounter(t, server.client, next, 8)
			assertAbsent(t, server.client, rejected)
			broker.assertEnd(t, topic+".dlq", 0)
			server.waitIdle(t)
		})
	}
}
