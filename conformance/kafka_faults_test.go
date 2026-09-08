//go:build integration

package conformance_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	sink "github.com/liran/sink-go"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type testBroker struct {
	address   string
	admin     *kadm.Client
	client    *kgo.Client
	container string
}

// This is a disposable single broker for the commit-boundary contract. It is
// deliberately not presented as multi-broker durability qualification.
func startBroker(t *testing.T) *testBroker {
	t.Helper()
	address := freeAddress(t)
	name := fmt.Sprintf("sink-boundary-kafka-%d", time.Now().UnixNano())
	args := []string{"run", "--detach", "--name", name, "--publish", address + ":19092"}
	settings := []string{
		"KAFKA_NODE_ID=1", "KAFKA_PROCESS_ROLES=broker,controller",
		"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
		"KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://" + address,
		"KAFKA_LISTENERS=CONTROLLER://:29093,PLAINTEXT://:19092",
		"KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT", "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
		"KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:29093",
		"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1", "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0",
		"KAFKA_NUM_PARTITIONS=1", "KAFKA_HEAP_OPTS=-Xms256m -Xmx256m",
	}
	for _, setting := range settings {
		args = append(args, "--env", setting)
	}
	args = append(args, "apache/kafka:4.2.1@sha256:9916d60eca5d599550e2c320230808fda342124ba550bb4ac4ea8591803262a0")
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	output, err := exec.CommandContext(ctx, "docker", args...).Output()
	if err != nil {
		t.Fatalf("start isolated Kafka: %s, %v", output, err)
	}
	container := strings.TrimSpace(string(output))
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		logs, logErr := exec.CommandContext(cleanup, "docker", "logs", container).CombinedOutput()
		if root := os.Getenv("SINK_CONFORMANCE_ARTIFACTS"); root != "" {
			if err := os.WriteFile(filepath.Join(root, name+".log"), logs, 0600); err != nil {
				t.Error(err)
			}
		}
		if logErr != nil {
			t.Errorf("capture broker log: %v", logErr)
		}
		// Only the exact container created by this test is removed.
		output, err := exec.CommandContext(cleanup, "docker", "rm", "--force", "--volumes", container).CombinedOutput()
		if err != nil {
			t.Errorf("remove isolated broker: %s, %v", output, err)
		}
		if t.Failed() {
			t.Logf("broker log: %s", logs)
		}
	})
	client, err := kgo.NewClient(kgo.SeedBrokers(address), kgo.RequestTimeoutOverhead(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	for {
		attempt, stop := context.WithTimeout(ctx, time.Second)
		err = client.Ping(attempt)
		stop()
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("broker did not become ready: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	broker := &testBroker{address: address, admin: kadm.NewClient(client), client: client, container: container}
	return broker
}

func TestAcceptedMutationCrashBoundaries(t *testing.T) {
	broker := startBroker(t)
	for _, store := range searchBackends(t) {
		for _, phase := range []string{"before-commit", "after-commit", "after-offset-commit"} {
			t.Run(store.driver+"/"+phase, func(t *testing.T) {
				index := indexFor(t, store, "-1")
				proxy := proxyBackend(t, store)
				topic := fmt.Sprintf("sink-boundary-%d", time.Now().UnixNano())
				opts := serverOptions{backend: store, broker: broker.address, topic: topic}
				publisher := startCandidate(t, opts)
				address := addressFor(t, index, "crash")
				operation := merge(t, address, idempotentIncrement)
				accepted(t, publisher.client, operation)
				assertAbsent(t, publisher.client, address)
				broker.assertEnd(t, topic, 1)
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
				opts.worker, opts.backend = true, proxy.backend
				worker := startCandidate(t, opts)
				if gate != nil {
					gate.wait(t)
					assertBackendCounter(t, store, index, phase != "before-commit")
					if offset := broker.committed(t, topic); offset > 0 {
						t.Fatalf("worker committed source offset %d before observing backend completion", offset)
					}
				} else {
					broker.waitCommitted(t, topic, 1)
					assertCounter(t, publisher.client, address, 1)
				}
				worker.crash(t)
				if gate != nil {
					if phase == "before-commit" {
						gate.discard()
						assertAbsent(t, publisher.client, address)
					} else {
						gate.open()
					}
				}
				startCandidate(t, opts)
				// A following accepted record proves the replacement owns and
				// drains the partition, including the previously unresolved prefix.
				next := addressFor(t, index, "following")
				accepted(t, publisher.client, put(t, next, `{"counter":7}`, sink.WriteUpsert))
				broker.waitCommitted(t, topic, 2)
				assertCounter(t, publisher.client, address, 1)
				assertCounter(t, publisher.client, next, 7)
				reads := proxy.count("/_mget", "crash")
				if phase == "after-offset-commit" && reads != 1 {
					t.Fatalf("committed mutation was replayed on restart: %d snapshots", reads)
				}
				if phase != "after-offset-commit" && reads < 2 {
					t.Fatalf("uncommitted mutation was not replayed after worker crash: %d snapshots", reads)
				}
				broker.assertEnd(t, topic+".dlq", 0)
			})
		}
	}
	t.Run("shutdown-during-broker-outage", func(t *testing.T) {
		store := searchBackends(t)[0]
		index := indexFor(t, store, "-1")
		topic := fmt.Sprintf("sink-shutdown-%d", time.Now().UnixNano())
		opts := serverOptions{backend: store, broker: broker.address, topic: topic}
		publisher := startCandidate(t, opts)
		opts.worker = true
		worker := startCandidate(t, opts)
		address := addressFor(t, index, "crash")
		accepted(t, publisher.client, merge(t, address, idempotentIncrement))
		broker.waitCommitted(t, topic, 1)
		assertCounter(t, publisher.client, address, 1)
		broker.docker(t, "pause")
		t.Cleanup(func() { broker.docker(t, "unpause") })
		ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
		err := broker.client.Ping(ctx)
		cancel()
		if err == nil {
			t.Fatal("paused broker unexpectedly answered a Kafka request")
		}
		worker.stop(t)
		publisher.stop(t)
	})
}

func (b *testBroker) docker(t *testing.T, action string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, "docker", action, b.container).CombinedOutput()
	if err != nil {
		t.Fatalf("broker %s: %s, %v", action, output, err)
	}
}

func accepted(t *testing.T, client *sink.Client, operation sink.WriteOperation) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	results, err := client.Write(ctx, sink.CompletionReturnAfterAccepted, operation)
	if err != nil || len(results) != 1 || results[0].Status != sink.WriteAccepted || results[0].Failure != nil {
		t.Fatalf("accept mutation: %+v, %v", results, err)
	}
}

func (b *testBroker) assertEnd(t *testing.T, topic string, want int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	offsets, err := b.admin.ListEndOffsets(ctx, topic)
	if err != nil || offsets.Error() != nil {
		t.Fatalf("source/DLQ end offsets: %v, %v", err, offsets.Error())
	}
	offset, ok := offsets.Lookup(topic, 0)
	if !ok || len(offsets[topic]) != 1 || offset.Offset != want {
		t.Fatalf("topic %s end offsets: %+v, want one partition at %d", topic, offsets, want)
	}
	t.Logf("topic=%s end_offset=%d", topic, offset.Offset)
}

func (b *testBroker) committed(t *testing.T, topic string) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	offsets, err := b.admin.FetchOffsetsForTopics(ctx, topic+"-workers", topic)
	if err != nil || offsets.Error() != nil {
		t.Fatalf("fetch committed offsets: %v, %v", err, offsets.Error())
	}
	offset, ok := offsets.Lookup(topic, 0)
	if !ok {
		t.Fatal("source partition missing from committed-offset observation")
	}
	return offset.At
}

func (b *testBroker) waitCommitted(t *testing.T, topic string, want int64) {
	t.Helper()
	// Keep the production client's default 45-second session timeout and
	// 60-second rebalance window. A SIGKILL cannot send LeaveGroup.
	deadline := time.Now().Add(75 * time.Second)
	var last int64
	for time.Now().Before(deadline) {
		last = b.committed(t, topic)
		if last == want {
			t.Logf("topic=%s committed_offset=%d", topic, last)
			return
		}
		if last > want {
			t.Fatalf("committed unexpected source offset %d, want %d", last, want)
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("source did not drain after restart: committed=%d want=%d", last, want)
}
