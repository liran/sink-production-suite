//go:build integration

package conformance_test

import (
	"os"
	"strconv"
	"testing"

	sink "github.com/liran/sink-go"
	"github.com/liran/sink-production-suite/internal/statecheck"
)

func TestOperationStateMachine(t *testing.T) {
	seed, steps := int64(37), 256
	if raw := os.Getenv("SINK_STATE_SEED"); raw != "" {
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			t.Fatalf("SINK_STATE_SEED: %v", err)
		}
		seed = value
	}
	if raw := os.Getenv("SINK_STATE_STEPS"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 1 || value > 100000 {
			t.Fatal("SINK_STATE_STEPS must be between 1 and 100000")
		}
		steps = value
	}
	profiles := []struct {
		name      string
		unbatched bool
		batchOps  int
		rpcSize   int
	}{
		{name: "serial-rpcs", rpcSize: 1},
		{name: "default-batching", rpcSize: 16},
		{name: "batching-disabled", unbatched: true, rpcSize: 16},
		{name: "small-batches", batchOps: 3, rpcSize: 16},
	}
	for _, store := range searchBackends(t) {
		for _, profile := range profiles {
			t.Run(store.driver+"/"+profile.name, func(t *testing.T) {
				index := indexFor(t, store, "-1")
				opts := serverOptions{backend: store, unbatched: profile.unbatched, batchOps: profile.batchOps}
				server := startCandidate(t, opts)
				t.Logf("state model seed=%d steps=%d", seed, steps)
				check := statecheck.Options{Client: server.client, Store: "primary", Dataset: index,
					Encoding: sink.DocumentEncodingJSON, BatchSize: profile.rpcSize, Seed: seed, Steps: steps}
				statecheck.Run(t, check)
			})
		}
	}
}
