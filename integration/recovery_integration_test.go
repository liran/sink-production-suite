//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	sink "github.com/liran/sink-go"
	"github.com/liran/sink-production-suite/internal/fixture"
	"github.com/liran/sink-production-suite/internal/reference"
	"github.com/liran/sink-production-suite/programs"
)

func TestKafkaBacklogSurvivesWorkerRestart(t *testing.T) {
	phase := os.Getenv("SINK_RECOVERY_PHASE")
	if phase == "" {
		t.Skip("SINK_RECOVERY_PHASE is not set")
	}
	index := os.Getenv("SINK_RECOVERY_INDEX")
	key := os.Getenv("SINK_RECOVERY_KEY")
	if index == "" || key == "" {
		t.Fatal("SINK_RECOVERY_INDEX and SINK_RECOVERY_KEY are required")
	}
	environment := newTestEnvironment(t)
	address := sinkAddress(t, index, key)
	ctx, cancel := context.WithTimeout(context.Background(), productionTestTimeout)
	defer cancel()
	switch phase {
	case "publish":
		environment.ensureIndex(t, index)
		publishRecoveryBacklog(t, ctx, environment.client, address, key)
		results, err := environment.client.Read(ctx, address)
		if err != nil {
			t.Fatalf("Read(before worker restart) error = %v", err)
		}
		if len(results) != 1 || results[0].Status != sink.ReadNotFound {
			t.Fatalf("Read(before worker restart) = %+v, want not found", results)
		}
	case "verify":
		waitForRecoveryProduct(t, ctx, environment.client, address)
		environment.deleteIndex(t, index)
	default:
		t.Fatalf("unsupported SINK_RECOVERY_PHASE %q", phase)
	}
}

func publishRecoveryBacklog(t *testing.T, ctx context.Context, client *sink.Client, address sink.Address, key string) {
	t.Helper()
	base := fixture.RepresentativeProduct(0)
	base.UID = key
	base.UIDs = []string{key}
	base.Languages = []string{"base"}
	put, err := sink.NewPut(address, documentForAddress(t, address, base), sink.WriteUpsert)
	if err != nil {
		t.Fatalf("sink.NewPut(recovery) error = %v", err)
	}
	operations := []sink.WriteOperation{put}
	for index := range 20 {
		incoming := &reference.Product{
			UID:       key,
			Platform:  base.Platform,
			ID:        base.ID,
			URL:       base.URL,
			Languages: []string{fmt.Sprintf("backlog-%02d", index)},
			Available: true,
		}
		operation := newMergeOperation(t, address, incoming, programs.ProductMerge, sink.MissingDocumentFail)
		operations = append(operations, operation)
	}
	results, err := client.Write(ctx, sink.CompletionReturnAfterAccepted, operations...)
	if err != nil {
		t.Fatalf("Write(recovery backlog) error = %v", err)
	}
	assertWriteResults(t, results, sink.WriteAccepted)
}

func waitForRecoveryProduct(t *testing.T, ctx context.Context, client *sink.Client, address sink.Address) {
	t.Helper()
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		results, err := client.Read(ctx, address)
		if err == nil && len(results) == 1 && results[0].Status == sink.ReadFound {
			product := &reference.Product{}
			decodeErr := results[0].Document.Decode(product)
			if decodeErr == nil && len(product.Languages) == 21 {
				return
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for recovery backlog: %v", ctx.Err())
		case <-ticker.C:
		}
	}
}
