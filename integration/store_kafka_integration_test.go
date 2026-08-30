//go:build integration

package integration_test

import (
	"context"
	"testing"
	"time"

	sink "github.com/liran/sink-go"
)

func TestStoreKafkaRoutingAndSyncOnlyBehavior(t *testing.T) {
	environment := newTestEnvironment(t)
	index := environment.createIndex(t, "store-kafka-routing")
	ctx, cancel := context.WithTimeout(context.Background(), productionTestTimeout)
	defer cancel()

	primaryAddress := sinkAddressForStore(t, "primary", index, "primary-async")
	secondaryAddress := sinkAddressForStore(t, "secondary", index, "secondary-async")
	syncOnlyAddress := sinkAddressForStore(t, "sync-only", index, "sync-only")
	primaryDocument := map[string]any{"store": "primary"}
	secondaryDocument := map[string]any{"store": "secondary"}
	syncOnlyDocument := map[string]any{"store": "sync-only"}

	primaryPut, err := sink.NewPut(primaryAddress, primaryDocument, sink.WriteUpsert)
	if err != nil {
		t.Fatalf("sink.NewPut(primary) error = %v", err)
	}
	secondaryPut, err := sink.NewPut(secondaryAddress, secondaryDocument, sink.WriteUpsert)
	if err != nil {
		t.Fatalf("sink.NewPut(secondary) error = %v", err)
	}
	syncOnlyAsyncPut, err := sink.NewPut(syncOnlyAddress, syncOnlyDocument, sink.WriteUpsert)
	if err != nil {
		t.Fatalf("sink.NewPut(sync-only async) error = %v", err)
	}

	results, err := environment.client.Write(
		ctx,
		sink.CompletionReturnAfterAccepted,
		primaryPut,
		secondaryPut,
		syncOnlyAsyncPut,
	)
	if err != nil {
		t.Fatalf("Write(mixed store async batch) error = %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("Write(mixed store async batch) returned %d results, want 3", len(results))
	}
	for indexValue, result := range results {
		if result.OperationIndex != indexValue {
			t.Fatalf("result[%d].OperationIndex = %d", indexValue, result.OperationIndex)
		}
	}
	if results[0].Status != sink.WriteAccepted || results[0].Failure != nil {
		t.Fatalf("primary result = %+v, want accepted", results[0])
	}
	if results[1].Status != sink.WriteAccepted || results[1].Failure != nil {
		t.Fatalf("secondary result = %+v, want accepted", results[1])
	}
	syncOnlyResult := results[2]
	if syncOnlyResult.Status != sink.WriteFailed || syncOnlyResult.Failure == nil {
		t.Fatalf("sync-only async result = %+v, want failed", syncOnlyResult)
	}
	if syncOnlyResult.Failure.Code != sink.FailureUnavailable || !syncOnlyResult.Failure.Retryable {
		t.Fatalf("sync-only async failure = %+v, want retryable unavailable", syncOnlyResult.Failure)
	}

	waitForDocumentFound(t, ctx, environment.client, primaryAddress)
	waitForDocumentFound(t, ctx, environment.client, secondaryAddress)
	assertDocumentNotFound(t, ctx, environment.client, syncOnlyAddress)

	syncOnlyPut, err := sink.NewPut(syncOnlyAddress, syncOnlyDocument, sink.WriteUpsert)
	if err != nil {
		t.Fatalf("sink.NewPut(sync-only synchronous) error = %v", err)
	}
	results, err = environment.client.Write(ctx, sink.CompletionWaitUntilVisible, syncOnlyPut)
	if err != nil {
		t.Fatalf("Write(sync-only synchronous) error = %v", err)
	}
	assertWriteResults(t, results, sink.WriteApplied)
	waitForDocumentFound(t, ctx, environment.client, syncOnlyAddress)

	deleteResults, err := environment.client.Delete(ctx, sink.CompletionReturnAfterAccepted, syncOnlyAddress)
	if err != nil {
		t.Fatalf("Delete(sync-only asynchronous) error = %v", err)
	}
	if len(deleteResults) != 1 || deleteResults[0].Status != sink.DeleteFailed || deleteResults[0].Failure == nil {
		t.Fatalf("Delete(sync-only asynchronous) = %+v, want failed", deleteResults)
	}
	deleteFailure := deleteResults[0].Failure
	if deleteFailure.Code != sink.FailureUnavailable || !deleteFailure.Retryable {
		t.Fatalf("Delete(sync-only asynchronous) failure = %+v, want retryable unavailable", deleteFailure)
	}
	waitForDocumentFound(t, ctx, environment.client, syncOnlyAddress)

	deleteResults, err = environment.client.Delete(ctx, sink.CompletionWaitUntilVisible, syncOnlyAddress)
	if err != nil {
		t.Fatalf("Delete(sync-only synchronous) error = %v", err)
	}
	if len(deleteResults) != 1 || deleteResults[0].Status != sink.DeleteApplied || deleteResults[0].Failure != nil {
		t.Fatalf("Delete(sync-only synchronous) = %+v, want applied", deleteResults)
	}
	assertDocumentNotFound(t, ctx, environment.client, syncOnlyAddress)
}

func waitForDocumentFound(t *testing.T, ctx context.Context, client *sink.Client, address sink.Address) {
	t.Helper()
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		results, err := client.Read(ctx, address)
		if err == nil && len(results) == 1 && results[0].Status == sink.ReadFound {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for document %q: %v", address.Dataset(), ctx.Err())
		case <-ticker.C:
		}
	}
}

func assertDocumentNotFound(t *testing.T, ctx context.Context, client *sink.Client, address sink.Address) {
	t.Helper()
	results, err := client.Read(ctx, address)
	if err != nil {
		t.Fatalf("Read(sync-only after async rejection) error = %v", err)
	}
	if len(results) != 1 || results[0].Status != sink.ReadNotFound {
		t.Fatalf("Read(sync-only after async rejection) = %+v, want not found", results)
	}
}
