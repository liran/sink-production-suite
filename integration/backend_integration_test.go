//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	sink "github.com/liran/sink-go"
)

const backendCounterMerge = `
return function(current, incoming)
    current.counter = current.counter + incoming.delta
    current.updated_at = sink.v1.time.now()
    return current
end`

type backendStoreSpec struct {
	name         string
	asynchronous bool
}

type backendDocument struct {
	Value     string `json:"value"`
	Counter   int64  `json:"counter"`
	UpdatedAt string `json:"updated_at,omitempty"`
}

type backendExpectation struct {
	client      *sink.Client
	address     sink.Address
	wantValue   string
	wantCounter int64
}

func TestConfiguredStorageBackendsThroughSink(t *testing.T) {
	specs := configuredBackendStores(t)
	if len(specs) == 0 {
		t.Skip("SINK_BACKEND_STORES is not set")
	}

	environment := newTestEnvironment(t)
	for _, spec := range specs {
		spec := spec
		t.Run(spec.name, func(t *testing.T) {
			testConfiguredBackend(t, environment, spec)
		})
	}
}

func testConfiguredBackend(t *testing.T, environment *testEnvironment, spec backendStoreSpec) {
	t.Helper()
	dataset := fmt.Sprintf("sink-backend-%s-%d", strings.ReplaceAll(spec.name, "-", ""), time.Now().UnixNano())
	address := sinkAddressForStore(t, spec.name, dataset, "counter")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	initial := backendDocument{Value: spec.name, Counter: 0}
	create, err := sink.NewPut(address, initial, sink.WriteCreate)
	if err != nil {
		t.Fatalf("sink.NewPut(create) error = %v", err)
	}
	results, err := environment.client.Write(ctx, sink.CompletionWaitUntilVisible, create)
	if err != nil {
		t.Fatalf("Write(create) error = %v", err)
	}
	assertWriteResults(t, results, sink.WriteApplied)
	initialExpectation := backendExpectation{
		client:      environment.secondaryClient,
		address:     address,
		wantValue:   spec.name,
		wantCounter: 0,
	}
	assertBackendDocument(t, ctx, initialExpectation)

	results, err = environment.secondaryClient.Write(ctx, sink.CompletionWaitUntilApplied, create)
	if err != nil {
		t.Fatalf("Write(duplicate create) error = %v", err)
	}
	if len(results) != 1 || results[0].Status != sink.WritePreconditionFailed || results[0].Failure == nil {
		t.Fatalf("Write(duplicate create) = %+v, want precondition failure", results)
	}

	program, err := sink.NewLuaProgram([]byte(backendCounterMerge))
	if err != nil {
		t.Fatalf("sink.NewLuaProgram() error = %v", err)
	}
	const writers = 32
	start := make(chan struct{})
	errorsChannel := make(chan error, writers)
	var waitGroup sync.WaitGroup
	waitGroup.Add(writers)
	for writerIndex := range writers {
		go func() {
			defer waitGroup.Done()
			<-start
			incoming := map[string]any{"delta": 1}
			mergeOptions := sink.MergeOptions{
				Incoming:            incoming,
				Program:             program,
				MissingDocumentMode: sink.MissingDocumentFail,
			}
			merge, mergeErr := sink.NewMerge(address, mergeOptions)
			if mergeErr != nil {
				errorsChannel <- mergeErr
				return
			}
			client := environment.client
			if writerIndex%2 == 1 {
				client = environment.secondaryClient
			}
			requestContext, requestCancel := context.WithTimeout(ctx, 30*time.Second)
			mergeResults, writeErr := client.Write(requestContext, sink.CompletionWaitUntilApplied, merge)
			requestCancel()
			if writeErr != nil {
				errorsChannel <- writeErr
				return
			}
			if len(mergeResults) != 1 || mergeResults[0].Status != sink.WriteApplied || mergeResults[0].Failure != nil {
				errorsChannel <- fmt.Errorf("writer %d result = %+v", writerIndex, mergeResults)
			}
		}()
	}
	close(start)
	waitGroup.Wait()
	close(errorsChannel)
	for writeErr := range errorsChannel {
		t.Errorf("concurrent merge failed: %v", writeErr)
	}
	if t.Failed() {
		return
	}
	mergedExpectation := backendExpectation{
		client:      environment.client,
		address:     address,
		wantValue:   spec.name,
		wantCounter: writers,
	}
	assertBackendDocument(t, ctx, mergedExpectation)

	asyncAddress := sinkAddressForStore(t, spec.name, dataset, "async")
	asyncDocument := backendDocument{Value: "async", Counter: 7}
	asyncPut, err := sink.NewPut(asyncAddress, asyncDocument, sink.WriteUpsert)
	if err != nil {
		t.Fatalf("sink.NewPut(async) error = %v", err)
	}
	results, err = environment.client.Write(ctx, sink.CompletionReturnAfterAccepted, asyncPut)
	if err != nil {
		t.Fatalf("Write(async) error = %v", err)
	}
	if spec.asynchronous {
		assertWriteResults(t, results, sink.WriteAccepted)
		waitForDocumentFound(t, ctx, environment.secondaryClient, asyncAddress)
		asyncExpectation := backendExpectation{
			client:      environment.secondaryClient,
			address:     asyncAddress,
			wantValue:   "async",
			wantCounter: 7,
		}
		assertBackendDocument(t, ctx, asyncExpectation)
		deleteResults, deleteErr := environment.client.Delete(ctx, sink.CompletionReturnAfterAccepted, asyncAddress)
		if deleteErr != nil {
			t.Fatalf("Delete(async) error = %v", deleteErr)
		}
		if len(deleteResults) != 1 || deleteResults[0].Status != sink.DeleteAccepted || deleteResults[0].Failure != nil {
			t.Fatalf("Delete(async) = %+v, want accepted", deleteResults)
		}
		waitForDocumentNotFound(t, ctx, environment.secondaryClient, asyncAddress)
	} else {
		if len(results) != 1 || results[0].Status != sink.WriteFailed || results[0].Failure == nil {
			t.Fatalf("Write(sync-only async) = %+v, want failed", results)
		}
		failure := results[0].Failure
		if failure.Code != sink.FailureUnavailable || !failure.Retryable {
			t.Fatalf("Write(sync-only async) failure = %+v", failure)
		}
		assertDocumentNotFound(t, ctx, environment.client, asyncAddress)
	}

	deleteResults, err := environment.secondaryClient.Delete(ctx, sink.CompletionWaitUntilVisible, address)
	if err != nil {
		t.Fatalf("Delete(sync) error = %v", err)
	}
	if len(deleteResults) != 1 || deleteResults[0].Status != sink.DeleteApplied || deleteResults[0].Failure != nil {
		t.Fatalf("Delete(sync) = %+v, want applied", deleteResults)
	}
	assertDocumentNotFound(t, ctx, environment.client, address)
}

func assertBackendDocument(
	t *testing.T,
	ctx context.Context,
	expectation backendExpectation,
) {
	t.Helper()
	results, err := expectation.client.Read(ctx, expectation.address)
	if err != nil {
		t.Fatalf("Read(backend document) error = %v", err)
	}
	if len(results) != 1 || results[0].Status != sink.ReadFound {
		t.Fatalf("Read(backend document) = %+v, want found", results)
	}
	document := backendDocument{}
	if err := results[0].Document.Decode(&document); err != nil {
		t.Fatalf("Decode(backend document) error = %v", err)
	}
	if document.Value != expectation.wantValue || document.Counter != expectation.wantCounter {
		t.Fatalf(
			"backend document = %+v, want value=%q counter=%d",
			document,
			expectation.wantValue,
			expectation.wantCounter,
		)
	}
}

func waitForDocumentNotFound(t *testing.T, ctx context.Context, client *sink.Client, address sink.Address) {
	t.Helper()
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		results, err := client.Read(ctx, address)
		if err == nil && len(results) == 1 && results[0].Status == sink.ReadNotFound {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for document deletion: %v", ctx.Err())
		case <-ticker.C:
		}
	}
}

func configuredBackendStores(t *testing.T) []backendStoreSpec {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv("SINK_BACKEND_STORES"))
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	specs := make([]backendStoreSpec, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		fields := strings.Split(strings.TrimSpace(part), ":")
		if len(fields) != 2 || strings.TrimSpace(fields[0]) == "" {
			t.Fatalf("SINK_BACKEND_STORES entry %q must be <store>:<sync|async>", part)
		}
		name := strings.TrimSpace(fields[0])
		if _, exists := seen[name]; exists {
			t.Fatalf("SINK_BACKEND_STORES contains duplicate store %q", name)
		}
		seen[name] = struct{}{}
		spec := backendStoreSpec{name: name}
		switch strings.TrimSpace(fields[1]) {
		case "sync":
		case "async":
			spec.asynchronous = true
		default:
			t.Fatalf("SINK_BACKEND_STORES entry %q has an invalid mode", part)
		}
		specs = append(specs, spec)
	}
	return specs
}
