//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	sink "github.com/liran/sink-go"
	"github.com/liran/sink-production-suite/internal/fixture"
	"github.com/liran/sink-production-suite/internal/reference"
	"github.com/liran/sink-production-suite/programs"
)

func TestProductMergeMatchesReferenceThroughSinkAndOpenSearch(t *testing.T) {
	environment := newTestEnvironment(t)
	index := environment.createIndex(t, "product-contract")
	current, incoming := fixture.ProductPair()
	address := sinkAddress(t, index, current.UID)
	ctx, cancel := context.WithTimeout(context.Background(), productionTestTimeout)
	defer cancel()
	writePut(t, ctx, environment.client, address, current)
	operation := newMergeOperation(t, address, incoming, programs.ProductMerge, sink.MissingDocumentFail)
	results, err := environment.client.Write(ctx, sink.CompletionWaitUntilVisible, operation)
	if err != nil {
		t.Fatalf("Write(product merge) error = %v", err)
	}
	assertWriteResults(t, results, sink.WriteApplied)
	actual := readProduct(t, ctx, environment.client, address)
	if actual.LastFoundAt == nil {
		t.Fatal("merged product has no last_found_at")
	}
	expected := reference.MergeProduct(current, incoming, *actual.LastFoundAt)
	assertEquivalent(t, expected, actual)
}

func TestOfferMergeMatchesReferenceThroughSinkAndOpenSearch(t *testing.T) {
	environment := newTestEnvironment(t)
	index := environment.createIndex(t, "offer-contract")
	current, incoming := fixture.OfferPair()
	address := sinkAddress(t, index, current.UID)
	ctx, cancel := context.WithTimeout(context.Background(), productionTestTimeout)
	defer cancel()
	writePut(t, ctx, environment.client, address, current)
	operation := newMergeOperation(t, address, incoming, programs.OfferMerge, sink.MissingDocumentFail)
	results, err := environment.client.Write(ctx, sink.CompletionWaitUntilVisible, operation)
	if err != nil {
		t.Fatalf("Write(offer merge) error = %v", err)
	}
	assertWriteResults(t, results, sink.WriteApplied)
	actual := readOffer(t, ctx, environment.client, address)
	if actual.LastFoundAt == nil {
		t.Fatal("merged offer has no last_found_at")
	}
	expected := reference.MergeOffer(current, incoming, *actual.LastFoundAt)
	assertEquivalent(t, expected, actual)
}

func TestConcurrentProductMergesAcrossSinkReplicasLoseNoSuccessfulUpdates(t *testing.T) {
	environment := newTestEnvironment(t)
	index := environment.createIndex(t, "product-concurrency")
	current, _ := fixture.ProductPair()
	current.Languages = []string{"base"}
	address := sinkAddress(t, index, current.UID)
	ctx, cancel := context.WithTimeout(context.Background(), productionTestTimeout)
	defer cancel()
	writePut(t, ctx, environment.client, address, current)

	const writers = 64
	operations := make([]sink.WriteOperation, writers)
	for index := range writers {
		incoming := &reference.Product{
			UID:       current.UID,
			Platform:  current.Platform,
			ID:        current.ID,
			URL:       current.URL,
			Languages: []string{fmt.Sprintf("writer-%02d", index)},
			Available: true,
		}
		operations[index] = newMergeOperation(t, address, incoming, programs.ProductMerge, sink.MissingDocumentFail)
	}
	start := make(chan struct{})
	errorsChannel := make(chan error, writers)
	var waitGroup sync.WaitGroup
	waitGroup.Add(writers)
	for index := range writers {
		go func() {
			defer waitGroup.Done()
			<-start
			client := environment.client
			if index%2 == 1 {
				client = environment.secondaryClient
			}
			requestContext, requestCancel := context.WithTimeout(ctx, 30*time.Second)
			results, err := client.Write(requestContext, sink.CompletionWaitUntilApplied, operations[index])
			requestCancel()
			if err != nil {
				errorsChannel <- err
				return
			}
			if len(results) != 1 || results[0].Status != sink.WriteApplied || results[0].Failure != nil {
				errorsChannel <- fmt.Errorf("writer %d result = %+v", index, results)
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

	actual := readProduct(t, ctx, environment.client, address)
	wanted := make(map[string]struct{}, writers+1)
	wanted["base"] = struct{}{}
	for index := range writers {
		wanted[fmt.Sprintf("writer-%02d", index)] = struct{}{}
	}
	if len(actual.Languages) != len(wanted) {
		t.Fatalf("languages = %d, want %d: %v", len(actual.Languages), len(wanted), actual.Languages)
	}
	for _, language := range actual.Languages {
		delete(wanted, language)
	}
	if len(wanted) != 0 {
		t.Fatalf("missing concurrent languages: %v", wanted)
	}
}
