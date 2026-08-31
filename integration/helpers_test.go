//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	sink "github.com/liran/sink-go"
	"github.com/liran/sink-production-suite/internal/reference"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultSinkAddress     = "127.0.0.1:18080"
	defaultSecondAddress   = "127.0.0.1:18081"
	defaultSearchEndpoint  = "http://127.0.0.1:19200"
	productionTestTimeout  = 2 * time.Minute
	dependencyWaitInterval = 200 * time.Millisecond
)

type testEnvironment struct {
	client          *sink.Client
	secondaryClient *sink.Client
	searchEndpoint  string
	httpClient      *http.Client
}

func newTestEnvironment(t *testing.T) *testEnvironment {
	t.Helper()
	client := dialClient(t, environmentValue("SINK_ADDRESS", defaultSinkAddress))
	secondary := dialClient(t, environmentValue("SINK_SECONDARY_ADDRESS", defaultSecondAddress))
	environment := &testEnvironment{
		client:          client,
		secondaryClient: secondary,
		searchEndpoint:  strings.TrimRight(environmentValue("SINK_SEARCH_ENDPOINT", defaultSearchEndpoint), "/"),
		httpClient:      &http.Client{Timeout: 30 * time.Second},
	}
	ctx, cancel := context.WithTimeout(context.Background(), productionTestTimeout)
	defer cancel()
	waitForHealth(t, ctx, environment.client)
	waitForHealth(t, ctx, environment.secondaryClient)
	return environment
}

func dialClient(t *testing.T, address string) *sink.Client {
	t.Helper()
	dialOptions := sink.DialOptions{TransportCredentials: insecure.NewCredentials()}
	client, err := sink.Dial(address, dialOptions)
	if err != nil {
		t.Fatalf("sink.Dial(%q) error = %v", address, err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("Close(%q) error = %v", address, err)
		}
	})
	return client
}

func waitForHealth(t *testing.T, ctx context.Context, client *sink.Client) {
	t.Helper()
	ticker := time.NewTicker(dependencyWaitInterval)
	defer ticker.Stop()
	var lastErr error
	for {
		attemptContext, cancel := context.WithTimeout(ctx, time.Second)
		err := client.CheckHealth(attemptContext)
		cancel()
		if err == nil {
			return
		}
		lastErr = err
		select {
		case <-ctx.Done():
			t.Fatalf("wait for Sink health: %v", errors.Join(ctx.Err(), lastErr))
		case <-ticker.C:
		}
	}
}

func (e *testEnvironment) createIndex(t *testing.T, prefix string) string {
	t.Helper()
	index := fmt.Sprintf("sink-qualification-%s-%d", prefix, time.Now().UnixNano())
	e.ensureIndex(t, index)
	t.Cleanup(func() {
		status, _ := e.searchRequest(t, http.MethodDelete, "/"+index, nil)
		if status != http.StatusOK && status != http.StatusNotFound {
			t.Errorf("delete index %q returned HTTP %d", index, status)
		}
	})
	return index
}

func (e *testEnvironment) ensureIndex(t *testing.T, index string) {
	t.Helper()
	settings := []byte(`{"settings":{"number_of_shards":1,"number_of_replicas":0,"refresh_interval":"100ms"}}`)
	status, response := e.searchRequest(t, http.MethodPut, "/"+index, settings)
	if status == http.StatusBadRequest && bytes.Contains(response, []byte("resource_already_exists_exception")) {
		return
	}
	if status < 200 || status >= 300 {
		t.Fatalf("create index %q returned HTTP %d: %s", index, status, response)
	}
}

func (e *testEnvironment) deleteIndex(t *testing.T, index string) {
	t.Helper()
	status, response := e.searchRequest(t, http.MethodDelete, "/"+index, nil)
	if status != http.StatusOK && status != http.StatusNotFound {
		t.Fatalf("delete index %q returned HTTP %d: %s", index, status, response)
	}
}

func (e *testEnvironment) searchRequest(t *testing.T, method string, path string, body []byte) (int, []byte) {
	t.Helper()
	request, err := http.NewRequestWithContext(context.Background(), method, e.searchEndpoint+path, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("http.NewRequestWithContext() error = %v", err)
	}
	if len(body) > 0 {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := e.httpClient.Do(request)
	if err != nil {
		t.Fatalf("search request error = %v", err)
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("io.ReadAll(search response) error = %v", err)
	}
	return response.StatusCode, responseBody
}

func sinkAddress(t *testing.T, index string, key string) sink.Address {
	t.Helper()
	return sinkAddressForStore(t, "primary", index, key)
}

func sinkAddressForStore(t *testing.T, store string, index string, key string) sink.Address {
	t.Helper()
	address, err := sink.NewAddress(store, "catalog", index, sink.StringKey(key))
	if err != nil {
		t.Fatalf("sink.NewAddress() error = %v", err)
	}
	return address
}

func writePut(t *testing.T, ctx context.Context, client *sink.Client, address sink.Address, value any) {
	t.Helper()
	document := documentForAddress(t, address, value)
	operation, err := sink.NewPut(address, document, sink.WriteUpsert)
	if err != nil {
		t.Fatalf("sink.NewPut() error = %v", err)
	}
	results, err := client.Write(ctx, sink.CompletionWaitUntilVisible, operation)
	if err != nil {
		t.Fatalf("Write(put) error = %v", err)
	}
	assertWriteResults(t, results, sink.WriteApplied)
}

func newMergeOperation(t *testing.T, address sink.Address, incoming any, source []byte, missing sink.MissingDocumentMode) sink.WriteOperation {
	t.Helper()
	program, err := sink.NewLuaProgram(source)
	if err != nil {
		t.Fatalf("sink.NewLuaProgram() error = %v", err)
	}
	options := sink.MergeOptions{
		Incoming:            documentForAddress(t, address, incoming),
		Program:             program,
		MissingDocumentMode: missing,
	}
	operation, err := sink.NewMerge(address, options)
	if err != nil {
		t.Fatalf("sink.NewMerge() error = %v", err)
	}
	return operation
}

func documentForAddress(t *testing.T, address sink.Address, value any) sink.Document {
	t.Helper()
	document, err := newDocumentForAddress(address, value)
	if err != nil {
		t.Fatalf("sink.NewDocument() error = %v", err)
	}
	return document
}

func newDocumentForAddress(address sink.Address, value any) (sink.Document, error) {
	encoding := sink.DocumentEncodingJSON
	if strings.HasPrefix(address.Store(), "mongodb-") {
		encoding = sink.DocumentEncodingBSON
	}
	return sink.NewDocument(value, encoding)
}

func assertWriteResults(t *testing.T, results []sink.WriteResult, status sink.WriteStatus) {
	t.Helper()
	if len(results) == 0 {
		t.Fatal("Write() returned no results")
	}
	for index, result := range results {
		if result.Status != status || result.Failure != nil {
			t.Fatalf("Write() result[%d] = %+v, want %s", index, result, status)
		}
	}
}

func readProduct(t *testing.T, ctx context.Context, client *sink.Client, address sink.Address) *reference.Product {
	t.Helper()
	results, err := client.Read(ctx, address)
	if err != nil {
		t.Fatalf("Read(product) error = %v", err)
	}
	if len(results) != 1 || results[0].Status != sink.ReadFound {
		t.Fatalf("Read(product) results = %+v", results)
	}
	product := &reference.Product{}
	if err := results[0].Document.Decode(product); err != nil {
		t.Fatalf("Decode(product) error = %v", err)
	}
	return product
}

func readOffer(t *testing.T, ctx context.Context, client *sink.Client, address sink.Address) *reference.Offer {
	t.Helper()
	results, err := client.Read(ctx, address)
	if err != nil {
		t.Fatalf("Read(offer) error = %v", err)
	}
	if len(results) != 1 || results[0].Status != sink.ReadFound {
		t.Fatalf("Read(offer) results = %+v", results)
	}
	offer := &reference.Offer{}
	if err := results[0].Document.Decode(offer); err != nil {
		t.Fatalf("Decode(offer) error = %v", err)
	}
	return offer
}

func assertEquivalent(t *testing.T, expected any, actual any) {
	t.Helper()
	if reflect.DeepEqual(expected, actual) {
		return
	}
	expectedJSON, _ := json.MarshalIndent(expected, "", "  ")
	actualJSON, _ := json.MarshalIndent(actual, "", "  ")
	t.Fatalf("documents differ\nexpected:\n%s\nactual:\n%s", expectedJSON, actualJSON)
}

func environmentValue(name string, fallback string) string {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	return value
}
