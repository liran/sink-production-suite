//go:build integration

package integration_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"os"
	"strings"
	"testing"

	sink "github.com/liran/sink-go"
	"google.golang.org/grpc/credentials/insecure"
)

func TestReliabilityRejectsOversizedAsyncMutation(t *testing.T) {
	environment := newTestEnvironment(t)
	index := environment.createIndex(t, "oversized-async")
	ctx, cancel := context.WithTimeout(t.Context(), productionTestTimeout)
	defer cancel()
	address := sinkAddress(t, index, "same-record")
	value := map[string]any{"value": strings.Repeat("x", 901<<10)}
	document := documentForAddress(t, address, value)
	operation, err := sink.NewPut(address, document, sink.WriteUpsert)
	if err != nil {
		t.Fatal(err)
	}
	results, err := environment.client.Write(ctx, sink.CompletionReturnAfterAccepted, operation)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Status != sink.WriteFailed || results[0].Failure == nil {
		t.Fatalf("oversized mutation results = %+v", results)
	}
	failure := results[0].Failure
	if failure.Code != sink.FailureInvalidArgument || failure.Retryable {
		t.Fatalf("oversized mutation failure = %+v, want permanent invalid argument", failure)
	}
	assertDocumentNotFound(t, ctx, environment.client, address)
	valid := map[string]any{"counter": 1}
	validOperation, err := sink.NewPut(address, documentForAddress(t, address, valid), sink.WriteUpsert)
	if err != nil {
		t.Fatal(err)
	}
	results, err = environment.client.Write(ctx, sink.CompletionReturnAfterAccepted, validOperation)
	if err != nil {
		t.Fatal(err)
	}
	assertWriteResults(t, results, sink.WriteAccepted)
	waitForDocumentFound(t, ctx, environment.client, address)
}

func TestReliabilityReadBudgetCountsRepeatedKeysAcrossStores(t *testing.T) {
	environment := newTestEnvironment(t)
	// Observe the first server response before the SDK retries failed entries.
	retry := sink.RetryPolicy{MaxAttempts: 1}
	clientOptions := sink.ClientOptions{ReadRetry: retry}
	dialOptions := sink.DialOptions{Client: clientOptions, TransportCredentials: insecure.NewCredentials()}
	firstAttempt, err := sink.Dial(environmentValue("SINK_ADDRESS", defaultSinkAddress), dialOptions)
	if err != nil {
		t.Fatal(err)
	}
	defer firstAttempt.Close()
	index := environment.createIndex(t, "read-budget")
	ctx, cancel := context.WithTimeout(t.Context(), productionTestTimeout)
	defer cancel()
	primary := sinkAddress(t, index, "large-record")
	secondary := sinkAddressForStore(t, "secondary", index, "large-record")
	value := map[string]any{"value": strings.Repeat("x", 768<<10)}
	writePut(t, ctx, environment.client, primary, value)
	addresses := make([]sink.Address, 64)
	for i := range addresses {
		addresses[i] = primary
		if i%2 != 0 {
			addresses[i] = secondary
		}
	}
	results, err := firstAttempt.Read(ctx, addresses...)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != len(addresses) {
		t.Fatalf("read result count = %d, want %d", len(results), len(addresses))
	}
	found, exhausted := 0, 0
	for i, result := range results {
		if result.OperationIndex != i {
			t.Fatalf("result[%d] has operation index %d", i, result.OperationIndex)
		}
		switch result.Status {
		case sink.ReadFound:
			found++
		case sink.ReadFailed:
			if result.Failure == nil || result.Failure.Code != sink.FailureResourceExhausted || !result.Failure.Retryable {
				t.Fatalf("read failure = %+v, want retryable resource exhaustion", result.Failure)
			}
			exhausted++
		default:
			t.Fatalf("unexpected read result = %+v", result)
		}
	}
	if found == 0 || exhausted == 0 {
		t.Fatalf("read budget: found=%d exhausted=%d; expected bounded partial progress", found, exhausted)
	}
	t.Logf("first response: %d found, %d retryable budget rejections", found, exhausted)
	// The normal SDK retries only unresolved entries, eventually reading all.
	results, err = environment.client.Read(ctx, addresses...)
	if err != nil || len(results) != len(addresses) {
		t.Fatalf("read with SDK retries: %d results, error=%v", len(results), err)
	}
	for i, result := range results {
		if result.Status != sink.ReadFound || result.OperationIndex != i {
			t.Fatalf("read with SDK retries result[%d] = %+v", i, result)
		}
	}
}

func TestReliabilityLuaAliasExpansionIsRejectedWithoutWriting(t *testing.T) {
	environment := newTestEnvironment(t)
	index := environment.createIndex(t, "lua-budget")
	ctx, cancel := context.WithTimeout(t.Context(), productionTestTimeout)
	defer cancel()
	address := sinkAddress(t, index, "alias-expansion")
	initial := map[string]any{"counter": 7}
	writePut(t, ctx, environment.client, address, initial)
	// A small VM graph would expand to gigabytes if aliases were copied first.
	source := []byte(`return function(current, incoming)
    local value = {value = incoming.value}
    for i = 1, 20 do value = {left = value, right = value} end
    return value
end`)
	incoming := map[string]any{"value": strings.Repeat("x", 4096)}
	operation := newMergeOperation(t, address, incoming, source)
	results, err := environment.client.Write(ctx, sink.CompletionWaitUntilApplied, operation)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Failure == nil || results[0].Status != sink.WriteFailed {
		t.Fatalf("expanded Lua results = %+v", results)
	}
	if failure := results[0].Failure; failure.Code != sink.FailureResourceExhausted || failure.Retryable {
		t.Fatalf("expanded Lua failure = %+v, want permanent resource exhaustion", failure)
	}
	observed, err := readSoakDocument(ctx, environment.client, address)
	if err != nil || !observed.found || observed.document.Counter != 7 {
		t.Fatalf("stored record changed after rejected Lua: %+v, error=%v", observed, err)
	}
}

func TestReliabilityDeadLetterRecovery(t *testing.T) {
	phase := os.Getenv("SINK_DLQ_PHASE")
	if phase == "" {
		t.Skip("SINK_DLQ_PHASE is not set")
	}
	index := os.Getenv("SINK_DLQ_INDEX")
	if index == "" {
		t.Fatal("SINK_DLQ_INDEX is required")
	}
	environment := newTestEnvironment(t)
	ctx, cancel := context.WithTimeout(t.Context(), productionTestTimeout)
	defer cancel()
	address := sinkAddress(t, index, "recovery")
	desired := soakDocumentExpectation{client: environment.client, address: address, wantFound: true}
	previous := desired
	switch phase {
	case "publish":
		environment.ensureIndex(t, index)
		initial := map[string]any{"counter": 0}
		writePut(t, ctx, environment.client, address, initial)
		createValue := map[string]any{"counter": 1}
		create, err := sink.NewPut(address, documentForAddress(t, address, createValue), sink.WriteCreate)
		if err != nil {
			t.Fatal(err)
		}
		updateValue := map[string]any{"counter": 2}
		update, err := sink.NewPut(address, documentForAddress(t, address, updateValue), sink.WriteUpsert)
		if err != nil {
			t.Fatal(err)
		}
		results, err := environment.client.Write(ctx, sink.CompletionReturnAfterAccepted, create, update)
		if err != nil {
			t.Fatal(err)
		}
		assertWriteResults(t, results, sink.WriteAccepted)
		desired.wantCounter = 2
	case "repair":
		results, err := environment.client.Delete(ctx, sink.CompletionWaitUntilVisible, address)
		if err != nil || len(results) != 1 || results[0].Status != sink.DeleteApplied {
			t.Fatalf("repair conflict: results=%+v error=%v", results, err)
		}
		assertDocumentNotFound(t, ctx, environment.client, address)
		return
	case "verify":
		verifyDeadLetterReports(t)
		desired.wantCounter = 1
		previous.wantFound = false
		defer environment.deleteIndex(t, index)
	default:
		t.Fatalf("unknown DLQ phase %q", phase)
	}
	if err := waitForSoakDocumentState(ctx, desired, previous); err != nil {
		t.Fatal(err)
	}
}

type deadLetterReport struct {
	Topic        string            `json:"topic"`
	Partition    int32             `json:"partition"`
	Offset       int64             `json:"offset"`
	SHA256       string            `json:"sha256"`
	Headers      map[string]string `json:"headers"`
	ReplayStatus string            `json:"replay_status"`
}

func verifyDeadLetterReports(t *testing.T) {
	t.Helper()
	var inspected deadLetterReport
	for _, name := range []string{"SINK_DLQ_INSPECT_REPORT", "SINK_DLQ_REPLAY_REPORT"} {
		file, err := os.Open(os.Getenv(name))
		if err != nil {
			t.Fatal(err)
		}
		decoder := json.NewDecoder(file)
		decoder.DisallowUnknownFields()
		var report deadLetterReport
		err = decoder.Decode(&report)
		file.Close()
		if err != nil {
			t.Fatal(err)
		}
		digest, err := hex.DecodeString(report.SHA256)
		if err != nil || len(digest) != 32 || report.Topic != "sink-production-mutations.dlq" || report.Offset != 0 || report.Partition < 0 {
			t.Fatalf("invalid DLQ report: %+v", report)
		}
		if report.Headers["sink-source-topic"] != "sink-production-mutations" || report.Headers["sink-error"] == "" {
			t.Fatalf("DLQ source/error evidence is missing: %+v", report)
		}
		if name == "SINK_DLQ_INSPECT_REPORT" {
			if report.ReplayStatus != "" {
				t.Fatalf("inspection unexpectedly reports replay: %+v", report)
			}
			inspected = report
		} else if report.ReplayStatus != "accepted" || report.SHA256 != inspected.SHA256 || report.Partition != inspected.Partition {
			t.Fatalf("replay does not match inspected record: %+v", report)
		}
	}
}
