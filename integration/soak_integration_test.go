//go:build integration

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sink "github.com/liran/sink-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	soakCycleTimeout             = 3 * time.Minute
	soakMutationAttemptTimeout   = 15 * time.Second
	soakAmbiguousObservationTime = 5 * time.Second
	soakPollInterval             = 100 * time.Millisecond
)

const soakIdempotentCounterMerge = `
return function(current, incoming)
    if current.operation_id ~= incoming.operation_id then
        current.counter = current.counter + incoming.delta
        current.operation_id = incoming.operation_id
    end
    current.updated_at = sink.v1.time.now()
    return current
end`

type soakCycleOptions struct {
	client       *sink.Client
	store        backendStoreSpec
	dataset      string
	key          string
	program      sink.LuaProgram
	asynchronous bool
	counters     *soakCounters
}

type soakWorkerOptions struct {
	environment *testEnvironment
	stores      []backendStoreSpec
	dataset     string
	program     sink.LuaProgram
	workerIndex int
	counters    *soakCounters
}

type soakCounters struct {
	completedCycles      atomic.Int64
	reconciledTransients atomic.Int64
	mutationRetries      atomic.Int64
}

type soakDocument struct {
	Value       string `json:"value"`
	Counter     int64  `json:"counter"`
	OperationID string `json:"operation_id"`
	UpdatedAt   string `json:"updated_at,omitempty"`
}

type soakDocumentExpectation struct {
	client          *sink.Client
	address         sink.Address
	wantFound       bool
	wantValue       string
	wantCounter     int64
	wantOperationID string
}

type soakObservedDocument struct {
	found    bool
	document soakDocument
}

type soakWriteReconciliationOptions struct {
	client        *sink.Client
	completion    sink.CompletionMode
	operation     sink.WriteOperation
	wantStatus    sink.WriteStatus
	previous      soakDocumentExpectation
	desired       soakDocumentExpectation
	counters      *soakCounters
	operationName string
}

type soakDeleteReconciliationOptions struct {
	client        *sink.Client
	completion    sink.CompletionMode
	address       sink.Address
	wantStatus    sink.DeleteStatus
	previous      soakDocumentExpectation
	desired       soakDocumentExpectation
	counters      *soakCounters
	operationName string
}

func TestStorageBackendSoak(t *testing.T) {
	if os.Getenv("SINK_RUN_SOAK") != "1" {
		t.Skip("SINK_RUN_SOAK is not 1")
	}
	stores := configuredBackendStores(t)
	if len(stores) == 0 {
		t.Fatal("SINK_BACKEND_STORES is required for the soak test")
	}
	duration := soakDuration(t)
	concurrency := positiveEnvironmentInt(t, "SINK_SOAK_CONCURRENCY", 16)
	minimumCycles := positiveEnvironmentInt(t, "SINK_SOAK_MIN_CYCLES", max(100, concurrency*5))
	environment := newTestEnvironment(t)
	program, err := sink.NewLuaProgram([]byte(soakIdempotentCounterMerge))
	if err != nil {
		t.Fatalf("sink.NewLuaProgram() error = %v", err)
	}

	dataset := "sink-soak-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	schedulingContext, stopScheduling := context.WithTimeout(context.Background(), duration)
	defer stopScheduling()
	operationContext, cancelOperations := context.WithCancel(context.Background())
	defer cancelOperations()
	errorsChannel := make(chan error, 1)
	counters := &soakCounters{}
	var waitGroup sync.WaitGroup
	waitGroup.Add(concurrency)
	started := time.Now()
	for workerIndex := range concurrency {
		workerOptions := soakWorkerOptions{
			environment: environment,
			stores:      stores,
			dataset:     dataset,
			program:     program,
			workerIndex: workerIndex,
			counters:    counters,
		}
		go func() {
			defer waitGroup.Done()
			workerErr := runSoakWorker(schedulingContext, operationContext, workerOptions)
			if workerErr != nil {
				select {
				case errorsChannel <- workerErr:
					stopScheduling()
					cancelOperations()
				default:
				}
			}
		}()
	}
	waitGroup.Wait()
	cancelOperations()
	close(errorsChannel)
	if soakErr := <-errorsChannel; soakErr != nil {
		t.Fatalf(
			"soak test failed after %s, %d cycles, %d reconciled transients, and %d mutation retries: %v",
			time.Since(started),
			counters.completedCycles.Load(),
			counters.reconciledTransients.Load(),
			counters.mutationRetries.Load(),
			soakErr,
		)
	}

	cycles := counters.completedCycles.Load()
	elapsed := time.Since(started)
	if cycles < int64(minimumCycles) {
		t.Fatalf("soak cycles = %d, want at least %d", cycles, minimumCycles)
	}
	operations := cycles * 6
	t.Logf(
		"storage soak: configured_duration=%s elapsed=%s concurrency=%d cycles=%d logical_operations=%d rate=%.1f operations/s reconciled_transients=%d mutation_retries=%d",
		duration,
		elapsed,
		concurrency,
		cycles,
		operations,
		float64(operations)/elapsed.Seconds(),
		counters.reconciledTransients.Load(),
		counters.mutationRetries.Load(),
	)
}

func runSoakWorker(
	schedulingContext context.Context,
	operationContext context.Context,
	opts soakWorkerOptions,
) error {
	for sequence := 0; ; sequence++ {
		if schedulingContext.Err() != nil {
			return nil
		}
		store := opts.stores[(opts.workerIndex+sequence)%len(opts.stores)]
		client := opts.environment.client
		if (opts.workerIndex+sequence)%2 == 1 {
			client = opts.environment.secondaryClient
		}
		cycleOptions := soakCycleOptions{
			client:       client,
			store:        store,
			dataset:      opts.dataset,
			key:          fmt.Sprintf("worker-%03d-cycle-%012d", opts.workerIndex, sequence),
			program:      opts.program,
			asynchronous: store.asynchronous && sequence%2 == 1,
			counters:     opts.counters,
		}
		cycleContext, cancel := context.WithTimeout(operationContext, soakCycleTimeout)
		err := runSoakCycle(cycleContext, cycleOptions)
		cancel()
		if err != nil {
			if operationContext.Err() != nil {
				return nil
			}
			return fmt.Errorf("store %s worker %d cycle %d: %w", store.name, opts.workerIndex, sequence, err)
		}
		opts.counters.completedCycles.Add(1)
	}
}

func runSoakCycle(ctx context.Context, opts soakCycleOptions) error {
	address, err := sink.NewAddress(opts.store.name, "catalog", opts.dataset, sink.StringKey(opts.key))
	if err != nil {
		return fmt.Errorf("create address: %w", err)
	}
	createOperationID := opts.key + ":create"
	mergeOperationID := opts.key + ":merge"
	document := soakDocument{
		Value:       opts.key,
		Counter:     0,
		OperationID: createOperationID,
	}
	put, err := sink.NewPut(address, document, sink.WriteUpsert)
	if err != nil {
		return fmt.Errorf("create put: %w", err)
	}
	completion := sink.CompletionWaitUntilVisible
	wantWriteStatus := sink.WriteApplied
	if opts.asynchronous {
		completion = sink.CompletionReturnAfterAccepted
		wantWriteStatus = sink.WriteAccepted
	}
	absentExpectation := soakDocumentExpectation{
		client:    opts.client,
		address:   address,
		wantFound: false,
	}
	createdExpectation := soakDocumentExpectation{
		client:          opts.client,
		address:         address,
		wantFound:       true,
		wantValue:       opts.key,
		wantCounter:     0,
		wantOperationID: createOperationID,
	}
	createOptions := soakWriteReconciliationOptions{
		client:        opts.client,
		completion:    completion,
		operation:     put,
		wantStatus:    wantWriteStatus,
		previous:      absentExpectation,
		desired:       createdExpectation,
		counters:      opts.counters,
		operationName: "create",
	}
	if err := writeSoakWithReconciliation(ctx, createOptions); err != nil {
		return fmt.Errorf("write create: %w", err)
	}

	incoming := map[string]any{
		"delta":        1,
		"operation_id": mergeOperationID,
	}
	mergeOptions := sink.MergeOptions{
		Incoming:            incoming,
		Program:             opts.program,
		MissingDocumentMode: sink.MissingDocumentFail,
	}
	merge, err := sink.NewMerge(address, mergeOptions)
	if err != nil {
		return fmt.Errorf("create merge: %w", err)
	}
	mergedExpectation := soakDocumentExpectation{
		client:          opts.client,
		address:         address,
		wantFound:       true,
		wantValue:       opts.key,
		wantCounter:     1,
		wantOperationID: mergeOperationID,
	}
	writeMergeOptions := soakWriteReconciliationOptions{
		client:        opts.client,
		completion:    completion,
		operation:     merge,
		wantStatus:    wantWriteStatus,
		previous:      createdExpectation,
		desired:       mergedExpectation,
		counters:      opts.counters,
		operationName: "merge",
	}
	if err := writeSoakWithReconciliation(ctx, writeMergeOptions); err != nil {
		return fmt.Errorf("write merge: %w", err)
	}

	wantDeleteStatus := sink.DeleteApplied
	if opts.asynchronous {
		wantDeleteStatus = sink.DeleteAccepted
	}
	deleteOptions := soakDeleteReconciliationOptions{
		client:        opts.client,
		completion:    completion,
		address:       address,
		wantStatus:    wantDeleteStatus,
		previous:      mergedExpectation,
		desired:       absentExpectation,
		counters:      opts.counters,
		operationName: "delete",
	}
	if err := deleteSoakWithReconciliation(ctx, deleteOptions); err != nil {
		return fmt.Errorf("delete: %w", err)
	}
	return nil
}

func writeSoakWithReconciliation(ctx context.Context, opts soakWriteReconciliationOptions) error {
	var lastErr error
	sawTransient := false
	for {
		attemptContext, cancel := context.WithTimeout(ctx, soakMutationAttemptTimeout)
		results, err := opts.client.Write(attemptContext, opts.completion, opts.operation)
		cancel()
		if err == nil {
			resultErr := validateSoakWriteResult(results, opts.wantStatus)
			if resultErr == nil {
				waitErr := waitForSoakDocumentState(ctx, opts.desired, opts.previous)
				if waitErr != nil {
					return fmt.Errorf("verify %s: %w", opts.operationName, waitErr)
				}
				if sawTransient {
					opts.counters.reconciledTransients.Add(1)
				}
				return nil
			}
			if !retryableSoakWriteResult(results) {
				return resultErr
			}
			lastErr = resultErr
		} else {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if !isRetryableSoakRPCError(err) {
				return err
			}
			lastErr = err
		}

		sawTransient = true
		resolved, reconcileErr := observeSoakMutation(ctx, opts.desired, opts.previous)
		if reconcileErr != nil {
			return fmt.Errorf("reconcile %s after %v: %w", opts.operationName, lastErr, reconcileErr)
		}
		if resolved {
			opts.counters.reconciledTransients.Add(1)
			return nil
		}
		opts.counters.mutationRetries.Add(1)
		if err := waitForSoakRetry(ctx); err != nil {
			return err
		}
	}
}

func deleteSoakWithReconciliation(ctx context.Context, opts soakDeleteReconciliationOptions) error {
	var lastErr error
	sawTransient := false
	for {
		attemptContext, cancel := context.WithTimeout(ctx, soakMutationAttemptTimeout)
		results, err := opts.client.Delete(attemptContext, opts.completion, opts.address)
		cancel()
		if err == nil {
			resultErr := validateSoakDeleteResult(results, opts.wantStatus)
			if resultErr == nil {
				waitErr := waitForSoakDocumentState(ctx, opts.desired, opts.previous)
				if waitErr != nil {
					return fmt.Errorf("verify %s: %w", opts.operationName, waitErr)
				}
				if sawTransient {
					opts.counters.reconciledTransients.Add(1)
				}
				return nil
			}
			if !retryableSoakDeleteResult(results) {
				return resultErr
			}
			lastErr = resultErr
		} else {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if !isRetryableSoakRPCError(err) {
				return err
			}
			lastErr = err
		}

		sawTransient = true
		resolved, reconcileErr := observeSoakMutation(ctx, opts.desired, opts.previous)
		if reconcileErr != nil {
			return fmt.Errorf("reconcile %s after %v: %w", opts.operationName, lastErr, reconcileErr)
		}
		if resolved {
			opts.counters.reconciledTransients.Add(1)
			return nil
		}
		opts.counters.mutationRetries.Add(1)
		if err := waitForSoakRetry(ctx); err != nil {
			return err
		}
	}
}

func validateSoakWriteResult(results []sink.WriteResult, want sink.WriteStatus) error {
	if len(results) != 1 || results[0].Status != want || results[0].Failure != nil {
		return fmt.Errorf("result = %+v, want %s", results, want)
	}
	return nil
}

func validateSoakDeleteResult(results []sink.DeleteResult, want sink.DeleteStatus) error {
	if len(results) != 1 || results[0].Status != want || results[0].Failure != nil {
		return fmt.Errorf("result = %+v, want %s", results, want)
	}
	return nil
}

func retryableSoakWriteResult(results []sink.WriteResult) bool {
	return len(results) == 1 && results[0].Failure != nil && results[0].Failure.Retryable
}

func retryableSoakDeleteResult(results []sink.DeleteResult) bool {
	return len(results) == 1 && results[0].Failure != nil && results[0].Failure.Retryable
}

func isRetryableSoakRPCError(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted:
		return true
	default:
		return false
	}
}

func isRetryableSoakReadError(err error) bool {
	var operationError *sink.OperationError
	if errors.As(err, &operationError) {
		return operationError.Retryable
	}
	return isRetryableSoakRPCError(err)
}

func observeSoakMutation(
	ctx context.Context,
	desired soakDocumentExpectation,
	previous soakDocumentExpectation,
) (bool, error) {
	ticker := time.NewTicker(soakPollInterval)
	defer ticker.Stop()
	var previousTimer *time.Timer
	var previousTimerChannel <-chan time.Time
	defer func() {
		if previousTimer != nil {
			previousTimer.Stop()
		}
	}()
	var lastReadErr error
	for {
		attemptContext, cancel := context.WithTimeout(ctx, soakAmbiguousObservationTime)
		observed, err := readSoakDocument(attemptContext, desired.client, desired.address)
		cancel()
		if err == nil {
			lastReadErr = nil
			if observed.matches(desired) {
				return true, nil
			}
			if observed.matches(previous) {
				if previousTimer == nil {
					previousTimer = time.NewTimer(soakAmbiguousObservationTime)
					previousTimerChannel = previousTimer.C
				}
			} else {
				return false, unexpectedSoakDocumentError(observed, desired, previous)
			}
		} else {
			if !isRetryableSoakReadError(err) {
				return false, fmt.Errorf("observe mutation state: %w", err)
			}
			lastReadErr = err
		}
		select {
		case <-ctx.Done():
			return false, fmt.Errorf("could not observe previous or desired state: %w", errors.Join(ctx.Err(), lastReadErr))
		case <-previousTimerChannel:
			return false, nil
		case <-ticker.C:
		}
	}
}

func waitForSoakDocumentState(
	ctx context.Context,
	desired soakDocumentExpectation,
	previous soakDocumentExpectation,
) error {
	ticker := time.NewTicker(soakPollInterval)
	defer ticker.Stop()
	var lastReadErr error
	for {
		observed, err := readSoakDocument(ctx, desired.client, desired.address)
		if err == nil {
			if observed.matches(desired) {
				return nil
			}
			if !observed.matches(previous) {
				return unexpectedSoakDocumentError(observed, desired, previous)
			}
		} else {
			lastReadErr = err
		}
		select {
		case <-ctx.Done():
			return errors.Join(ctx.Err(), lastReadErr)
		case <-ticker.C:
		}
	}
}

func readSoakDocument(ctx context.Context, client *sink.Client, address sink.Address) (soakObservedDocument, error) {
	observed := soakObservedDocument{}
	results, err := client.Read(ctx, address)
	if err != nil {
		return observed, err
	}
	if len(results) != 1 {
		return observed, fmt.Errorf("read returned %d results, want 1", len(results))
	}
	result := results[0]
	if result.Failure != nil {
		return observed, result.Failure
	}
	switch result.Status {
	case sink.ReadNotFound:
		return observed, nil
	case sink.ReadFound:
		document := soakDocument{}
		if err := result.Document.Decode(&document); err != nil {
			return observed, fmt.Errorf("decode soak document: %w", err)
		}
		observed.found = true
		observed.document = document
		return observed, nil
	default:
		return observed, fmt.Errorf("read status = %s, want FOUND or NOT_FOUND", result.Status)
	}
}

func (observed soakObservedDocument) matches(expectation soakDocumentExpectation) bool {
	if observed.found != expectation.wantFound {
		return false
	}
	if !observed.found {
		return true
	}
	return observed.document.Value == expectation.wantValue &&
		observed.document.Counter == expectation.wantCounter &&
		observed.document.OperationID == expectation.wantOperationID
}

func unexpectedSoakDocumentError(
	observed soakObservedDocument,
	desired soakDocumentExpectation,
	previous soakDocumentExpectation,
) error {
	return fmt.Errorf(
		"observed document = %+v found=%t, want desired=%s or previous=%s",
		observed.document,
		observed.found,
		describeSoakExpectation(desired),
		describeSoakExpectation(previous),
	)
}

func describeSoakExpectation(expectation soakDocumentExpectation) string {
	if !expectation.wantFound {
		return "not found"
	}
	return fmt.Sprintf(
		"{value:%q counter:%d operation_id:%q}",
		expectation.wantValue,
		expectation.wantCounter,
		expectation.wantOperationID,
	)
}

func waitForSoakRetry(ctx context.Context) error {
	timer := time.NewTimer(soakPollInterval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func soakDuration(t *testing.T) time.Duration {
	t.Helper()
	raw := os.Getenv("SINK_SOAK_DURATION")
	if raw == "" {
		return time.Hour
	}
	duration, err := time.ParseDuration(raw)
	if err != nil || duration < time.Minute {
		t.Fatalf("SINK_SOAK_DURATION must be a duration of at least one minute")
	}
	return duration
}
