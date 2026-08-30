//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	sink "github.com/liran/sink-go"
	"github.com/liran/sink-production-suite/internal/fixture"
	"github.com/liran/sink-production-suite/programs"
)

type loadTask struct {
	address   sink.Address
	operation sink.WriteOperation
}

type loadTaskOptions struct {
	dataset string
	index   int
	program sink.LuaProgram
}

type loadResult struct {
	duration time.Duration
	err      error
}

func TestRepresentativeProductMergeLoad(t *testing.T) {
	if os.Getenv("SINK_RUN_LOAD") != "1" {
		t.Skip("SINK_RUN_LOAD is not 1")
	}
	operationCount := positiveEnvironmentInt(t, "SINK_LOAD_OPERATIONS", 1000)
	concurrency := positiveEnvironmentInt(t, "SINK_LOAD_CONCURRENCY", 32)
	clientCount := positiveEnvironmentInt(t, "SINK_LOAD_CLIENTS", 2)
	minimumThroughput := positiveEnvironmentFloat(t, "SINK_LOAD_MIN_OPS_PER_SECOND", 100)
	maximumP95 := time.Duration(positiveEnvironmentInt(t, "SINK_LOAD_MAX_P95_MILLISECONDS", 2000)) * time.Millisecond
	loadTimeout := positiveEnvironmentDuration(t, "SINK_LOAD_TIMEOUT", 5*time.Minute)

	environment := newTestEnvironment(t)
	clients := loadClients(t, environment, min(clientCount, concurrency))
	index := environment.createIndex(t, "product-load")
	program, err := sink.NewLuaProgram(programs.ProductMerge)
	if err != nil {
		t.Fatalf("sink.NewLuaProgram() error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), loadTimeout)
	defer cancel()
	jobs := make(chan int)
	results := make(chan loadResult, operationCount)
	var waitGroup sync.WaitGroup
	workerCount := min(concurrency, operationCount)
	waitGroup.Add(workerCount)
	started := time.Now()
	for workerIndex := range workerCount {
		go func() {
			defer waitGroup.Done()
			client := clients[workerIndex%len(clients)]
			for taskIndex := range jobs {
				if ctx.Err() != nil {
					return
				}
				taskOptions := loadTaskOptions{dataset: index, index: taskIndex, program: program}
				task, taskErr := newLoadTask(taskOptions)
				if taskErr != nil {
					result := loadResult{err: taskErr}
					results <- result
					continue
				}
				requestStarted := time.Now()
				writeResults, writeErr := client.Write(ctx, sink.CompletionWaitUntilApplied, task.operation)
				duration := time.Since(requestStarted)
				if writeErr == nil && (len(writeResults) != 1 || writeResults[0].Status != sink.WriteApplied || writeResults[0].Failure != nil) {
					writeErr = fmt.Errorf("operation %d result = %+v", taskIndex, writeResults)
				}
				result := loadResult{duration: duration, err: writeErr}
				results <- result
			}
		}()
	}
	go func() {
		defer close(jobs)
		for indexValue := range operationCount {
			select {
			case jobs <- indexValue:
			case <-ctx.Done():
				return
			}
		}
	}()
	waitGroup.Wait()
	close(results)
	elapsed := time.Since(started)

	durations := make([]time.Duration, 0, operationCount)
	failed := 0
	firstErrors := make([]error, 0, 10)
	for result := range results {
		durations = append(durations, result.duration)
		if result.err != nil {
			failed++
			if len(firstErrors) < cap(firstErrors) {
				firstErrors = append(firstErrors, result.err)
			}
		}
	}
	if ctx.Err() != nil {
		t.Fatalf(
			"load timed out after %s: completed=%d requested=%d failed=%d first_errors=%v",
			loadTimeout,
			len(durations),
			operationCount,
			failed,
			firstErrors,
		)
	}
	if failed > 0 {
		t.Fatalf("load operations failed: completed=%d failed=%d first_errors=%v", len(durations), failed, firstErrors)
	}
	if len(durations) != operationCount {
		t.Fatalf("load completed %d operations, want %d", len(durations), operationCount)
	}
	sort.Slice(durations, func(left int, right int) bool {
		return durations[left] < durations[right]
	})
	p95Index := max(0, (len(durations)*95+99)/100-1)
	p95 := durations[p95Index]
	throughput := float64(operationCount) / elapsed.Seconds()
	t.Logf(
		"product merge load: operations=%d concurrency=%d clients=%d elapsed=%s throughput=%.1f ops/s p95=%s",
		operationCount,
		workerCount,
		len(clients),
		elapsed,
		throughput,
		p95,
	)
	if throughput < minimumThroughput {
		t.Errorf("throughput = %.1f ops/s, minimum = %.1f ops/s", throughput, minimumThroughput)
	}
	if p95 > maximumP95 {
		t.Errorf("p95 = %s, maximum = %s", p95, maximumP95)
	}

	firstOptions := loadTaskOptions{dataset: index, index: 0, program: program}
	firstTask, err := newLoadTask(firstOptions)
	if err != nil {
		t.Fatalf("newLoadTask(first) error = %v", err)
	}
	lastOptions := loadTaskOptions{dataset: index, index: operationCount - 1, program: program}
	lastTask, err := newLoadTask(lastOptions)
	if err != nil {
		t.Fatalf("newLoadTask(last) error = %v", err)
	}
	first := readProduct(t, ctx, environment.client, firstTask.address)
	last := readProduct(t, ctx, environment.secondaryClient, lastTask.address)
	if first.UID == "" || last.UID == "" {
		t.Fatal("load verification returned an empty product UID")
	}
}

func loadClients(t *testing.T, environment *testEnvironment, count int) []*sink.Client {
	t.Helper()
	clients := make([]*sink.Client, 0, count)
	clients = append(clients, environment.client)
	if count > 1 {
		clients = append(clients, environment.secondaryClient)
	}
	primaryAddress := environmentValue("SINK_ADDRESS", defaultSinkAddress)
	secondaryAddress := environmentValue("SINK_SECONDARY_ADDRESS", defaultSecondAddress)
	for len(clients) < count {
		address := primaryAddress
		if len(clients)%2 == 1 {
			address = secondaryAddress
		}
		clients = append(clients, dialClient(t, address))
	}
	return clients
}

func newLoadTask(opts loadTaskOptions) (loadTask, error) {
	var task loadTask
	product := fixture.RepresentativeProduct(opts.index)
	address, err := sink.NewAddress("primary", "catalog", opts.dataset, sink.StringKey(product.UID))
	if err != nil {
		return task, fmt.Errorf("create load address: %w", err)
	}
	mergeOptions := sink.MergeOptions{
		Incoming:            product,
		Program:             opts.program,
		MissingDocumentMode: sink.MissingDocumentCreate,
	}
	operation, err := sink.NewMerge(address, mergeOptions)
	if err != nil {
		return task, fmt.Errorf("create load merge: %w", err)
	}
	task = loadTask{address: address, operation: operation}
	return task, nil
}

func positiveEnvironmentInt(t *testing.T, name string, fallback int) int {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		t.Fatalf("%s must be a positive integer", name)
	}
	return parsed
}

func positiveEnvironmentFloat(t *testing.T, name string, fallback float64) float64 {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil || parsed <= 0 {
		t.Fatalf("%s must be a positive number", name)
	}
	return parsed
}

func positiveEnvironmentDuration(t *testing.T, name string, fallback time.Duration) time.Duration {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		t.Fatalf("%s must be a positive duration", name)
	}
	return parsed
}
