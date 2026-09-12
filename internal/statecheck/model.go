// Package statecheck checks public Sink operations against a sequential model.
// The oracle uses ordinary Go values, never Sink's folding code or Lua engine.
package statecheck

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	sink "github.com/liran/sink-go"
)

// Options selects the real service, encoding and RPC boundaries under test.
type Options struct {
	Client    *sink.Client
	Store     string
	Dataset   string
	Encoding  sink.DocumentEncoding
	BatchSize int
	Seed      int64
	Steps     int
}

type document struct {
	Counter int64  `json:"counter" bson:"counter"`
	Label   string `json:"label" bson:"label"`
}

type step struct {
	kind  int
	key   int
	value int64
}

type expectation struct {
	status sink.WriteStatus
	code   sink.FailureCode
}

const addProgram = `return function(current, incoming)
  current = current or {}
  current.counter = (current.counter or 0) + incoming.counter
  current.label = current.label or ""
  return current
end`

const invalidProgram = `return function(current, incoming)
  current = current or {}
  current.counter = 999999
  error("injected failure after local mutation")
end`

// Run covers every pair of conditional/merge operations from both absent and
// present states, then interleaves reproducible writes, reads and deletes on
// three keys. Failures identify the seed, sequence, RPC and operation position.
func Run(t *testing.T, opts Options) {
	t.Helper()
	if opts.BatchSize < 1 || opts.Steps < 1 {
		t.Fatal("state model needs positive BatchSize and Steps")
	}
	for present := range 2 {
		for first := range 5 {
			for second := range 5 {
				name := fmt.Sprintf("pair-%d-%d-%d", present, first, second)
				var sequence []step
				if present == 1 {
					initial := step{kind: 1, value: 17}
					sequence = append(sequence, initial)
				}
				a := step{kind: first, value: 3}
				b := step{kind: second, value: -2}
				sequence = append(sequence, a, b)
				runSequence(t, opts, name, sequence)
			}
		}
	}
	random := rand.New(rand.NewSource(opts.Seed))
	sequence := make([]step, opts.Steps)
	for i := range sequence {
		operation := step{kind: random.Intn(7), key: random.Intn(3), value: int64(random.Intn(19) - 9)}
		sequence[i] = operation
	}
	runSequence(t, opts, fmt.Sprintf("seed-%d", opts.Seed), sequence)
}

func runSequence(t *testing.T, opts Options, name string, sequence []step) {
	t.Helper()
	ctx := t.Context()
	addresses := make([]sink.Address, 3)
	for key := range addresses {
		address, err := sink.NewAddress(opts.Store, "catalog", opts.Dataset, sink.StringKey(fmt.Sprintf("%s-key-%d", name, key)))
		if err != nil {
			t.Fatal(err)
		}
		addresses[key] = address
	}
	state := make(map[int]document)
	var operations []sink.WriteOperation
	var expected []expectation
	flush := func(position int) {
		if len(operations) == 0 {
			return
		}
		attempt, cancel := context.WithTimeout(ctx, 5*time.Second)
		results, err := opts.Client.Write(attempt, sink.CompletionWaitUntilApplied, operations...)
		cancel()
		if err != nil || len(results) != len(expected) {
			t.Fatalf("%s at step %d sequence=%+v: RPC=%v results=%+v", name, position, sequence[:position+1], err, results)
		}
		for i, want := range expected {
			result := results[i]
			if result.OperationIndex != i || result.Status != want.status {
				t.Fatalf("%s step=%d operation=%d sequence=%+v: result=%+v want=%+v", name, position, i, sequence[:position+1], result, want)
			}
			if want.status == sink.WriteApplied {
				if result.Failure != nil || len(result.Revision.Bytes()) == 0 {
					t.Fatalf("%s: successful write has failure or no revision: %+v", name, result)
				}
			} else if result.Failure == nil || result.Failure.Code != want.code || result.Failure.Retryable || len(result.Revision.Bytes()) != 0 {
				t.Fatalf("%s step=%d sequence=%+v: incorrect permanent failure: %+v, want %+v", name, position, sequence[:position+1], result, want)
			}
		}
		operations, expected = nil, nil
		check := readCheck{client: opts.Client, addresses: addresses, state: state, name: fmt.Sprintf("%s step=%d", name, position)}
		checkState(t, ctx, check)
	}
	for position, operation := range sequence {
		if operation.kind >= 5 {
			flush(position)
			if operation.kind == 5 {
				address := addresses[operation.key]
				attempt, cancel := context.WithTimeout(ctx, 5*time.Second)
				results, err := opts.Client.Delete(attempt, sink.CompletionWaitUntilApplied, address, address)
				cancel()
				if err != nil || len(results) != 2 {
					t.Fatalf("%s step=%d duplicate delete: %+v %v", name, position, results, err)
				}
				for i, result := range results {
					if result.OperationIndex != i || result.Status != sink.DeleteApplied || result.Failure != nil {
						t.Fatalf("%s step=%d delete result[%d]=%+v", name, position, i, result)
					}
				}
				delete(state, operation.key)
			}
			check := readCheck{client: opts.Client, addresses: addresses, state: state, name: fmt.Sprintf("%s step=%d", name, position)}
			checkState(t, ctx, check)
			continue
		}
		current, exists := state[operation.key]
		want := expectation{status: sink.WriteApplied}
		value := document{Counter: operation.value, Label: fmt.Sprintf("value-%d", operation.value)}
		switch operation.kind {
		case 0, 2:
			if (operation.kind == 0 && exists) || (operation.kind == 2 && !exists) {
				want.status, want.code = sink.WritePreconditionFailed, sink.FailurePreconditionFailed
			}
		case 4:
			want.status, want.code = sink.WriteFailed, sink.FailureInvalidArgument
		}
		if want.status == sink.WriteApplied {
			if operation.kind == 3 {
				current.Counter += operation.value
				value = current
			}
			state[operation.key] = value
		}
		build := operationOptions{address: addresses[operation.key], encoding: opts.Encoding, step: operation}
		operations = append(operations, makeOperation(t, build))
		expected = append(expected, want)
		if len(operations) >= opts.BatchSize || position == len(sequence)-1 {
			flush(position)
		}
	}
}

type operationOptions struct {
	address  sink.Address
	encoding sink.DocumentEncoding
	step     step
}

func makeOperation(t *testing.T, opts operationOptions) sink.WriteOperation {
	t.Helper()
	value := document{Counter: opts.step.value, Label: fmt.Sprintf("value-%d", opts.step.value)}
	payload, err := sink.NewDocument(value, opts.encoding)
	if err != nil {
		t.Fatal(err)
	}
	var operation sink.WriteOperation
	if opts.step.kind <= 2 {
		modes := []sink.WriteMode{sink.WriteCreate, sink.WriteUpsert, sink.WriteReplace}
		operation, err = sink.NewPut(opts.address, payload, modes[opts.step.kind])
	} else {
		source := addProgram
		if opts.step.kind == 4 {
			source = invalidProgram
		}
		program, compileErr := sink.NewLuaProgram([]byte(source))
		if compileErr != nil {
			t.Fatal(compileErr)
		}
		mergeOptions := sink.MergeOptions{Incoming: payload, Program: program}
		operation, err = sink.NewMerge(opts.address, mergeOptions)
	}
	if err != nil {
		t.Fatal(err)
	}
	return operation
}

type readCheck struct {
	client    *sink.Client
	addresses []sink.Address
	state     map[int]document
	name      string
}

func checkState(t *testing.T, ctx context.Context, check readCheck) {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	// Deliberately reorder and repeat keys to check result ownership as well
	// as document state after every RPC. Read retries are disabled by callers.
	keys := []int{2, 0, 1, 0}
	addresses := make([]sink.Address, len(keys))
	for i, key := range keys {
		addresses[i] = check.addresses[key]
	}
	results, err := check.client.Read(ctx, addresses...)
	if err != nil || len(results) != len(addresses) {
		t.Fatalf("%s read: %+v, %v", check.name, results, err)
	}
	for i, result := range results {
		want, exists := check.state[keys[i]]
		status := sink.ReadNotFound
		if exists {
			status = sink.ReadFound
		}
		if result.OperationIndex != i || result.Status != status || result.Failure != nil {
			t.Fatalf("%s read[%d]=%+v want status=%s", check.name, i, result, status)
		}
		if exists {
			var actual document
			if err := result.Document.Decode(&actual); err != nil {
				t.Fatal(err)
			}
			if actual != want || len(result.Revision.Bytes()) == 0 {
				t.Fatalf("%s key=%d: persisted=%+v expected=%+v revision=%x", check.name, keys[i], actual, want, result.Revision.Bytes())
			}
		}
	}
}
