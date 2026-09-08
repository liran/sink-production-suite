//go:build integration

package conformance_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sink "github.com/liran/sink-go"
	"github.com/liran/sink-production-suite/internal/historycheck"
)

func TestConcurrentHistories(t *testing.T) {
	seed := int64(37)
	if raw := os.Getenv("SINK_STATE_SEED"); raw != "" {
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			t.Fatalf("SINK_STATE_SEED: %v", err)
		}
		seed = value
	}
	rounds := 24
	if raw := os.Getenv("SINK_HISTORY_ROUNDS"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 6 || value > 1000 {
			t.Fatal("SINK_HISTORY_ROUNDS must be between 6 and 1000")
		}
		rounds = value
	}
	profiles := []struct {
		name      string
		unbatched bool
		batchOps  int
	}{
		{name: "batched", batchOps: 1000},
		{name: "direct", unbatched: true},
		{name: "one-op-batches", batchOps: 1},
	}
	for _, store := range searchBackends(t) {
		for _, profile := range profiles {
			t.Run(store.driver+"/"+profile.name, func(t *testing.T) {
				index := indexFor(t, store, "-1")
				opts := serverOptions{backend: store, unbatched: profile.unbatched, batchOps: profile.batchOps, batchWait: 10}
				first, second := startCandidate(t, opts), startCandidate(t, opts)
				clients := []*sink.Client{first.client, second.client, first.client}
				random := rand.New(rand.NewSource(seed))
				var clock atomic.Int64
				t.Logf("concurrent history seed=%d rounds=%d clients=3 replicas=2", seed, rounds)
				for round := range rounds {
					address := addressFor(t, index, fmt.Sprintf("history-%d", round))
					history := make([]historycheck.Entry, 10)
					kinds := []historycheck.Kind{historycheck.Read, historycheck.Set, historycheck.Create,
						historycheck.Replace, historycheck.Add, historycheck.Delete}
					for i := range 9 {
						kind := kinds[random.Intn(len(kinds))]
						if round < 2 {
							kind = historycheck.Add
						} else if i == 0 {
							kind = kinds[round%len(kinds)]
						}
						entry := historycheck.Entry{ID: i, Client: i / 3, Kind: kind, Input: random.Intn(5) + 1}
						history[i] = entry
					}
					start := make(chan struct{})
					errors := make(chan error, len(clients))
					var workers sync.WaitGroup
					for clientID, client := range clients {
						workers.Go(func() {
							<-start
							for step := range 3 {
								i := clientID*3 + step
								entry := history[i]
								entry.Start = clock.Add(1)
								result, err := runHistoryCall(t.Context(), client, address, entry)
								entry.End, entry.Result = clock.Add(1), result
								history[i] = entry
								if err != nil {
									errors <- fmt.Errorf("entry %d: %w", i, err)
									return
								}
							}
						})
					}
					close(start)
					workers.Wait()
					close(errors)
					for err := range errors {
						t.Errorf("seed=%d round=%d: %v", seed, round, err)
					}
					final := historycheck.Entry{ID: 9, Client: 0, Start: clock.Add(1), Kind: historycheck.Read}
					result, err := runHistoryCall(t.Context(), first.client, address, final)
					final.End, final.Result = clock.Add(1), result
					history[9] = final
					if err != nil {
						t.Error(err)
					}
					initial := historycheck.State{}
					if err := historycheck.Check(initial, history); err != nil {
						t.Errorf("seed=%d round=%d: %v", seed, round, err)
					}
					if !historyOverlaps(history) {
						t.Error("concurrent history never exercised overlapping calls")
					}
					if t.Failed() {
						preserveHistory(t, history)
						t.FailNow()
					}
				}
			})
		}
	}
}

func runHistoryCall(ctx context.Context, client *sink.Client, address sink.Address, entry historycheck.Entry) (historycheck.Result, error) {
	result := historycheck.Result{}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if entry.Kind == historycheck.Read {
		results, err := client.Read(ctx, address)
		if err != nil || len(results) != 1 {
			return result, fmt.Errorf("read result count=%d error=%v", len(results), err)
		}
		read := results[0]
		if read.OperationIndex != 0 || read.Failure != nil {
			return result, fmt.Errorf("read result: %+v", read)
		}
		switch read.Status {
		case sink.ReadNotFound:
			return result, nil
		case sink.ReadFound:
			var document map[string]int
			if err := read.Document.Decode(&document); err != nil {
				return result, err
			}
			counter, exists := document["counter"]
			if !exists || len(document) != 1 || len(read.Revision.Bytes()) == 0 {
				return result, fmt.Errorf("unexpected persisted document/revision: %+v", read)
			}
			result.State.Found, result.State.Counter = true, counter
			return result, nil
		default:
			return result, fmt.Errorf("unexpected read status: %+v", read)
		}
	}
	if entry.Kind == historycheck.Delete {
		results, err := client.Delete(ctx, sink.CompletionWaitUntilApplied, address)
		if err != nil || len(results) != 1 || results[0].OperationIndex != 0 || results[0].Status != sink.DeleteApplied || results[0].Failure != nil {
			return result, fmt.Errorf("delete: %+v, %v", results, err)
		}
		result.Applied = true
		return result, nil
	}
	document, err := sink.NewRawDocument(sink.DocumentEncodingJSON, []byte(fmt.Sprintf(`{"counter":%d}`, entry.Input)))
	if err != nil {
		return result, err
	}
	var operation sink.WriteOperation
	if entry.Kind == historycheck.Add {
		program, programErr := sink.NewLuaProgram([]byte(increment))
		if programErr != nil {
			return result, programErr
		}
		opts := sink.MergeOptions{Incoming: document, Program: program, MissingDocumentMode: sink.MissingDocumentCreate}
		operation, err = sink.NewMerge(address, opts)
	} else {
		mode := sink.WriteUpsert
		if entry.Kind == historycheck.Create {
			mode = sink.WriteCreate
		} else if entry.Kind == historycheck.Replace {
			mode = sink.WriteReplace
		}
		operation, err = sink.NewPut(address, document, mode)
	}
	if err != nil {
		return result, err
	}
	results, err := client.Write(ctx, sink.CompletionWaitUntilApplied, operation)
	if err != nil || len(results) != 1 || results[0].OperationIndex != 0 {
		return result, fmt.Errorf("write: %+v, %v", results, err)
	}
	write := results[0]
	if write.Status == sink.WriteFailed && write.Failure != nil && write.Failure.Code == sink.FailureConflict &&
		write.Failure.Retryable && len(write.Revision.Bytes()) == 0 && (entry.Kind == historycheck.Add || entry.Kind == historycheck.Replace) {
		result.Conflict = true
		return result, nil
	}
	if write.Status == sink.WritePreconditionFailed && (entry.Kind == historycheck.Create || entry.Kind == historycheck.Replace) {
		if write.Failure == nil || write.Failure.Code != sink.FailurePreconditionFailed || write.Failure.Retryable || len(write.Revision.Bytes()) != 0 {
			return result, fmt.Errorf("invalid conditional failure: %+v", write)
		}
		return result, nil
	}
	if write.Status != sink.WriteApplied || write.Failure != nil || len(write.Revision.Bytes()) == 0 {
		return result, fmt.Errorf("unexpected write outcome: %+v", write)
	}
	result.Applied = true
	return result, nil
}

func historyOverlaps(history []historycheck.Entry) bool {
	for i, first := range history {
		for _, second := range history[i+1:] {
			if first.Client != second.Client && first.Start < second.End && second.Start < first.End {
				return true
			}
		}
	}
	return false
}

func preserveHistory(t *testing.T, history []historycheck.Entry) {
	t.Helper()
	payload, err := json.MarshalIndent(history, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("complete failing history:\n%s", payload)
	if root := os.Getenv("SINK_CONFORMANCE_ARTIFACTS"); root != "" {
		name := fmt.Sprintf("failed-history-%d.json", time.Now().UnixNano())
		if err := os.WriteFile(filepath.Join(root, name), payload, 0600); err != nil {
			t.Error(err)
		}
	}
}
