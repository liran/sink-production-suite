//go:build integration

package conformance_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	sink "github.com/liran/sink-go"
	sinkv1 "github.com/liran/sink-go/api/sink/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func nativeSearch(index string) sink.Command {
	command := sink.Command{Store: "primary", Method: http.MethodPost, Path: "/" + index + "/_search",
		ContentType: "application/json", Payload: []byte(`{"query":{"match_all":{}},"sort":[{"counter":"asc"}]}`)}
	return command
}

type nativeResponseFault struct {
	path string
	mode string
}

type nativeResponseProxy struct {
	backend backend
	fault   atomic.Pointer[nativeResponseFault]
	seen    atomic.Int64
}

// Damage real backend responses, retaining real scroll IDs so failures exercise
// cursor cleanup. Never fabricate a successful page or storage operation.
func nativeResponses(t *testing.T, store backend) *nativeResponseProxy {
	t.Helper()
	target, err := url.Parse(store.endpoint)
	if err != nil {
		t.Fatal(err)
	}
	fixture := &nativeResponseProxy{backend: store}
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.ModifyResponse = func(response *http.Response) error {
		fault := fixture.fault.Load()
		if fault == nil || response.Request.URL.Path != fault.path || response.Request.Method != http.MethodPost {
			return nil
		}
		fixture.seen.Add(1)
		payload, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil {
			return err
		}
		var body map[string]any
		if err := json.Unmarshal(payload, &body); err != nil {
			return err
		}
		switch fault.mode {
		case "timed-out":
			body["timed_out"] = true
		case "failed-shard":
			body["_shards"] = map[string]int{"failed": 1}
		case "missing-timeout":
			delete(body, "timed_out")
		case "missing-shards":
			delete(body, "_shards")
		case "incomplete-shards":
			body["_shards"] = map[string]int{"total": 2, "successful": 1, "failed": 0}
		case "skipped-cluster":
			body["_clusters"] = map[string]int{"total": 2, "successful": 1, "skipped": 1}
		case "missing-hits":
			delete(body, "hits")
		case "approximate-count":
			hits, ok := body["hits"].(map[string]any)
			if !ok {
				return errors.New("real count response omitted hits")
			}
			hits["total"] = map[string]any{"value": 1, "relation": "gte"}
		}
		payload, err = json.Marshal(body)
		if err != nil {
			return err
		}
		if fault.mode == "malformed" {
			payload = []byte(`{"damaged":`)
		}
		response.Body = io.NopCloser(strings.NewReader(string(payload)))
		response.ContentLength = int64(len(payload))
		response.Header.Set("Content-Length", fmt.Sprint(len(payload)))
		return nil
	}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Header.Del("Accept-Encoding")
		proxy.ServeHTTP(w, r)
	})
	server := httptest.NewServer(handler)
	fixture.backend.endpoint = server.URL
	t.Cleanup(server.Close)
	return fixture
}

func TestNativeRejectsIncompleteBackendResults(t *testing.T) {
	for _, store := range searchBackends(t) {
		t.Run(store.driver, func(t *testing.T) {
			index := indexFor(t, store, "100ms")
			proxy := nativeResponses(t, store)
			opts := serverOptions{backend: proxy.backend}
			server := startCandidate(t, opts)
			for i := range 3 {
				address := addressFor(t, index, fmt.Sprint(i))
				operation := put(t, address, fmt.Sprintf(`{"counter":%d}`, i), sink.WriteCreate)
				applied(t, writeAsync(t.Context(), server.client, sink.CompletionWaitUntilVisible, operation), 1)
			}
			command := nativeSearch(index)
			for _, method := range []string{"Query", "Count", "Scan"} {
				for _, mode := range []string{"timed-out", "failed-shard", "missing-timeout", "missing-shards", "incomplete-shards", "skipped-cluster", "missing-hits", "malformed", "approximate-count"} {
					if mode == "approximate-count" && method != "Count" {
						continue
					}
					t.Run(method+"/"+mode, func(t *testing.T) {
						fault := &nativeResponseFault{path: command.Path, mode: mode}
						proxy.seen.Store(0)
						proxy.fault.Store(fault)
						t.Cleanup(func() { proxy.fault.Store(nil) })
						ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
						defer cancel()
						var err error
						switch method {
						case "Query":
							req := sink.QueryRequest{Command: command, PageSize: 1}
							var result sink.QueryResponse
							result, err = server.client.Query(ctx, req)
							if len(result.Documents) != 0 || result.HasMore {
								t.Fatal("failed Query exposed an apparently valid page")
							}
						case "Count":
							req := sink.CountRequest{Command: command}
							var result sink.CountResponse
							result, err = server.client.Count(ctx, req)
							if result.Count != 0 || result.Estimated {
								t.Fatal("failed Count exposed a misleading total")
							}
						case "Scan":
							req := sink.ScanRequest{Command: command, BatchSize: 1}
							var result sink.ScanResponse
							result, err = server.client.Scan(ctx, req)
							if len(result.Documents) != 0 || len(result.NextCursor) != 0 {
								t.Fatal("failed Scan exposed a partial page")
							}

						}
						if status.Code(err) != codes.Internal || proxy.seen.Load() != 1 {
							t.Fatalf("damaged response must fail once without retry: attempts=%d, %v", proxy.seen.Load(), err)
						}
						proxy.fault.Store(nil)
						assertNoSearchCursors(t, store, index)
						req := sink.CountRequest{Command: command}
						result, err := server.client.Count(ctx, req)
						if err != nil || result.Count != 3 || result.Estimated {
							t.Fatalf("healthy request after native failure: %+v, %v", result, err)
						}
					})
				}
			}
		})
	}
}

func assertNoSearchCursors(t *testing.T, store backend, index string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var contexts int
	for time.Now().Before(deadline) {
		call := httpCall{endpoint: store.endpoint, method: http.MethodGet, path: "/" + index + "/_stats/search"}
		code, body := request(t, call)
		var result struct {
			All struct {
				Total struct {
					Search struct {
						OpenContexts *int `json:"open_contexts"`
					} `json:"search"`
				} `json:"total"`
			} `json:"_all"`
		}
		if err := json.Unmarshal(body, &result); err != nil || code != 200 || result.All.Total.Search.OpenContexts == nil {
			t.Fatalf("cursor cleanup observation unavailable: HTTP %d %s, %v", code, body, err)
		}
		contexts = *result.All.Total.Search.OpenContexts
		if contexts == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("native scan leaked %d backend cursors", contexts)
}

func TestNativeScanCancellationReleasesCursorAndAdmission(t *testing.T) {
	for _, store := range searchBackends(t) {
		t.Run(store.driver, func(t *testing.T) {
			index := indexFor(t, store, "100ms")
			proxy := proxyBackend(t, store)
			opts := serverOptions{backend: proxy.backend, capacity: 1}
			server := startCandidate(t, opts)
			for i := range 3 {
				address := addressFor(t, index, fmt.Sprint(i))
				operation := put(t, address, fmt.Sprintf(`{"counter":%d}`, i), sink.WriteCreate)
				applied(t, writeAsync(t.Context(), server.client, sink.CompletionWaitUntilVisible, operation), 1)
			}
			command := nativeSearch(index)
			req := sink.ScanRequest{Command: command, BatchSize: 1}
			gate := proxy.hold(command.Path, "search_after", 1)
			t.Cleanup(gate.open)
			first, err := server.client.Scan(t.Context(), req)
			if err != nil || len(first.NextCursor) == 0 {
				t.Fatalf("first=%+v err=%v", first, err)
			}
			req.Cursor = first.NextCursor
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() { _, err := server.client.Scan(ctx, req); done <- err }()
			gate.wait(t)
			cancel()
			if err := <-done; status.Code(err) != codes.Canceled {
				t.Fatalf("cancel=%v", err)
			}
			assertNoSearchCursors(t, store, index)
			count := sink.CountRequest{Command: command}
			deadline, stopCount := context.WithTimeout(t.Context(), 3*time.Second)
			defer stopCount()
			result, err := server.client.Count(deadline, count)
			if err != nil || result.Count != 3 {
				t.Fatalf("canceled scan retained admission: %+v, %v", result, err)
			}
			gate.open()
			page, err := server.client.Scan(t.Context(), req)
			if err != nil || len(page.Documents) != 1 {
				t.Fatalf("cancellation invalidated checkpoint: %+v %v", page, err)
			}
		})
	}
}

func TestNativeExecuteLostResponseDoesNotReplay(t *testing.T) {
	for _, store := range searchBackends(t) {
		t.Run(store.driver, func(t *testing.T) {
			index := indexFor(t, store, "-1")
			proxy := proxyBackend(t, store)
			opts := serverOptions{backend: proxy.backend}
			server := startCandidate(t, opts)
			address := addressFor(t, index, "native")
			operation := put(t, address, `{"counter":0}`, sink.WriteCreate)
			applied(t, writeAsync(t.Context(), server.client, sink.CompletionWaitUntilApplied, operation), 1)
			command := sink.Command{Store: "primary", Method: http.MethodPost, Path: "/" + index + "/_update/native",
				ContentType: "application/json", Payload: []byte(`{"script":{"source":"ctx._source.counter += 1"}}`)}
			gate := proxy.holdResponse(command.Path, "script", true)
			t.Cleanup(gate.open)
			req := sink.ExecuteRequest{Command: command}
			done := make(chan error, 1)
			go func() { _, err := server.client.Execute(t.Context(), req); done <- err }()
			gate.wait(t)
			assertCounter(t, server.client, address, 1)
			gate.open()
			select {
			case err := <-done:
				if status.Code(err) != codes.Unavailable {
					t.Fatalf("lost native response did not report unavailable: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("native Execute did not finish after lost response")
			}
			assertCounter(t, server.client, address, 1)
			proxy.mu.Lock()
			attempts := gate.seen
			proxy.mu.Unlock()
			if attempts != 1 {
				t.Fatalf("ambiguous native increment was replayed: %d attempts", attempts)
			}
		})
	}
}

func TestNativeScanDeadlinesReleaseResources(t *testing.T) {
	for _, store := range searchBackends(t) {
		t.Run(store.driver, func(t *testing.T) {
			index := indexFor(t, store, "100ms")
			proxy := proxyBackend(t, store)
			opts := serverOptions{backend: proxy.backend, capacity: 1, requestTimeout: 1}
			server := startCandidate(t, opts)
			address := addressFor(t, index, "deadline")
			operation := put(t, address, `{"counter":1}`, sink.WriteCreate)
			applied(t, writeAsync(t.Context(), server.client, sink.CompletionWaitUntilVisible, operation), 1)
			command := nativeSearch(index)
			gate := proxy.hold(command.Path, "sort", 1)
			t.Cleanup(gate.open)
			req := sink.ScanRequest{Command: command, BatchSize: 1}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			page, err := server.client.Scan(ctx, req)
			gate.wait(t)
			if status.Code(err) != codes.DeadlineExceeded || ctx.Err() != nil || len(page.Documents) != 0 || len(page.NextCursor) != 0 {
				t.Fatalf("server deadline: page=%+v client=%v error=%v", page, ctx.Err(), err)
			}
			assertNoSearchCursors(t, store, index)
			count := sink.CountRequest{Command: command}
			result, err := server.client.Count(t.Context(), count)
			if err != nil || result.Count != 1 {
				t.Fatalf("deadline retained resources: %+v %v", result, err)
			}
		})
	}
}

func TestReturnedWriteCommitAndConflictBoundaries(t *testing.T) {
	for _, store := range searchBackends(t) {
		for _, batchOps := range []int{1000, 1} {
			t.Run(fmt.Sprintf("%s/batch-ops=%d", store.driver, batchOps), func(t *testing.T) {
				index := indexFor(t, store, "-1")
				proxy := proxyBackend(t, store)
				opts := serverOptions{backend: proxy.backend, batchOps: batchOps}
				server := startCandidate(t, opts)
				address := addressFor(t, index, "returned")
				gate := proxy.hold("/_bulk", "returned", 1)
				t.Cleanup(gate.open)
				operation := merge(t, address, increment).WithReturnedDocument()
				done := writeAsync(t.Context(), server.client, sink.CompletionWaitUntilApplied, operation, operation)
				gate.wait(t)
				call := httpCall{endpoint: store.endpoint, method: http.MethodPut, path: "/" + index + "/_doc/returned", body: []byte(`{"counter":10}`)}
				code, body := request(t, call)
				if code != http.StatusCreated {
					t.Fatalf("competing writer: %d %s", code, body)
				}
				gate.open()
				results := applied(t, done, 2)
				for i, result := range results {
					var document struct {
						Counter int `json:"counter"`
					}
					if err := result.Document.Decode(&document); err != nil || document.Counter != 11+i {
						t.Fatalf("returned speculative or final-chain document for operation %d: %+v, %v", i, document, err)
					}
				}
				if writes := proxy.count("/_bulk", "returned"); writes != 3 {
					t.Fatalf("returned chain must commit individually after one conflict: %d writes", writes)
				}
				assertCounter(t, server.client, address, 12)
			})
		}
	}
}

func TestReturnedWriteBudgetsBelongToOriginalRPC(t *testing.T) {
	for _, store := range searchBackends(t) {
		t.Run(store.driver, func(t *testing.T) {
			index := indexFor(t, store, "-1")
			opts := serverOptions{backend: store, readBytes: 768, batchOps: 2, batchWait: 1000}
			server := startCandidate(t, opts)
			address := addressFor(t, index, "budget")
			first := put(t, address, fmt.Sprintf(`{"counter":1,"pad":%q}`, strings.Repeat("a", 400)), sink.WriteUpsert).WithReturnedDocument()
			second := put(t, address, fmt.Sprintf(`{"counter":2,"pad":%q}`, strings.Repeat("b", 400)), sink.WriteUpsert).WithReturnedDocument()
			results, err := server.client.Write(t.Context(), sink.CompletionWaitUntilApplied, first, second)
			if err != nil || len(results) != 2 || results[0].Status != sink.WriteApplied || results[1].Status != sink.WriteFailed ||
				results[1].Failure == nil || results[1].Failure.Code != sink.FailureResourceExhausted || len(results[1].Document.Payload()) != 0 {
				t.Fatalf("returned response budget did not reject before the second commit: %+v, %v", results, err)
			}
			assertCounter(t, server.client, address, 1)
			one := writeAsync(t.Context(), server.client, sink.CompletionWaitUntilApplied, first)
			server.waitQueued(t, "Write")
			two := writeAsync(t.Context(), server.client, sink.CompletionWaitUntilApplied, second)
			for _, result := range [][]sink.WriteResult{applied(t, one, 1), applied(t, two, 1)} {
				if len(result[0].Document.Payload()) < 400 {
					t.Fatal("valid original RPC lost its returned-document budget")
				}
			}
			assertCounter(t, server.client, address, 2)
		})
	}
}

func TestNativeResponseLimitsFailWithoutTruncation(t *testing.T) {
	for _, store := range searchBackends(t) {
		t.Run(store.driver, func(t *testing.T) {
			index := indexFor(t, store, "100ms")
			opts := serverOptions{backend: store, readBytes: 1024}
			server := startCandidate(t, opts)
			address := addressFor(t, index, "large")
			operation := put(t, address, fmt.Sprintf(`{"counter":1,"pad":%q}`, strings.Repeat("x", 2048)), sink.WriteCreate)
			applied(t, writeAsync(t.Context(), server.client, sink.CompletionWaitUntilVisible, operation), 1)
			command := nativeSearch(index)
			query := sink.QueryRequest{Command: command, PageSize: 1}
			page, err := server.client.Query(t.Context(), query)
			if status.Code(err) != codes.ResourceExhausted || len(page.Documents) != 0 || page.HasMore {
				t.Fatalf("oversized Query returned a truncated page: %+v, %v", page, err)
			}
			execute := sink.ExecuteRequest{Command: command}
			response, err := server.client.Execute(t.Context(), execute)
			if status.Code(err) != codes.ResourceExhausted || response.Success || len(response.Payload) != 0 {
				t.Fatalf("oversized Execute returned a truncated body: %+v, %v", response, err)
			}
			scan := sink.ScanRequest{Command: command, BatchSize: 1}
			scanPage, err := server.client.Scan(t.Context(), scan)
			if status.Code(err) != codes.ResourceExhausted || len(scanPage.Documents) != 0 || len(scanPage.NextCursor) != 0 {
				t.Fatalf("oversized Scan returned a partial page: %+v %v", scanPage, err)
			}
			assertNoSearchCursors(t, store, index)

			count := sink.CountRequest{Command: command}
			result, err := server.client.Count(t.Context(), count)
			if err != nil || result.Count != 1 || result.Estimated {
				t.Fatalf("count should fit without fetching the oversized document: %+v, %v", result, err)
			}
		})
	}
}

func TestNativeWireValidationBeforeExecution(t *testing.T) {
	broker := startBroker(t)
	for _, store := range searchBackends(t) {
		t.Run(store.driver, func(t *testing.T) {
			index := indexFor(t, store, "-1")
			proxy := proxyBackend(t, store)
			topic := fmt.Sprintf("sink-native-validation-%d", time.Now().UnixNano())
			opts := serverOptions{backend: proxy.backend, broker: broker.address, topic: topic}
			server := startCandidate(t, opts)
			connection, err := grpc.NewClient(server.address, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = connection.Close() })
			client := sinkv1.NewSinkClient(connection)
			command := &sinkv1.Command{Store: "primary", Method: "POST", Path: "/" + index + "/_search", ContentType: "application/json", Payload: []byte(`{}`)}
			// Use raw RPCs to bypass SDK validation and qualify the server boundary.
			tooLarge := &sinkv1.QueryRequest{Command: command, PageSize: 1001}
			_, err = client.Query(t.Context(), tooLarge)
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("server accepted oversized page: %v", err)
			}
			field := &sinkv1.SortField{Field: "counter"}
			duplicate := &sinkv1.QueryRequest{Command: command, Sort: []*sinkv1.SortField{field, field}}
			_, err = client.Query(t.Context(), duplicate)
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("server accepted duplicate sort: %v", err)
			}
			scan := &sinkv1.ScanRequest{Command: command, BatchSize: 1001}
			_, err = client.Scan(t.Context(), scan)
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("server accepted oversized scan batch: %v", err)
			}
			for _, parameter := range []string{"scroll=2m", "filter_path=hits", "source=%7B%7D", "pit=x"} {
				command.Query = parameter
				query := &sinkv1.QueryRequest{Command: command}
				_, err := client.Query(t.Context(), query)
				if status.Code(err) != codes.InvalidArgument {
					t.Fatalf("server accepted managed pagination parameter %s: %v", parameter, err)
				}
			}
			keyValue := &sinkv1.RecordKey_StringValue{StringValue: "unpublished"}
			key := &sinkv1.RecordKey{Kind: keyValue}
			address := &sinkv1.RecordAddress{Store: "primary", Namespace: "catalog", Dataset: index, Key: key}
			document := &sinkv1.Document{Encoding: sinkv1.DocumentEncoding_DOCUMENT_ENCODING_JSON, Payload: []byte(`{"counter":1}`)}
			put := &sinkv1.PutOperation{Mode: sinkv1.WriteMode_WRITE_MODE_UPSERT, Document: document}
			action := &sinkv1.WriteOperation_Put{Put: put}
			operation := &sinkv1.WriteOperation{Address: address, Action: action, ReturnDocument: true}
			write := &sinkv1.WriteRequest{CompletionMode: sinkv1.CompletionMode_COMPLETION_MODE_RETURN_AFTER_ACCEPTED, Operations: []*sinkv1.WriteOperation{operation}}
			_, err = client.Write(t.Context(), write)
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("server accepted asynchronous returned write: %v", err)
			}
			broker.assertEnd(t, topic, 0)
			proxy.mu.Lock()
			var calls []string
			for _, request := range proxy.requests {
				if strings.HasPrefix(request.Path, "/"+index) || request.Path == "/_bulk" {
					calls = append(calls, request.Path)
				}
			}
			proxy.mu.Unlock()
			if len(calls) != 0 {
				t.Fatalf("invalid native requests reached storage: %v", calls)
			}
		})
	}
}
