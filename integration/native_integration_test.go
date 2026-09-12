//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	sink "github.com/liran/sink-go"
	"go.mongodb.org/mongo-driver/v2/bson"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type nativeFixture struct {
	environment *testEnvironment
	spec        backendStoreSpec
	name        string
	dataset     *sink.Dataset
	bson        bool
}

func nativeFixtures(t *testing.T, check func(*testing.T, *nativeFixture)) {
	t.Helper()
	specs := configuredBackendStores(t)
	if len(specs) == 0 {
		t.Fatal("SINK_BACKEND_STORES is required for native qualification")
	}
	environment := newTestEnvironment(t)
	for _, spec := range specs {
		t.Run(spec.name, func(t *testing.T) {
			name := fmt.Sprintf("sink-native-%s-%d", spec.name, time.Now().UnixNano())
			isBSON := strings.HasPrefix(spec.name, "mongodb-")
			encoding := sink.DocumentEncodingJSON
			if isBSON {
				encoding = sink.DocumentEncodingBSON
			}
			opts := sink.DatasetOptions{Store: spec.name, Namespace: "catalog", Dataset: name, Encoding: encoding}
			dataset, err := sink.NewDataset(environment.client, opts)
			if err != nil {
				t.Fatal(err)
			}
			fixture := &nativeFixture{environment: environment, spec: spec, name: name, dataset: dataset, bson: isBSON}
			check(t, fixture)
		})
	}
}

func (f *nativeFixture) command(t *testing.T, document bson.D, json string) sink.Command {
	t.Helper()
	if f.bson {
		command, err := sink.NewBSONCommand(f.spec.name, "catalog", document)
		if err != nil {
			t.Fatal(err)
		}
		return command
	}
	command := sink.Command{ContentType: "application/json", Payload: []byte(json)}
	return command
}

func (f *nativeFixture) seed(t *testing.T, count int) {
	t.Helper()
	records := make([]sink.Record, count)
	// Reverse insertion order makes implicit backend ordering an invalid oracle.
	for i := range records {
		n := count - i - 1
		value := backendDocument{UID: fmt.Sprintf("record-%02d", n), Counter: int64(n), Value: "retained",
			UpdatedAt: time.Date(2026, 9, 8, 1, 2, 3, 0, time.UTC)}
		record := sink.Record{Key: sink.StringKey(value.UID), Value: value}
		records[i] = record
	}
	results, err := f.dataset.Create(t.Context(), sink.CompletionWaitUntilVisible, records...)
	if err != nil || len(results) != count {
		t.Fatalf("seed: %v, %d results", err, len(results))
	}
	assertWriteResults(t, results, sink.WriteApplied)
}

func (f *nativeFixture) decode(t *testing.T, document sink.Document) backendDocument {
	t.Helper()
	var value backendDocument
	if f.bson {
		if document.Encoding() != sink.DocumentEncodingBSON {
			t.Fatal("MongoDB native document lost BSON encoding")
		}
		if err := document.Decode(&value); err != nil {
			t.Fatal(err)
		}
	} else {
		var hit struct {
			ID     string          `json:"_id"`
			Source backendDocument `json:"_source"`
		}
		if document.Encoding() != sink.DocumentEncodingJSON {
			t.Fatal("search native hit lost JSON encoding")
		}
		if err := document.Decode(&hit); err != nil {
			t.Fatal(err)
		}
		value = hit.Source
		if hit.ID == "" || (value.UID != "" && hit.ID != value.UID) {
			t.Fatalf("native search hit lost its identity: %+v", hit)
		}
	}
	return value
}

func TestNativeBackendQueryCountScan(t *testing.T) {
	nativeFixtures(t, func(t *testing.T, f *nativeFixture) {
		f.seed(t, 12)
		t.Run("pages-and-projection", func(t *testing.T) {
			find := bson.D{{Key: "find", Value: ""}, {Key: "skip", Value: 99}, {Key: "limit", Value: 1},
				{Key: "sort", Value: bson.D{{Key: "counter", Value: -1}}},
				{Key: "projection", Value: bson.D{{Key: "value", Value: 1}}}}
			command := f.command(t, find, `{"from":99,"size":1,"sort":[{"counter":"desc"}],"_source":["value"]}`)
			projection := &sink.Projection{Fields: []string{"counter"}}
			sort := []sink.SortField{{Field: "counter"}}
			for _, size := range []int{1, 4, 5, 1000} {
				var counters []int64
				for page := 1; page <= 14; page++ {
					req := sink.QueryRequest{Command: command, Page: page, PageSize: size, Sort: sort, Projection: projection}
					result, err := f.dataset.Query(t.Context(), req)
					if err != nil {
						t.Fatal(err)
					}
					wantSize := min(size, max(0, 12-(page-1)*size))
					wantMore := page*size < 12
					if len(result.Documents) != wantSize || result.HasMore != wantMore {
						t.Fatalf("size=%d page=%d: length=%d more=%t, want %d/%t", size, page, len(result.Documents), result.HasMore, wantSize, wantMore)
					}
					for _, document := range result.Documents {
						value := f.decode(t, document)
						if value.Value != "" || !value.UpdatedAt.IsZero() {
							t.Fatalf("projection retained excluded fields: %+v", value)
						}
						counters = append(counters, value.Counter)
					}
					// Also request the first page beyond the end, including exact multiples.
					if wantSize == 0 {
						break
					}
				}
				want := []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}
				if !slices.Equal(counters, want) {
					t.Fatalf("pagination duplicated, lost or reordered records: %v", counters)
				}
			}
			projection = &sink.Projection{Fields: []string{"value"}, Exclude: true}
			sort[0].Descending = true
			req := sink.QueryRequest{PageSize: 2, Sort: sort, Projection: projection}
			result, err := f.dataset.Query(t.Context(), req)
			if err != nil || len(result.Documents) != 2 || !result.HasMore {
				t.Fatalf("default page/exclusion: %+v, %v", result, err)
			}
			for i, document := range result.Documents {
				value := f.decode(t, document)
				if value.Counter != int64(11-i) || value.Value != "" || value.UID == "" || value.UpdatedAt.IsZero() {
					t.Fatalf("descending exclusion projection: %+v", value)
				}
			}
		})
		t.Run("count-strategy-and-filter", func(t *testing.T) {
			req := sink.CountRequest{}
			result, err := f.dataset.Count(t.Context(), req)
			if err != nil || result.Count != 12 || result.Estimated != f.bson {
				t.Fatalf("unfiltered count strategy: %+v, %v", result, err)
			}
			find := bson.D{{Key: "find", Value: ""}, {Key: "filter", Value: bson.D{{Key: "counter", Value: bson.D{{Key: "$gte", Value: 7}}}}},
				{Key: "skip", Value: 99}, {Key: "limit", Value: 1}, {Key: "projection", Value: bson.D{{Key: "value", Value: 1}}}}
			command := f.command(t, find, `{"query":{"range":{"counter":{"gte":7}}},"from":99,"size":1,"track_total_hits":1,"collapse":{"field":"counter"},"aggs":{"bad":{"terms":{"field":"value"}}}}`)
			req.Command = command
			result, err = f.dataset.Count(t.Context(), req)
			if err != nil || result.Count != 5 || result.Estimated {
				t.Fatalf("count must ignore pagination/presentation and remain exact: %+v, %v", result, err)
			}
			if f.bson {
				pipeline := bson.A{bson.D{{Key: "$match", Value: bson.D{{Key: "counter", Value: bson.D{{Key: "$gte", Value: 7}}}}}}}
				document := bson.D{{Key: "aggregate", Value: ""}, {Key: "pipeline", Value: pipeline}}
				command := f.command(t, document, "")
				sort := []sink.SortField{{Field: "counter", Descending: true}}
				query := sink.QueryRequest{Command: command, Page: 2, PageSize: 2, Sort: sort}
				page, err := f.dataset.Query(t.Context(), query)
				if err != nil || len(page.Documents) != 2 || !page.HasMore {
					t.Fatalf("aggregate pagination: %+v, %v", page, err)
				}
				for i, document := range page.Documents {
					if value := f.decode(t, document); value.Counter != int64(9-i) {
						t.Fatalf("aggregate page ordering: %+v", value)
					}
				}
				for _, hint := range []bool{false, true} {
					pipeline := bson.A{bson.D{{Key: "$limit", Value: 3}}}
					document := bson.D{{Key: "aggregate", Value: ""}, {Key: "pipeline", Value: pipeline}}
					if hint {
						document = bson.D{{Key: "find", Value: ""}, {Key: "hint", Value: "_id_"}}
					}
					req.Command = f.command(t, document, "")
					result, err = f.dataset.Count(t.Context(), req)
					want := uint64(3)
					if hint {
						want = 12
					}
					if err != nil || result.Estimated || result.Count != want {
						t.Fatalf("pipeline/hint exact count: %+v, %v", result, err)
					}
				}
			}
			find = bson.D{{Key: "find", Value: ""}, {Key: "filter", Value: bson.D{{Key: "counter", Value: -1}}}}
			command = f.command(t, find, `{"query":{"term":{"counter":-1}}}`)
			req.Command = command
			result, err = f.dataset.Count(t.Context(), req)
			if err != nil || result.Count != 0 || result.Estimated {
				t.Fatalf("empty exact count: %+v, %v", result, err)
			}
			query := sink.QueryRequest{Command: command}
			empty, err := f.dataset.Query(t.Context(), query)
			if err != nil || len(empty.Documents) != 0 || empty.HasMore {
				t.Fatalf("empty query: %+v, %v", empty, err)
			}
		})
		if f.bson {
			t.Run("unsafe-scan-rejected-before-writing", func(t *testing.T) {
				for _, stage := range []string{"$out", "$merge"} {
					pipeline := bson.A{bson.D{{Key: stage, Value: f.name + "-forbidden"}}}
					document := bson.D{{Key: "aggregate", Value: ""}, {Key: "pipeline", Value: pipeline}}
					command := f.command(t, document, "")
					scan := sink.ScanRequest{Command: command, BatchSize: 1}
					page, err := f.dataset.Scan(t.Context(), scan)
					if status.Code(err) != codes.InvalidArgument || len(page.Documents) != 0 {
						t.Fatalf("Scan accepted writing stage %s: %+v %v", stage, page, err)
					}

					query := sink.QueryRequest{Command: command}
					_, err = f.dataset.Query(t.Context(), query)
					if status.Code(err) != codes.InvalidArgument {
						t.Fatalf("Query accepted writing stage %s: %v", stage, err)
					}
					count := sink.CountRequest{Command: command}
					_, err = f.dataset.Count(t.Context(), count)
					if status.Code(err) != codes.InvalidArgument {
						t.Fatalf("Count accepted writing stage %s: %v", stage, err)
					}
					find := bson.D{{Key: "find", Value: f.name + "-forbidden"}}
					command, err = sink.NewBSONCommand(f.spec.name, "catalog", find)
					if err != nil {
						t.Fatal(err)
					}
					count.Command = command
					result, err := f.environment.client.Count(t.Context(), count)
					if err != nil || result.Count != 0 {
						t.Fatalf("rejected native read created output documents: %+v, %v", result, err)
					}
				}
			})
		}
		t.Run("scan-pages-and-cancellation", func(t *testing.T) {
			for _, size := range []int{1, 5, 1000} {
				req := sink.ScanRequest{BatchSize: size}
				if !f.bson {
					req.Command.Payload = []byte(`{"sort":[{"counter":"asc"}]}`)
				}
				var counters []int64
				for {
					page, err := f.dataset.Scan(t.Context(), req)
					if err != nil {
						t.Fatal(err)
					}
					for _, document := range page.Documents {
						value := f.decode(t, document)
						if value.Value != "retained" || value.UpdatedAt.IsZero() {
							t.Fatalf("scan corrupted document: %+v", value)
						}
						counters = append(counters, value.Counter)
					}
					if len(page.NextCursor) == 0 {
						break
					}
					req.Cursor = page.NextCursor
				}
				slices.Sort(counters)
				want := []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}
				if !slices.Equal(counters, want) {
					t.Fatalf("scan size=%d: %v", size, counters)
				}
			}
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			req := sink.ScanRequest{}
			if !f.bson {
				req.Command.Payload = []byte(`{"sort":[{"counter":"asc"}]}`)
			}
			page, err := f.dataset.Scan(ctx, req)
			if status.Code(err) != codes.Canceled || len(page.Documents) != 0 {
				t.Fatalf("canceled scan: %+v %v", page, err)
			}
			count := sink.CountRequest{}
			result, err := f.dataset.Count(t.Context(), count)
			if err != nil || result.Count != 12 {
				t.Fatalf("operations did not recover: %+v %v", result, err)
			}
		})
	})
}

func TestNativeBackendExecute(t *testing.T) {
	nativeFixtures(t, func(t *testing.T, f *nativeFixture) {
		f.seed(t, 1)
		before, err := f.dataset.Read(t.Context(), sink.StringKey("record-00"))
		if err != nil || len(before) != 1 || before[0].Status != sink.ReadFound {
			t.Fatalf("read before native write: %+v, %v", before, err)
		}
		var command sink.Command
		if f.bson {
			update := bson.D{{Key: "findAndModify", Value: ""}, {Key: "query", Value: bson.D{{Key: "_id", Value: "record-00"}}},
				{Key: "update", Value: bson.D{{Key: "$inc", Value: bson.D{{Key: "counter", Value: 1}}}}}, {Key: "new", Value: true}}
			command = f.command(t, update, "")
		} else {
			command = sink.Command{Method: http.MethodPost, Path: "/_update/record-00", ContentType: "application/json",
				Payload: []byte("{\n\"script\":{\"source\":\"ctx._source.counter += 1\"}}")}
		}
		req := sink.ExecuteRequest{Command: command}
		result, err := f.dataset.Execute(t.Context(), req)
		if err != nil || !result.Success || len(result.Payload) == 0 {
			t.Fatalf("native mutation: %+v, %v", result, err)
		}
		address := sinkAddressForStore(t, f.spec.name, f.name, "record-00")
		want := backendExpectation{client: f.environment.secondaryClient, address: address, wantUID: "record-00", wantValue: "retained", wantCounter: 1, wantUpdated: true}
		assertBackendDocument(t, t.Context(), want)
		after, err := f.environment.secondaryClient.Read(t.Context(), address)
		if err != nil || len(after) != 1 || after[0].Status != sink.ReadFound || bytes.Equal(before[0].Revision.Bytes(), after[0].Revision.Bytes()) {
			t.Fatalf("native write did not invalidate the revision seen by the other server: %+v, %v", after, err)
		}
		if f.bson {
			invalid := bson.D{{Key: "count", Value: ""}, {Key: "sinkQualificationUnknownOption", Value: true}}
			req.Command = f.command(t, invalid, "")
		} else {
			req.Command = sink.Command{Method: http.MethodGet, Path: "/_doc/missing"}
		}
		result, err = f.dataset.Execute(t.Context(), req)
		var nativeError *sink.NativeError
		if !errors.As(err, &nativeError) || result.Success || len(result.Payload) == 0 || !bytes.Equal(result.Payload, nativeError.Response.Payload) {
			t.Fatalf("native error must retain complete backend response: %+v, %v", result, err)
		}
		if f.bson {
			var reply bson.M
			if err := result.Decode(&reply); err != nil || reply["code"] == nil || reply["errmsg"] == nil {
				t.Fatalf("MongoDB error fields missing: %v, %v", reply, err)
			}
			duplicate := bson.D{{Key: "_id", Value: "record-00"}}
			following := bson.D{{Key: "_id", Value: "partial-batch-success"}, {Key: "counter", Value: int64(7)}}
			insert := bson.D{{Key: "insert", Value: ""}, {Key: "documents", Value: bson.A{duplicate, following}}, {Key: "ordered", Value: false}}
			req.Command = f.command(t, insert, "")
			result, err = f.dataset.Execute(t.Context(), req)
			if !errors.As(err, &nativeError) || result.Success || bson.Raw(result.Payload).Lookup("writeErrors").Type != bson.TypeArray {
				t.Fatalf("partial MongoDB write lost its native error: %s err=%v", bson.Raw(result.Payload), err)
			}
			persisted, err := f.dataset.Read(t.Context(), sink.StringKey("partial-batch-success"))
			if err != nil || len(persisted) != 1 || persisted[0].Status != sink.ReadFound {
				t.Fatalf("unordered batch lost its successful sibling: %+v err=%v", persisted, err)
			}
			find := bson.D{{Key: "find", Value: ""}, {Key: "allowPartialResults", Value: true}}
			partial := sink.QueryRequest{Command: f.command(t, find, ""), PageSize: 1}
			if _, err := f.dataset.Query(t.Context(), partial); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("Query permitted incomplete shard results: %v", err)
			}
			for _, name := range []string{"find", "aggregate", "listIndexes", "getMore", "killCursors", "startSession", "commitTransaction", "drop", "dropDatabase", "renameCollection", "sinkQualificationUnknownCommand"} {
				document := bson.D{{Key: name, Value: ""}}
				req.Command = f.command(t, document, "")
				_, err := f.dataset.Execute(t.Context(), req)
				if status.Code(err) != codes.InvalidArgument {
					t.Fatalf("Execute accepted managed command %s: %v", name, err)
				}
			}
		} else if result.StatusCode != http.StatusNotFound || nativeError.Response.StatusCode != http.StatusNotFound {
			t.Fatalf("HTTP error status lost: %+v", result)
		}
	})
}

func TestNativeBackendReturnedWrites(t *testing.T) {
	nativeFixtures(t, func(t *testing.T, f *nativeFixture) {
		f.seed(t, 1)
		address := sinkAddressForStore(t, f.spec.name, f.name, "record-00")
		program, err := sink.NewLuaProgram([]byte(`return function(current, incoming) current.counter = current.counter + 1; return current end`))
		if err != nil {
			t.Fatal(err)
		}
		incoming := documentForAddress(t, address, map[string]int{"delta": 1})
		opts := sink.MergeOptions{Incoming: incoming, Program: program}
		operation, err := sink.NewMerge(address, opts)
		if err != nil {
			t.Fatal(err)
		}
		returned := operation.WithReturnedDocument()
		operations := []sink.WriteOperation{returned, operation, returned}
		results, err := f.environment.client.Write(t.Context(), sink.CompletionWaitUntilVisible, operations...)
		if err != nil || len(results) != 3 {
			t.Fatalf("returned chain: %+v, %v", results, err)
		}
		assertWriteResults(t, results, sink.WriteApplied)
		for i, result := range results {
			if i == 1 {
				if len(result.Document.Payload()) != 0 {
					t.Fatal("unrequested document was returned")
				}
				continue
			}
			var value backendDocument
			if err := result.Document.Decode(&value); err != nil || value.Counter != int64(i+1) || value.UID != "record-00" {
				t.Fatalf("operation %d returned another commit's document: %+v, %v", i, value, err)
			}
		}
		if bytes.Equal(results[0].Revision.Bytes(), results[2].Revision.Bytes()) {
			t.Fatal("returned operations were folded into one commit")
		}
		retained := append([]byte(nil), results[0].Document.Payload()...)
		const writers = 12
		start := make(chan struct{})
		outcomes := make(chan []sink.WriteResult, writers)
		failures := make(chan error, writers)
		var group sync.WaitGroup
		for i := range writers {
			group.Go(func() {
				<-start
				client := f.environment.client
				if i%2 == 1 {
					client = f.environment.secondaryClient
				}
				ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
				defer cancel()
				result, err := client.Write(ctx, sink.CompletionWaitUntilApplied, returned)
				outcomes <- result
				failures <- err
			})
		}
		close(start)
		group.Wait()
		close(outcomes)
		close(failures)
		for err := range failures {
			if err != nil {
				t.Fatal(err)
			}
		}
		var counters []int64
		revisions := make(map[string]bool)
		for result := range outcomes {
			if len(result) != 1 {
				t.Fatalf("concurrent returned write: %+v", result)
			}
			assertWriteResults(t, result, sink.WriteApplied)
			var value backendDocument
			if err := result[0].Document.Decode(&value); err != nil {
				t.Fatal(err)
			}
			counters = append(counters, value.Counter)
			revision := string(result[0].Revision.Bytes())
			if revision == "" || revisions[revision] {
				t.Fatal("concurrent writes returned duplicate or missing revisions")
			}
			revisions[revision] = true
		}
		slices.Sort(counters)
		for i, counter := range counters {
			if counter != int64(i+4) {
				t.Fatalf("concurrent returned values do not describe individual commits: %v", counters)
			}
		}
		if !reflect.DeepEqual(results[0].Document.Payload(), retained) {
			t.Fatal("later writes mutated a retained returned document")
		}
		want := backendExpectation{client: f.environment.secondaryClient, address: address, wantUID: "record-00", wantValue: "retained", wantCounter: 3 + writers, wantUpdated: true}
		assertBackendDocument(t, t.Context(), want)
		// Exercise Dataset's returned-document option and BatchError together.
		value := backendDocument{UID: "record-00", Counter: 999}
		record := sink.Record{Key: sink.StringKey("record-00"), Value: value, ReturnDocument: true}
		failed, err := f.dataset.Create(t.Context(), sink.CompletionWaitUntilApplied, record)
		var batchError *sink.BatchError
		if !errors.As(err, &batchError) || len(failed) != 1 || failed[0].Status != sink.WritePreconditionFailed || len(failed[0].Document.Payload()) != 0 {
			t.Fatalf("failed create returned an uncommitted document: %+v, %v", failed, err)
		}
		assertBackendDocument(t, t.Context(), want)
		value.Counter = 9007199254740993
		record.Value = value
		replaced, err := f.dataset.Replace(t.Context(), sink.CompletionWaitUntilApplied, record)
		if err != nil || len(replaced) != 1 || replaced[0].Status != sink.WriteApplied {
			t.Fatalf("Dataset returned replacement: %+v, %v", replaced, err)
		}
		var returnedValue backendDocument
		if err := replaced[0].Document.Decode(&returnedValue); err != nil || returnedValue != value {
			t.Fatalf("returned replacement lost exact integer or identity: %+v, %v", returnedValue, err)
		}
	})
}
