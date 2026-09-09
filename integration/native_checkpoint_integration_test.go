//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"testing"

	sink "github.com/liran/sink-go"
	"go.mongodb.org/mongo-driver/v2/bson"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNativeBackendScanCheckpointsDuringBusinessChanges(t *testing.T) {
	for _, descending := range []bool{false, true} {
		t.Run(fmt.Sprintf("descending=%t", descending), func(t *testing.T) {
			nativeFixtures(t, func(t *testing.T, f *nativeFixture) {
				f.seed(t, 6)
				direction, order := 1, "asc"
				if descending {
					direction, order = -1, "desc"
				}
				sort := bson.D{{Key: "_id", Value: direction}}
				find := bson.D{{Key: "find", Value: ""}, {Key: "sort", Value: sort}}
				command := f.command(t, find, fmt.Sprintf(`{"sort":[{"counter":%q}]}`, order))
				req := sink.ScanRequest{Command: command, BatchSize: 2}
				first, err := f.dataset.Scan(t.Context(), req)
				if err != nil || len(first.Documents) != 2 || len(first.NextCursor) == 0 {
					t.Fatalf("first page: %+v err=%v", first, err)
				}
				for i, document := range first.Documents {
					want := int64(i)
					if descending {
						want = 5 - want
					}
					if got := f.decode(t, document).Counter; got != want {
						t.Fatalf("first page order=%d want=%d", got, want)
					}
				}
				checkpoint := bytes.Clone(first.NextCursor)
				// Delete the next unseen record, update another unseen record and
				// insert on each side of the checkpoint. Seek must observe only
				// records after that checkpoint in the requested direction.
				deleted, changed := 2, 4
				want := []int64{3, 4, 5, 99}
				if descending {
					deleted, changed = 3, 1
					want = []int64{2, 1, 0, -1}
				}
				address := sinkAddressForStore(t, f.spec.name, f.name, fmt.Sprintf("record-%02d", deleted))
				deletedResults, err := f.environment.client.Delete(t.Context(), sink.CompletionWaitUntilVisible, address)
				if err != nil || len(deletedResults) != 1 || deletedResults[0].Status != sink.DeleteApplied {
					t.Fatalf("delete during scan: %+v err=%v", deletedResults, err)
				}
				for _, value := range []backendDocument{
					{UID: "before", Counter: -1, Value: "inserted"},
					{UID: "record-99", Counter: 99, Value: "inserted"},
					{UID: fmt.Sprintf("record-%02d", changed), Counter: int64(changed), Value: "changed"},
				} {
					record := sink.Record{Key: sink.StringKey(value.UID), Value: value}
					results, err := f.dataset.Upsert(t.Context(), sink.CompletionWaitUntilVisible, record)
					if err != nil {
						t.Fatal(err)
					}
					assertWriteResults(t, results, sink.WriteApplied)
				}
				encoding := sink.DocumentEncodingJSON
				if f.bson {
					encoding = sink.DocumentEncodingBSON
				}
				opts := sink.DatasetOptions{Store: f.spec.name, Namespace: "catalog", Dataset: f.name, Encoding: encoding}
				other, err := sink.NewDataset(f.environment.secondaryClient, opts)
				if err != nil {
					t.Fatal(err)
				}
				req.Cursor = checkpoint
				canceled, cancel := context.WithCancel(t.Context())
				cancel()
				if _, err := other.Scan(canceled, req); status.Code(err) != codes.Canceled {
					t.Fatalf("canceled checkpoint: %v", err)
				}
				corrupt := req
				corrupt.Cursor = bytes.Clone(checkpoint)
				corrupt.Cursor[len(corrupt.Cursor)-1] ^= 1
				if _, err := other.Scan(t.Context(), corrupt); status.Code(err) != codes.InvalidArgument {
					t.Fatalf("corrupted checkpoint accepted: %v", err)
				}
				// Replaying an unchanged checkpoint with quiescent data returns
				// the same remaining records, even with different page sizes.
				for _, size := range []int{1, 3} {
					req.Cursor, req.BatchSize = checkpoint, size
					var got []int64
					for calls := range 10 {
						client := other
						if calls%2 != 0 {
							client = f.dataset
						}
						page, err := client.Scan(t.Context(), req)
						if err != nil {
							t.Fatal(err)
						}
						for _, document := range page.Documents {
							value := f.decode(t, document)
							got = append(got, value.Counter)
							if value.Counter == int64(changed) && value.Value != "changed" {
								t.Fatal("scan returned a stale version of the unseen record")
							}
						}
						if len(page.NextCursor) == 0 {
							break
						}
						if bytes.Equal(page.NextCursor, req.Cursor) {
							t.Fatal("checkpoint failed to advance")
						}
						req.Cursor = page.NextCursor
					}
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("size=%d remaining records=%v want=%v", size, got, want)
					}
				}
				if !bytes.Equal(first.NextCursor, checkpoint) {
					t.Fatal("resuming mutated the caller's checkpoint")
				}
			})
		})
	}
}
