//go:build integration

package integration_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"testing"
	"time"

	sink "github.com/liran/sink-go"
	sinkv1 "github.com/liran/sink-go/api/sink/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// Encode the additive wire field explicitly so this qualification remains
// runnable with the released SDK while the paired client release is pending.
func wireOperationID(operation *sinkv1.WriteOperation, id string) {
	field := protowire.AppendTag(nil, 5, protowire.BytesType)
	field = protowire.AppendString(field, id)
	operation.ProtoReflect().SetUnknown(field)
}

func TestNativeBackendIdempotencyContract(t *testing.T) {
	nativeFixtures(t, func(t *testing.T, fixture *nativeFixture) {
		fixture.seed(t, 1)
		primary, err := grpc.NewClient(environmentValue("SINK_ADDRESS", defaultSinkAddress), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = primary.Close() })
		secondary, err := grpc.NewClient(environmentValue("SINK_SECONDARY_ADDRESS", defaultSecondAddress), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = secondary.Close() })
		address := sinkAddressForStore(t, fixture.spec.name, fixture.name, "record-00")
		incoming := documentForAddress(t, address, map[string]int{"delta": 1})
		document := &sinkv1.Document{Encoding: sinkv1.DocumentEncoding(incoming.Encoding()), Payload: incoming.Payload()}
		program := &sinkv1.LuaProgram{Source: []byte(backendCounterMerge)}
		merge := &sinkv1.MergeOperation{IncomingDocument: document, LuaProgram: program, MissingDocumentMode: sinkv1.MissingDocumentMode_MISSING_DOCUMENT_MODE_FAIL}
		action := &sinkv1.WriteOperation_Merge{Merge: merge}
		kind := &sinkv1.RecordKey_StringValue{StringValue: "record-00"}
		key := &sinkv1.RecordKey{Kind: kind}
		wireAddress := &sinkv1.RecordAddress{Store: fixture.spec.name, Namespace: "catalog", Dataset: fixture.name, Key: key}
		operation := &sinkv1.WriteOperation{Address: wireAddress, Action: action, ReturnDocument: true}
		digest := sha256.Sum256([]byte(fixture.name))
		id := fmt.Sprintf("v1:%d:%s", time.Now().UnixMilli(), base64.RawURLEncoding.EncodeToString(digest[:]))
		wireOperationID(operation, id)
		request := &sinkv1.WriteRequest{CompletionMode: sinkv1.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, Operations: []*sinkv1.WriteOperation{operation}}
		response := &sinkv1.WriteResponse{}
		err = primary.Invoke(t.Context(), "/sink.v1.Sink/WriteIdempotent", request, response)
		if !fixture.bson {
			if status.Code(err) != codes.Unimplemented {
				t.Fatalf("unsupported backend did not fail closed: %v %v", response, err)
			}
			values, readErr := fixture.dataset.Read(t.Context(), sink.StringKey("record-00"))
			if readErr != nil || len(values) != 1 {
				t.Fatal(readErr)
			}
			var stored backendDocument
			if err := values[0].Document.Decode(&stored); err != nil || stored.Counter != 0 {
				t.Fatalf("unsafe fallback mutated record: %+v %v", stored, err)
			}
			return
		}
		if err != nil || len(response.Results) != 1 || response.Results[0].Status != sinkv1.WriteStatus_WRITE_STATUS_APPLIED {
			t.Fatalf("protected merge: %v %v", response, err)
		}
		original := proto.Clone(response).(*sinkv1.WriteResponse)
		for range 3 {
			replayed := &sinkv1.WriteResponse{}
			if err := secondary.Invoke(t.Context(), "/sink.v1.Sink/WriteIdempotent", request, replayed); err != nil {
				t.Fatal(err)
			}
			if !proto.Equal(original, replayed) {
				t.Fatalf("another server did not return original receipt: %v %v", original, replayed)
			}
		}
		operation.ReturnDocument = false
		conflict := &sinkv1.WriteResponse{}
		if err := secondary.Invoke(t.Context(), "/sink.v1.Sink/WriteIdempotent", request, conflict); err != nil {
			t.Fatal(err)
		}
		if conflict.Results[0].GetFailure().GetCode() != sinkv1.FailureCode_FAILURE_CODE_CONFLICT {
			t.Fatalf("ID reuse accepted: %v", conflict)
		}
		stored, err := fixture.dataset.Read(t.Context(), sink.StringKey("record-00"))
		if err != nil || len(stored) != 1 {
			t.Fatal(err)
		}
		var value backendDocument
		if err := stored[0].Document.Decode(&value); err != nil || value.Counter != 1 {
			t.Fatalf("replayed effect: %+v %v", value, err)
		}
		if !bytes.Equal(stored[0].Revision.Bytes(), original.Results[0].Revision.Data) {
			t.Fatal("receipt revision did not match committed record")
		}
	})
}
