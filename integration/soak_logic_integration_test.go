//go:build integration

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	sink "github.com/liran/sink-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRetryableSoakRPCError(t *testing.T) {
	testCases := []struct {
		name string
		err  error
		want bool
	}{
		{name: "unavailable", err: status.Error(codes.Unavailable, "graceful stop"), want: true},
		{name: "wrapped unavailable", err: fmt.Errorf("write records: %w", status.Error(codes.Unavailable, "goaway")), want: true},
		{name: "deadline", err: context.DeadlineExceeded, want: true},
		{name: "resource exhausted", err: status.Error(codes.ResourceExhausted, "queue full"), want: true},
		{name: "invalid argument", err: status.Error(codes.InvalidArgument, "invalid"), want: false},
		{name: "plain error", err: errors.New("plain"), want: false},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := isRetryableSoakRPCError(testCase.err); got != testCase.want {
				t.Fatalf("isRetryableSoakRPCError() = %t, want %t", got, testCase.want)
			}
		})
	}
}

func TestRetryableSoakReadError(t *testing.T) {
	testCases := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "retryable operation failure",
			err:  &sink.OperationError{Code: sink.FailureUnavailable, Retryable: true},
			want: true,
		},
		{
			name: "permanent operation failure",
			err:  &sink.OperationError{Code: sink.FailureInvalidArgument, Retryable: false},
			want: false,
		},
		{name: "rpc unavailable", err: status.Error(codes.Unavailable, "backend restart"), want: true},
		{name: "plain error", err: errors.New("decode failure"), want: false},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := isRetryableSoakReadError(testCase.err); got != testCase.want {
				t.Fatalf("isRetryableSoakReadError() = %t, want %t", got, testCase.want)
			}
		})
	}
}

func TestSoakObservedDocumentMatches(t *testing.T) {
	absent := soakDocumentExpectation{wantFound: false}
	created := soakDocumentExpectation{
		wantFound:       true,
		wantValue:       "record",
		wantCounter:     0,
		wantOperationID: "record:create",
	}
	merged := soakDocumentExpectation{
		wantFound:       true,
		wantValue:       "record",
		wantCounter:     1,
		wantOperationID: "record:merge",
	}
	createdDocument := soakDocument{
		Value:       "record",
		Counter:     0,
		OperationID: "record:create",
	}
	mergedDocument := soakDocument{
		Value:       "record",
		Counter:     1,
		OperationID: "record:merge",
	}
	testCases := []struct {
		name        string
		observed    soakObservedDocument
		expectation soakDocumentExpectation
		want        bool
	}{
		{name: "absent", observed: soakObservedDocument{}, expectation: absent, want: true},
		{name: "created", observed: soakObservedDocument{found: true, document: createdDocument}, expectation: created, want: true},
		{name: "merged", observed: soakObservedDocument{found: true, document: mergedDocument}, expectation: merged, want: true},
		{name: "counter mismatch", observed: soakObservedDocument{found: true, document: mergedDocument}, expectation: created, want: false},
		{name: "presence mismatch", observed: soakObservedDocument{found: true, document: createdDocument}, expectation: absent, want: false},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := testCase.observed.matches(testCase.expectation); got != testCase.want {
				t.Fatalf("matches() = %t, want %t", got, testCase.want)
			}
		})
	}
}
