//go:build integration

package integration_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	sink "github.com/liran/sink-go"
	"github.com/liran/sink-production-suite/internal/statecheck"
	"google.golang.org/grpc/credentials/insecure"
)

func TestBackendOperationStateMachine(t *testing.T) {
	stores := configuredBackendStores(t)
	if len(stores) == 0 {
		t.Fatal("SINK_BACKEND_STORES is required for the state machine matrix")
	}
	retry := sink.RetryPolicy{MaxAttempts: 1}
	clientOptions := sink.ClientOptions{ReadRetry: retry}
	dialOptions := sink.DialOptions{TransportCredentials: insecure.NewCredentials(), Client: clientOptions}
	client, err := sink.Dial(environmentValue("SINK_ADDRESS", defaultSinkAddress), dialOptions)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	for _, store := range stores {
		t.Run(store.name, func(t *testing.T) {
			encoding := sink.DocumentEncodingJSON
			if strings.HasPrefix(store.name, "mongodb-") {
				encoding = sink.DocumentEncodingBSON
			}
			// The Compose project owns these namespaces and removes them on exit.
			check := statecheck.Options{Client: client, Store: store.name,
				Dataset: fmt.Sprintf("sink-model-%d", time.Now().UnixNano()), Encoding: encoding,
				BatchSize: 16, Seed: 41, Steps: 256}
			statecheck.Run(t, check)
		})
	}
}
