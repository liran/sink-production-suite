//go:build integration

package conformance_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	sink "github.com/liran/sink-go"
)

func TestNativeScanResumesAfterServerExit(t *testing.T) {
	for _, store := range searchBackends(t) {
		for _, crash := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/crash=%t", store.driver, crash), func(t *testing.T) {
				index := indexFor(t, store, "100ms")
				proxy := proxyBackend(t, store)
				opts := serverOptions{backend: proxy.backend}
				first := startCandidate(t, opts)
				for i := range 4 {
					address := addressFor(t, index, fmt.Sprint(i))
					operation := put(t, address, fmt.Sprintf(`{"counter":%d}`, i), sink.WriteCreate)
					applied(t, writeAsync(t.Context(), first.client, sink.CompletionWaitUntilVisible, operation), 1)
				}
				command := nativeSearch(index)
				req := sink.ScanRequest{Command: command, BatchSize: 1}
				page, err := first.client.Scan(t.Context(), req)
				if err != nil || len(page.Documents) != 1 || len(page.NextCursor) == 0 {
					t.Fatalf("first=%+v err=%v", page, err)
				}
				req.Cursor = page.NextCursor
				// The backend has already answered, but this page never reaches the SDK.
				gate := proxy.holdResponse(command.Path, "search_after", true)
				t.Cleanup(gate.open)
				done := make(chan error, 1)
				go func() { _, err := first.client.Scan(t.Context(), req); done <- err }()
				gate.wait(t)
				if crash {
					first.crash(t)
				} else {
					first.stop(t)
				}
				gate.open()
				select {
				case err := <-done:
					if err == nil {
						t.Fatal("lost response reported success")
					}
				case <-time.After(5 * time.Second):
					t.Fatal("old request did not terminate")
				}
				second := startCandidate(t, opts)
				want := 1
				for {
					page, err := second.client.Scan(t.Context(), req)
					if err != nil {
						t.Fatal(err)
					}
					for _, document := range page.Documents {
						var hit struct {
							Source struct {
								Counter int `json:"counter"`
							} `json:"_source"`
						}
						if err := json.Unmarshal(document.Payload(), &hit); err != nil {
							t.Fatal(err)
						}
						if hit.Source.Counter != want {
							t.Fatalf("lost or repeated page: got=%d want=%d", hit.Source.Counter, want)
						}
						want++
					}
					if len(page.NextCursor) == 0 {
						break
					}
					req.Cursor = page.NextCursor
				}
				if want != 4 {
					t.Fatalf("scan stopped early at %d", want)
				}
				assertNoSearchCursors(t, store, index)
			})
		}
	}
}
