//go:build integration

package conformance_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sink "github.com/liran/sink-go"
	"github.com/twmb/franz-go/pkg/kgo"
)

type storageFailure struct {
	name   string
	path   string
	mode   string
	status int
}

type storageFailureProxy struct {
	backend backend
	active  atomic.Pointer[storageFailure]
	seen    atomic.Int64
	mu      sync.Mutex
	events  []observedRequest
}

// Inject only failures or damaged responses; successful recovery always goes
// through the real backend. The write-block case records real storage errors.
func newStorageFailureProxy(t *testing.T, store backend) *storageFailureProxy {
	t.Helper()
	target, err := url.Parse(store.endpoint)
	if err != nil {
		t.Fatal(err)
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	fixture := &storageFailureProxy{backend: store}
	proxy.ModifyResponse = func(response *http.Response) error {
		fault := fixture.active.Load()
		if fault == nil || fault.mode != "observe" || response.Request.URL.Path != fault.path {
			return nil
		}
		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil {
			return err
		}
		response.Body = io.NopCloser(bytes.NewReader(body))
		if !bytes.Contains(body, []byte("cluster_block_exception")) {
			t.Errorf("real write block did not return its storage error: %s", body)
		} else {
			fixture.record(fault, response.StatusCode, body)
		}
		return nil
	}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Let the proxy transport decode compressed backend responses before
		// recording their real error payloads.
		r.Header.Del("Accept-Encoding")
		fault := fixture.active.Load()
		if fault == nil || fault.path != r.URL.Path || fault.mode == "observe" {
			proxy.ServeHTTP(w, r)
			return
		}
		request, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		r.Body.Close()
		status, body := storageFailureResponse(t, *fault, request)
		fixture.record(fault, status, body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write(body)
	})
	server := httptest.NewServer(handler)
	fixture.backend.endpoint = server.URL
	t.Cleanup(server.Close)
	t.Cleanup(func() {
		fixture.active.Store(nil)
		fixture.mu.Lock()
		defer fixture.mu.Unlock()
		if root := os.Getenv("SINK_CONFORMANCE_ARTIFACTS"); root != "" {
			payload, err := json.MarshalIndent(fixture.events, "", "  ")
			if err != nil {
				t.Error(err)
				return
			}
			name := fmt.Sprintf("storage-failures-%d.json", time.Now().UnixNano())
			if err := os.WriteFile(filepath.Join(root, name), payload, 0600); err != nil {
				t.Error(err)
			}
		}
	})
	return fixture
}

func (p *storageFailureProxy) record(fault *storageFailure, status int, body []byte) {
	p.mu.Lock()
	event := observedRequest{Time: time.Now().UTC(), Test: fault.name, Path: fault.path,
		Body: string(body), Phase: "storage-failure", Status: status}
	p.events = append(p.events, event)
	p.mu.Unlock()
	p.seen.Add(1)
}

func storageFailureResponse(t *testing.T, fault storageFailure, request []byte) (int, []byte) {
	t.Helper()
	if fault.mode == "http" {
		return fault.status, []byte(`{"error":{"type":"environment_failure","reason":"injected storage failure"}}`)
	}
	if fault.mode == "malformed" {
		return 200, []byte(`{"damaged":`)
	}
	if fault.mode == "partial" {
		return 200, []byte(`{"items":[]}`)
	}
	if fault.path == "/_mget" {
		var refs struct {
			Docs []json.RawMessage `json:"docs"`
		}
		if err := json.Unmarshal(request, &refs); err != nil {
			t.Error(err)
		}
		documents := make([]json.RawMessage, len(refs.Docs))
		for i := range documents {
			documents[i] = json.RawMessage(`{"error":{"type":"unknown_storage_error"}}`)
			if fault.mode == "missing-found" {
				documents[i] = json.RawMessage(`{}`)
			}
			if fault.mode == "missing-revision" {
				documents[i] = json.RawMessage(`{"found":true,"_source":{"counter":7}}`)
			}
		}
		body, err := json.Marshal(map[string]any{"docs": documents})
		if err != nil {
			t.Error(err)
		}
		return 200, body
	}
	lines := bytes.Split(bytes.TrimSpace(request), []byte{'\n'})
	items := make([]map[string]any, 0)
	for i := 0; i < len(lines); i++ {
		var actions map[string]json.RawMessage
		if err := json.Unmarshal(lines[i], &actions); err != nil {
			t.Error(err)
			break
		}
		for action := range actions {
			failure := map[string]any{"status": fault.status, "error": map[string]string{"type": "cluster_block_exception", "reason": "injected storage block"}}
			items = append(items, map[string]any{action: failure})
			if action != "delete" {
				i++
			}
		}
	}
	body, err := json.Marshal(map[string]any{"errors": true, "items": items})
	if err != nil {
		t.Error(err)
	}
	return 200, body
}

func TestWorkerRetainsStorageFailures(t *testing.T) {
	broker := startBroker(t)
	var failures []storageFailure
	for _, status := range []int{401, 403, 500, 400, 404, 408, 413, 429, 502, 503, 504} {
		failure := storageFailure{name: fmt.Sprintf("http-%d", status), mode: "http", path: "/_bulk", status: status}
		failures = append(failures, failure)
	}
	for _, status := range []int{200, 400, 403, 404, 409, 429, 500, 503} {
		failure := storageFailure{name: fmt.Sprintf("item-%d", status), mode: "item", path: "/_bulk", status: status}
		failures = append(failures, failure)
	}
	responseFailures := []storageFailure{
		{name: "malformed-bulk", mode: "malformed", path: "/_bulk"},
		{name: "partial-bulk", mode: "partial", path: "/_bulk"},
		{name: "mget-error", mode: "item", path: "/_mget"},
		{name: "mget-missing-found", mode: "missing-found", path: "/_mget"},
		{name: "mget-missing-revision", mode: "missing-revision", path: "/_mget"},
		{name: "real-write-block", mode: "observe", path: "/_bulk"},
	}
	failures = append(failures, responseFailures...)
	for _, store := range searchBackends(t) {
		t.Run(store.driver, func(t *testing.T) {
			index := indexFor(t, store, "-1")
			proxy := newStorageFailureProxy(t, store)
			topic := fmt.Sprintf("sink-storage-failures-%d", time.Now().UnixNano())
			opts := serverOptions{backend: store, broker: broker.address, topic: topic}
			publisher := startCandidate(t, opts)
			opts.backend, opts.worker = proxy.backend, true
			startCandidate(t, opts)
			var settled int64
			for _, fault := range failures {
				for _, action := range []string{"write", "delete"} {
					if fault.path == "/_mget" && action == "delete" {
						continue
					}
					passed := t.Run(fault.name+"/"+action, func(t *testing.T) {
						address := addressFor(t, index, fault.name+"-"+action)
						seed := put(t, address, `{"counter":7}`, sink.WriteUpsert)
						applied(t, writeAsync(t.Context(), publisher.client, sink.CompletionWaitUntilApplied, seed), 1)
						if fault.mode == "observe" {
							setIndexWriteBlock(t, store, index, true)
							defer setIndexWriteBlock(t, store, index, false)
						}
						proxy.seen.Store(0)
						proxy.active.Store(&fault)
						defer proxy.active.Store(nil)
						if action == "delete" {
							acceptedDelete(t, publisher.client, address)
						} else {
							op := put(t, address, `{"counter":1}`, sink.WriteUpsert)
							if fault.path == "/_mget" {
								op = merge(t, address, increment)
							}
							accepted(t, publisher.client, op)
						}
						accepted(t, publisher.client, put(t, address, `{"counter":2}`, sink.WriteUpsert))
						broker.assertEnd(t, topic, settled+2)
						// More than two default ten-attempt retry rounds must occur.
						// The conformance fixture uses 10..100ms backoff; sustained
						// qualification separately retains production retry timings.
						deadline := time.Now().Add(10 * time.Second)
						for proxy.seen.Load() < 22 {
							if end := broker.endOffset(t, topic+".dlq"); end != 0 {
								t.Fatalf("dependency failure moved records to DLQ: end=%d case=%s", end, t.Name())
							}
							if committed := broker.committed(t, topic); committed > settled {
								t.Fatalf("dependency failure acknowledged unfinished records: committed=%d settled=%d", committed, settled)
							}
							if time.Now().After(deadline) {
								t.Fatalf("dependency failure stopped being retried: attempts=%d", proxy.seen.Load())
							}
							time.Sleep(20 * time.Millisecond)
						}
						assertCounter(t, publisher.client, address, 7)
						broker.assertEnd(t, topic+".dlq", 0)
						if committed := broker.committed(t, topic); committed > settled {
							t.Fatalf("unresolved offset committed: %d", committed)
						}
						t.Logf("retained after %d backend failures; source prefix=%d", proxy.seen.Load(), settled)
						proxy.active.Store(nil)
						if fault.mode == "observe" {
							setIndexWriteBlock(t, store, index, false)
						}
						settled += 2
						broker.waitCommitted(t, topic, settled)
						assertCounter(t, publisher.client, address, 2)
						accepted(t, publisher.client, merge(t, address, increment))
						settled++
						broker.waitCommitted(t, topic, settled)
						assertCounter(t, publisher.client, address, 3)
						broker.assertEnd(t, topic+".dlq", 0)
					})
					if !passed {
						t.FailNow()
					}
				}
			}
			t.Run("permanent-record-only", func(t *testing.T) {
				address := addressFor(t, index, "invalid-document")
				// The index has a numeric counter mapping established by real writes.
				bad := put(t, address, `{"counter":{"invalid":true}}`, sink.WriteUpsert)
				accepted(t, publisher.client, bad)
				accepted(t, publisher.client, put(t, address, `{"counter":2}`, sink.WriteUpsert))
				broker.waitCommitted(t, topic, settled+2)
				assertCounter(t, publisher.client, address, 2)
				broker.assertEnd(t, topic+".dlq", 1)
				broker.assertQuarantinedSource(t, topic, settled)
			})
		})
	}
}

func acceptedDelete(t *testing.T, client *sink.Client, address sink.Address) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	results, err := client.Delete(ctx, sink.CompletionReturnAfterAccepted, address)
	if err != nil || len(results) != 1 || results[0].Status != sink.DeleteAccepted || results[0].Failure != nil {
		t.Fatalf("accept delete: %+v, %v", results, err)
	}
}

func setIndexWriteBlock(t *testing.T, store backend, index string, blocked bool) {
	t.Helper()
	call := httpCall{endpoint: store.endpoint, method: http.MethodPut, path: "/" + index + "/_settings",
		body: []byte(fmt.Sprintf(`{"index.blocks.write":%t}`, blocked))}
	code, body := request(t, call)
	if code != http.StatusOK {
		t.Fatalf("set real storage write block: HTTP %d %s", code, body)
	}
}

func (b *testBroker) endOffset(t *testing.T, topic string) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	offsets, err := b.admin.ListEndOffsets(ctx, topic)
	if err != nil || offsets.Error() != nil {
		t.Fatalf("inspect DLQ: %v, %v", err, offsets.Error())
	}
	offset, ok := offsets.Lookup(topic, 0)
	if !ok || len(offsets[topic]) != 1 {
		t.Fatalf("missing DLQ partition: %v", offsets)
	}
	return offset.Offset
}

func (b *testBroker) assertQuarantinedSource(t *testing.T, topic string, sourceOffset int64) {
	t.Helper()
	partitions := map[string]map[int32]kgo.Offset{topic + ".dlq": {0: kgo.NewOffset().AtStart()}}
	client, err := kgo.NewClient(kgo.SeedBrokers(b.address), kgo.ConsumePartitions(partitions))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	fetches := client.PollRecords(ctx, 1)
	records := fetches.Records()
	if len(fetches.Errors()) != 0 || len(records) != 1 {
		t.Fatalf("read quarantine evidence: %v, %v", records, fetches.Errors())
	}
	headers := make(map[string]string)
	for _, header := range records[0].Headers {
		headers[header.Key] = string(header.Value)
	}
	if headers["sink-source-topic"] != topic || headers["sink-source-offset"] != strconv.FormatInt(sourceOffset, 10) ||
		!strings.Contains(string(records[0].Value), "invalid") {
		t.Fatalf("wrong record quarantined: headers=%v", headers)
	}
}
