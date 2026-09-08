//go:build integration

package conformance_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	sink "github.com/liran/sink-go"
	"google.golang.org/grpc/credentials/insecure"
)

// This harness starts the candidate executable and uses only public RPCs and
// real backend HTTP APIs. Gates delay network requests; they do not implement
// storage, fabricate successful responses, or import server internals.
type backend struct {
	driver   string
	endpoint string
}

func searchBackends(t *testing.T) []backend {
	t.Helper()
	var backends []backend
	for _, driver := range []string{"elasticsearch", "opensearch"} {
		endpoint := os.Getenv("SINK_CONFORMANCE_" + strings.ToUpper(driver))
		if endpoint == "" {
			t.Fatalf("SINK_CONFORMANCE_%s is required; use make test-conformance", strings.ToUpper(driver))
		}
		entry := backend{driver: driver, endpoint: endpoint}
		backends = append(backends, entry)
	}
	return backends
}

type serverOptions struct {
	backend   backend
	unbatched bool
	batchOps  int
	batchWait int
	readBytes int
	worker    bool
	broker    string
	topic     string
	capacity  int
	queued    int
	maxOps    int
	secondary *backend
}

type candidate struct {
	client  *sink.Client
	metrics string
	command *exec.Cmd
	done    <-chan error
	stopped bool
}

func startCandidate(t *testing.T, opts serverOptions) *candidate {
	t.Helper()
	binary := os.Getenv("SINK_SERVER_BINARY")
	if binary == "" {
		t.Fatal("SINK_SERVER_BINARY is required; use make test-conformance")
	}
	grpcAddress := freeAddress(t)
	metricsAddress := freeAddress(t)
	dir := t.TempDir()
	if root := os.Getenv("SINK_CONFORMANCE_ARTIFACTS"); root != "" {
		var err error
		dir, err = os.MkdirTemp(root, "candidate-")
		if err != nil {
			t.Fatal(err)
		}
	}
	mode := "server"
	if opts.worker {
		mode = "worker"
	}
	config := fmt.Sprintf(`mode: %s
grpc:
  address: %q
prometheus:
  address: %q
storages:
  - name: primary
    driver: %s
    search:
      endpoints: [%q]
%s
%s
service:
  request_timeout_seconds: 20
  max_read_bytes: %d
  max_operations: %d
  max_merge_attempts: 50
  max_in_flight_requests: %d
  max_store_requests: %d
  batching:
    enabled: %t
    max_operations: %d
    max_wait_milliseconds: %d
    max_queued_operations: %d
shutdown_timeout_seconds: 2
`, mode, grpcAddress, metricsAddress, opts.backend.driver, opts.backend.endpoint,
		candidateKafkaConfig(opts), candidateSecondaryConfig(opts), defaultInt(opts.readBytes, 32<<20), defaultInt(opts.maxOps, 1000),
		defaultInt(opts.capacity*2, 128), defaultInt(opts.capacity, 32), !opts.unbatched,
		defaultInt(opts.batchOps, 1000), defaultInt(opts.batchWait, 2), defaultInt(opts.queued, 10000))
	configPath := filepath.Join(dir, "server.yaml")
	if err := os.WriteFile(filepath.Join(dir, "test-name.txt"), []byte(t.Name()), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(dir, "server.log")
	log, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(binary, "--config", configPath)
	command.Stdout, command.Stderr = log, log
	if err := command.Start(); err != nil {
		log.Close()
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	server := &candidate{metrics: "http://" + metricsAddress + "/metrics", command: command, done: done}
	t.Cleanup(func() {
		if !server.stopped {
			_ = command.Process.Signal(os.Interrupt)
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("candidate exited unsuccessfully (including race detector failures): %v", err)
				}
			case <-time.After(5 * time.Second):
				_ = command.Process.Kill()
				<-done
				t.Error("candidate did not shut down within five seconds")
			}
		}
		log.Close()
		contents, err := os.ReadFile(logPath)
		if err != nil {
			t.Error(err)
		}
		if bytes.Contains(contents, []byte("WARNING: DATA RACE")) {
			t.Error("candidate reported a data race, including before an intentional crash")
		}
		if t.Failed() {
			t.Logf("candidate log (%s):\n%s", logPath, contents)
		}
	})
	if opts.worker {
		server.waitReady(t)
		return server
	}
	retry := sink.RetryPolicy{MaxAttempts: 1}
	clientOptions := sink.ClientOptions{ReadRetry: retry}
	dialOptions := sink.DialOptions{TransportCredentials: insecure.NewCredentials(), Client: clientOptions}
	client, err := sink.Dial(grpcAddress, dialOptions)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	for {
		attempt, stop := context.WithTimeout(ctx, 200*time.Millisecond)
		err = client.CheckHealth(attempt)
		stop()
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("candidate did not become ready: %v", err)
		}
		select {
		case <-ctx.Done():
		case <-time.After(10 * time.Millisecond):
		}
	}
	t.Logf("candidate config and log: %s", dir)
	server.client = client
	server.waitReady(t)
	return server
}

func (c *candidate) waitReady(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	endpoint := strings.TrimSuffix(c.metrics, "/metrics") + "/readyz"
	var last string
	for ctx.Err() == nil {
		attempt, stop := context.WithTimeout(ctx, time.Second)
		req, err := http.NewRequestWithContext(attempt, http.MethodGet, endpoint, nil)
		if err != nil {
			stop()
			t.Fatal(err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			body, readErr := io.ReadAll(resp.Body)
			resp.Body.Close()
			last = fmt.Sprintf("HTTP %d %s %v", resp.StatusCode, body, readErr)
			if resp.StatusCode == http.StatusOK && readErr == nil {
				stop()
				return
			}
		} else {
			last = err.Error()
		}
		stop()
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("candidate dependencies did not become ready: %s", last)
}

func candidateKafkaConfig(opts serverOptions) string {
	if opts.broker == "" {
		return ""
	}
	return fmt.Sprintf(`    kafka:
      enabled: true
      brokers: [%q]
      topic: %s
      group_id: %s-workers
      dead_letter_topic: %s.dlq
      topic_partitions: 1
      topic_replication_factor: 1
      retry_backoff_milliseconds: 10
      max_retry_backoff_milliseconds: 100
`, opts.broker, opts.topic, opts.topic, opts.topic)
}

func candidateSecondaryConfig(opts serverOptions) string {
	if opts.secondary == nil {
		return ""
	}
	return fmt.Sprintf(`  - name: secondary
    driver: %s
    search:
      endpoints: [%q]
`, opts.secondary.driver, opts.secondary.endpoint)
}

func (c *candidate) crash(t *testing.T) {
	t.Helper()
	if c.stopped {
		t.Fatal("candidate was already stopped")
	}
	if err := c.command.Process.Kill(); err != nil {
		t.Fatalf("inject SIGKILL: %v", err)
	}
	select {
	case err := <-c.done:
		c.stopped = true
		var exit *exec.ExitError
		if !errors.As(err, &exit) || exit.ProcessState.String() != "signal: killed" {
			t.Fatalf("candidate did not exit from the injected SIGKILL: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("candidate did not terminate after SIGKILL")
	}
	t.Log("injected candidate SIGKILL")
}

func (c *candidate) stop(t *testing.T) {
	t.Helper()
	if err := c.command.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-c.done:
		c.stopped = true
		if err != nil {
			t.Fatalf("candidate graceful shutdown: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("candidate graceful shutdown exceeded five seconds")
	}
}

func defaultInt(value, fallback int) int {
	if value == 0 {
		return fallback
	}
	return value
}

func freeAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

type httpCall struct {
	endpoint string
	method   string
	path     string
	body     []byte
}

func request(t *testing.T, call httpCall) (int, []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, call.method, call.endpoint+call.path, bytes.NewReader(call.body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, body
}

func indexFor(t *testing.T, store backend, refresh string) string {
	t.Helper()
	index := fmt.Sprintf("sink-conformance-%d", time.Now().UnixNano())
	call := httpCall{endpoint: store.endpoint, method: http.MethodPut, path: "/" + index,
		body: []byte(fmt.Sprintf(`{"settings":{"number_of_shards":1,"number_of_replicas":0,"refresh_interval":%q}}`, refresh))}
	code, body := request(t, call)
	if code != http.StatusOK {
		t.Fatalf("create index: %d %s", code, body)
	}
	t.Cleanup(func() {
		call.method, call.body = http.MethodDelete, nil
		// testing cancels t.Context before cleanup callbacks run.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, call.method, call.endpoint+call.path, nil)
		if err != nil {
			t.Error(err)
			return
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Error(err)
			return
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("delete test index: HTTP %d", resp.StatusCode)
		}
	})
	return index
}

func addressFor(t *testing.T, index, key string) sink.Address {
	t.Helper()
	address, err := sink.NewAddress("primary", "catalog", index, sink.StringKey(key))
	if err != nil {
		t.Fatal(err)
	}
	return address
}

func put(t *testing.T, address sink.Address, raw string, mode sink.WriteMode) sink.WriteOperation {
	t.Helper()
	document, err := sink.NewRawDocument(sink.DocumentEncodingJSON, []byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	operation, err := sink.NewPut(address, document, mode)
	if err != nil {
		t.Fatal(err)
	}
	return operation
}

const increment = `return function(current, incoming)
  current = current or {}
  current.counter = (current.counter or 0) + incoming.counter
  return current
end`

func merge(t *testing.T, address sink.Address, source string) sink.WriteOperation {
	t.Helper()
	program, err := sink.NewLuaProgram([]byte(source))
	if err != nil {
		t.Fatal(err)
	}
	document, err := sink.NewRawDocument(sink.DocumentEncodingJSON, []byte(`{"counter":1}`))
	if err != nil {
		t.Fatal(err)
	}
	opts := sink.MergeOptions{Incoming: document, Program: program, MissingDocumentMode: sink.MissingDocumentCreate}
	operation, err := sink.NewMerge(address, opts)
	if err != nil {
		t.Fatal(err)
	}
	return operation
}

type writeOutcome struct {
	results []sink.WriteResult
	err     error
}

func writeAsync(ctx context.Context, client *sink.Client, mode sink.CompletionMode, operations ...sink.WriteOperation) <-chan writeOutcome {
	done := make(chan writeOutcome, 1)
	go func() {
		results, err := client.Write(ctx, mode, operations...)
		outcome := writeOutcome{results: results, err: err}
		done <- outcome
	}()
	return done
}

func applied(t *testing.T, done <-chan writeOutcome, count int) []sink.WriteResult {
	t.Helper()
	select {
	case result := <-done:
		if result.err != nil || len(result.results) != count {
			t.Fatalf("write: %v, %+v", result.err, result.results)
		}
		for i, operation := range result.results {
			if operation.OperationIndex != i || operation.Status != sink.WriteApplied || operation.Failure != nil || len(operation.Revision.Bytes()) == 0 {
				t.Fatalf("write result[%d]: %+v", i, operation)
			}
		}
		return result.results
	case <-time.After(5 * time.Second):
		t.Fatal("independent operation did not complete while the unrelated dependency was held")
		return nil
	}
}

func (c *candidate) waitQueued(t *testing.T, method string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		call := httpCall{endpoint: c.metrics, method: http.MethodGet}
		_, body := request(t, call)
		for _, line := range strings.Split(string(body), "\n") {
			prefix := `sink_batcher_queued_operations{method="` + method + `"} `
			if strings.HasPrefix(line, prefix) {
				value, err := strconv.Atoi(strings.TrimPrefix(line, prefix))
				if err == nil && value >= 1 {
					return
				}
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("first RPC was not observed in the coalescing queue; schedule was not exercised")
}

type requestGate struct {
	path     string
	key      string
	nth      int
	seen     int
	entered  chan struct{}
	release  chan struct{}
	once     sync.Once
	response bool
	drop     bool
}

func (g *requestGate) open() { g.once.Do(func() { close(g.release) }) }

type observedRequest struct {
	Time   time.Time
	Test   string
	Path   string
	Body   string
	Phase  string
	Status int
}

type backendProxy struct {
	backend  backend
	mu       sync.Mutex
	gates    []*requestGate
	requests []observedRequest
}

func proxyBackend(t *testing.T, store backend) *backendProxy {
	t.Helper()
	target, err := url.Parse(store.endpoint)
	if err != nil {
		t.Fatal(err)
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	observed := &backendProxy{}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		r.Body.Close()
		r.Body = io.NopCloser(bytes.NewReader(body))
		observed.mu.Lock()
		event := observedRequest{Time: time.Now().UTC(), Test: t.Name(), Path: r.URL.RequestURI(), Body: string(body)}
		observed.requests = append(observed.requests, event)
		var held []*requestGate
		for _, gate := range observed.gates {
			if !gate.response && r.URL.Path == gate.path && bytes.Contains(body, []byte(strconv.Quote(gate.key))) {
				gate.seen++
				if gate.seen == gate.nth {
					held = append(held, gate)
					close(gate.entered)
				}
			}
		}
		observed.mu.Unlock()
		for _, gate := range held {
			select {
			case <-gate.release:
			case <-r.Context().Done():
				return
			}
		}
		// Attach the request bytes so a response gate can select the same key
		// after the real backend has completed the request.
		requestKey := responseRequestKey{}
		r = r.WithContext(context.WithValue(r.Context(), requestKey, body))
		proxy.ServeHTTP(w, r)
	})
	proxy.ModifyResponse = func(response *http.Response) error {
		requestKey := responseRequestKey{}
		body, _ := response.Request.Context().Value(requestKey).([]byte)
		observed.mu.Lock()
		var held []*requestGate
		for _, gate := range observed.gates {
			if gate.response && response.Request.URL.Path == gate.path && bytes.Contains(body, []byte(strconv.Quote(gate.key))) {
				gate.seen++
				if gate.seen == gate.nth {
					held = append(held, gate)
				}
			}
		}
		observed.mu.Unlock()
		if len(held) == 0 {
			return nil
		}
		payload, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil {
			return err
		}
		response.Body = io.NopCloser(bytes.NewReader(payload))
		observed.mu.Lock()
		event := observedRequest{Time: time.Now().UTC(), Test: t.Name(), Path: response.Request.URL.RequestURI(),
			Body: string(payload), Phase: "backend-response", Status: response.StatusCode}
		observed.requests = append(observed.requests, event)
		observed.mu.Unlock()
		for _, gate := range held {
			close(gate.entered)
			select {
			case <-gate.release:
			case <-response.Request.Context().Done():
				return response.Request.Context().Err()
			}
			if gate.drop {
				return errDropResponse
			}
		}
		return nil
	}
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		if errors.Is(err, errDropResponse) {
			connection, _, hijackErr := w.(http.Hijacker).Hijack()
			if hijackErr != nil {
				t.Errorf("drop backend response: %v", hijackErr)
				return
			}
			connection.Close()
			return
		}
		http.Error(w, err.Error(), http.StatusBadGateway)
	}
	server := httptest.NewServer(handler)
	observed.backend = store
	observed.backend.endpoint = server.URL
	t.Cleanup(server.Close)
	t.Cleanup(func() {
		observed.mu.Lock()
		defer observed.mu.Unlock()
		for _, gate := range observed.gates {
			gate.open()
		}
		if root := os.Getenv("SINK_CONFORMANCE_ARTIFACTS"); root != "" {
			file, err := os.CreateTemp(root, "http-trace-*.json")
			if err != nil {
				t.Error(err)
				return
			}
			defer file.Close()
			if err := json.NewEncoder(file).Encode(observed.requests); err != nil {
				t.Error(err)
			}
		}
	})
	return observed
}

type responseRequestKey struct{}

var errDropResponse = errors.New("injected response loss after backend completion")

func (p *backendProxy) holdResponse(path, key string, drop bool) *requestGate {
	gate := &requestGate{path: path, key: key, nth: 1, response: true, drop: drop,
		entered: make(chan struct{}), release: make(chan struct{})}
	p.mu.Lock()
	p.gates = append(p.gates, gate)
	p.mu.Unlock()
	return gate
}

func (p *backendProxy) hold(path, key string, nth int) *requestGate {
	gate := &requestGate{path: path, key: key, nth: nth, entered: make(chan struct{}), release: make(chan struct{})}
	p.mu.Lock()
	p.gates = append(p.gates, gate)
	p.mu.Unlock()
	return gate
}

func (g *requestGate) wait(t *testing.T) {
	t.Helper()
	select {
	case <-g.entered:
	case <-time.After(5 * time.Second):
		t.Fatalf("backend gate %s/%s occurrence %d was not reached", g.path, g.key, g.nth)
	}
}

func (p *backendProxy) count(path, key string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	count := 0
	for _, req := range p.requests {
		if req.Phase == "" && strings.Split(req.Path, "?")[0] == path {
			count += strings.Count(req.Body, `"_id":`+strconv.Quote(key))
		}
	}
	return count
}
