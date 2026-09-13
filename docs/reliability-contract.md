# Reliability contract and incident regressions

Sink server PRs 37, 38, 40 and 41 exposed gaps in qualification. PR 39 only
documented the write flow. Passing business Lua examples, eventual final-state
checks and long-running traffic did not establish bounded backend work,
independent request completion or per-caller resource ownership.

The old fixtures set search refresh to 100ms, frequently requested visible
completion, and allowed minute-scale reconciliation. That combination concealed
archive indexes with slow/disabled refresh and latency amplification between
otherwise independent operations. The model fuzzers compared business Lua with
business Go models; they never exercised Sink's batching state machine. Server
PR CI did not run the public suite; release qualification ran only afterwards.

## Executable incident matrix

The native-access extension also requires `TestReturnedChainReleasesIndependentPut`
against both search engines: a held Merge snapshot must not prevent an independent
Put from committing or releasing its key. Server-side real-Mongo tests also
cover atomic native revision updates and cursor cleanup after cancellation.
Sink does not provide write deduplication: application-owned idempotence remains
required for retries and repeated asynchronous delivery across every backend.

Pre-commit crash gates explicitly discard intercepted requests after killing the
candidate. They never rely on HTTP disconnect notification arriving before a
gate is released. `TestRequestGateDiscardPreventsLateForwarding` keeps the client
connection alive and requires zero backend requests, then verifies normal traffic
can resume. Both synchronous and Kafka crash tests use this boundary.

| Incident | Public contract and oracle | Test |
| --- | --- | --- |
| [PR 37](https://github.com/liran/sink/pull/37): hot-key work amplification | 64 ordered merges use one snapshot and one conditional commit, share a committed revision, and persist counter=64 | `TestHotKeyMergeAmplification` |
| [PR 38](https://github.com/liran/sink/pull/38): archive inherits visible wait | Applied Write and Delete finish with archive refresh disabled while unrelated visible operations retain actual search visibility | `TestAppliedDoesNotInheritVisibleRefresh` |
| [PR 40](https://github.com/liran/sink/pull/40): coalesced byte budgets | Two individually valid Read RPCs each retain their budget; a duplicated result within one RPC is still charged twice, with SDK retries disabled | `TestReadBudgetsBelongToOriginalRPC` |
| PR 40: formatted JSON damages NDJSON framing | Pretty-printed JSON, escaped newlines, quotes and Unicode persist correctly alongside another bulk item | `TestFormattedJSONBulkFraming` |
| PR 40: Replace conflict changes existence semantics | A real concurrent revision change is retried; a concurrent delete is not resurrected; an acknowledged sibling is written once | `TestReplaceRechecksExistenceAfterConflict` |
| [PR 41](https://github.com/liran/sink/pull/41): completed key held by unrelated read | A subsequent write to an already committed key completes while the original multi-operation RPC remains blocked on another key's read | `TestCompletedDocumentReleasedBeforeSiblingRead` |
| PR 41: completed RPC held by sibling conflict retry | A real conflicting writer changes the snapshot; successful RPC returns before retry is released, is never replayed, and retry recomputes the new value | `TestSuccessfulSiblingNotReplayedDuringConflict` |
| PR 41: independent dataset inherits refresh wait | Fast visible dataset is searchable before manually refreshing the slow dataset; slow visible RPC stays pending until refresh | `TestVisibleDatasetsCompleteIndependently` |
| Cancellation and admission regression class | Repeated cancellation while queued leaves no cancelled writes and does not poison following successful writes | `TestQueuedCancellationDoesNotPoisonFollowingWrites` |
| Process crashes at commit/acknowledgement boundaries | Pre-commit work stays absent; committed state survives restart; an explicitly idempotent retry applies once | `TestSyncCrashBoundaries` |
| Lost backend response after a non-idempotent commit | No false acknowledgement or internal replay; persisted counter and backend attempts stay one | `TestLostBackendResponseDoesNotReplayMutation` |
| Cancellation after commit | Committed state remains and record execution capacity is released | `TestCancellationAfterCommitRetainsState` |
| Worker crashes around backend/offset commits | Unresolved records replay, committed records do not replay, following records drain and ordinary DLQ stays empty | `TestAcceptedMutationCrashBoundaries` |
| Concurrent operations | Three clients through two processes have a legal sequential explanation preserving real-time precedence | `TestConcurrentHistories` |
| Slow store saturation | Excess work is rejected, healthy-store writes meet individual deadlines and cancellation releases reservations without applying pre-commit work | `TestSlowStoreSaturationIsBounded` |
| Storage failure misclassified as a bad record | Real Kafka records survive whole-request failures, per-item errors, malformed responses and real index write blocks; only a confirmed invalid document reaches DLQ | `TestWorkerRetainsStorageFailures` |

The conformance harness starts the candidate executable with its public YAML
configuration, sends real gRPC requests with the public SDK, and uses actual
Elasticsearch/OpenSearch storage. A recording HTTP proxy holds a selected read
or commit at a named key/occurrence. Competing writes create real revision
conflicts. The proxy does not emulate a storage engine or fabricate success.
Queue metrics establish that callers overlap in the batching queue. Five-second
deadlines bound failed assertions; the main oracle is progress while a dependency
is deliberately held, not a microbenchmark timing threshold. Backend request
counts establish amplification and retry scope independently of machine speed.

Response gates consume the actual backend reply before holding or dropping it;
the persisted document is independently read before a crash is injected. Child
logs are checked for races even after intentional SIGKILL. Kafka boundary tests
start a disposable Apache Kafka broker and inspect actual source/DLQ end offsets
and committed group offsets. They retain the production 45-second client session
timeout and 60-second rebalance window, allowing 75 seconds for replacement after
SIGKILL. A following mutation must drain after every recovery. A broker pause
also checks graceful shutdown, and the server's LeaveGroup response-loss
regression must fail without the shutdown deadline fix.

## Independent operation model

`internal/statecheck` models Create, Upsert, Replace, Merge with automatic
missing-document creation, Lua failure after local mutation, Read and duplicate
Delete using ordinary Go values. It covers all 25 pairs of write operations from
both absent and present states, then generates 256 seeded operations on three keys.
After every RPC it checks result order, exact permanent error classes, revision
presence and persisted state through reordered and repeated reads. It checks
that failed operations leave state intact and later operations still execute.

The same model runs through:

- one-operation RPCs, default microbatching, and one-operation and
  three-operation batch boundaries against Elasticsearch and OpenSearch;
- all seven configured stores in the full suite, including native BSON through
  MongoDB and JSON through both search engines.

One-operation batches retain queue admission and within-RPC folding.
One-operation RPCs exercise the sequential comparison path. The independent model is the oracle
for all profiles; two modes agreeing with each other is not enough.

`SINK_STATE_SEED` and `SINK_STATE_STEPS` control the conformance sequence.
Nightly runs use the workflow run ID as the seed and 4096 operations. A failure
prints its seed and operation prefix so it can be replayed.

## Concurrent histories and saturation

`internal/historycheck` independently models integer-document Read, Upsert,
Create, Replace, increment and Delete. It searches eligible orderings using
invocation/return intervals and memoized states. Definite retryable CAS exhaustion
is an aborted operation with no committed effect. Unknown results and transport
errors fail the workload; they are never removed from its history. Empty or
incomplete histories and exhausted search budgets fail closed. Deliberately
corrupted histories test lost updates, double application, duplicate successful
creates, stale reads, failed writes changing state and resurrection after Delete;
legal overlapping histories must pass.

The live workload checks ten-call histories from three clients and two server
processes, with default batches and batches restricted to one operation, on
both search engines. `SINK_STATE_SEED` controls generated operations;
`SINK_HISTORY_ROUNDS` defaults to 24 and nightly runs use 128. Failed histories
retain every invocation, response, client ID and operation as JSON. This is
bounded, single-record, single-operation-RPC linearizability checking; it does not
establish multi-record atomicity or check arbitrary uncertain, unbounded or async
histories. Automatic shrinking and coverage-guided server-process fuzzing remain
future work.

Saturation tests hold two real executions, fill an eight-call queue and require
all 56 excess calls to receive overload responses before cancellation. Eight
healthy-store writes must each complete within one second during saturation.
After cancellation, all execution and queue reservations return to zero and all
66 pre-commit/rejected documents remain absent. Quiescent Go heap growth is
limited to 64 MiB over baseline and goroutine growth to 80 for this fixture.
`SINK_SATURATION_ROUNDS` defaults to six per backend; nightly runs use 64. These
sampled thresholds are not strict Lua heap quotas or universal RSS guarantees.

## Storage failure classification

The public storage-failure matrix runs through the SDK, server, real Kafka,
worker and Elasticsearch/OpenSearch adapters. It covers HTTP 400/401/403/404,
408/413/429 and 500/502/503/504; per-item errors even under misleading 200/404/409
statuses; truncated/partial bulk replies; failed or incomplete snapshot replies;
and actual index write blocks. Each case exceeds two ten-attempt retry rounds
with the fixture's 10..100ms backoff. During failure the pre-existing document
must remain unchanged, source offsets must not advance, and DLQ must stay empty.
After recovery, ordered following writes and a fresh increment must reconcile.
A real mapping rejection separately proves that one bad document is quarantined
with the correct source offset and its valid same-record successor completes.

The worker retains unknown/missing/internal failure details. Search adapters
classify whole-request failures as dependency failures and only quarantine
explicit per-document mapping rejections. A retryable classification means keep
the queued work; authentication and write blocks may require operator repair.
MongoDB driver-level tests also distinguish environment errors and write-concern
uncertainty from explicit document rejection, and reject invalid bulk-error
indexes. This matrix qualifies Sink's reactions to storage failures; it does not
qualify the database's own durability or recovery implementation.

## Native access qualification (Sink PR #44)

The suite pins its paired SDK in `go.mod` and exercises public RPCs
against the candidate executable. The new contracts are required by the same
event checker as the incident regressions; missing and skipped tests fail.

| Contract | Independent oracle |
| --- | --- |
| Query | Reverse-inserted known records, pages of 1/4/5/1000, exact and partial last pages, an extra empty page, ascending/descending ordering and include/exclude projections. Native pagination and presentation overrides are checked. |
| Count | Known totals before pagination; MongoDB metadata estimates versus exact filtered/hinted/pipeline results; empty results; HTTP counts ignore collapse, aggregation and approximate-total requests. |
| Scan | Every seeded record appears once across several batch sizes; native JSON hit identity and BSON fields survive; callback errors are preserved and already canceled contexts invoke no callbacks. |
| Cursor ownership | A real Scan page response is held while the caller cancels or the per-request deadline expires. Per-index open contexts reach zero, and the sole admission slot handles the next request while the gate remains held. No backend session is retained between pages. |
| Incomplete backend results | A proxy damages real pages with timeout, shard failure, missing timeout/shard metadata, inconsistent shard counts, missing hits, malformed JSON or approximate totals. Query/Count/Scan fail without exposing documents, a total or a continuation cursor. No automatic retry is allowed and healthy requests recover. |
| Native mutations | Execute changes a real document and retains native error payloads/status. Losing the actual increment response leaves exactly one increment and one backend attempt. |
| Validation and limits | Raw gRPC bypasses SDK validation for managed query parameters, duplicate sorting, oversized pages/batches and asynchronous returned writes. Backend traces must contain no data requests. Oversized native responses fail without truncated output. |
| Returned documents | Mixed returned/non-returned chains and concurrent writes through two servers return their own values and distinct revisions. Real CAS conflicts recompute against the competing writer. Failed Create returns no document. A shared response budget rejects the second commit; two coalesced RPCs retain independent budgets. |
| Native MongoDB revision protection | Native writes change the revision observed through a second server. Unknown/destructive commands are rejected before execution; supported commands still retain native database errors. The server's MongoDB integration tests additionally hold a Merge snapshot while another service instance commits operator, replacement, or pipeline writes, then require a fresh snapshot and the combined committed value. |
| Live Scan checkpoints | With both sort directions on all seven stores, delete the next unseen record, update another unseen record and insert on each side of the checkpoint. Alternate server replicas and page sizes, then replay the original checkpoint. An independent expected sequence detects skips, repeats and stale documents. Cancellation and a corrupt token must not damage the valid token. |
| Cross-cluster completeness | A proxy adds a skipped remote cluster to a real successful response. Query/Count/Scan must reject it even when every reported shard succeeded. This validates response handling, not a real cross-cluster network partition. |
| MongoDB partial writes and budgets | An unordered native insert with a duplicate key returns a NativeError while its valid sibling remains readable. Query refuses `allowPartialResults: true`. Server integration additionally checks ordered batches, an actual write-concern timeout, and small Query pages followed by an oversized lookahead document for both find and aggregate. |

The backend contracts cover all seven configured stores. Deterministic HTTP
faults, scan deadline/cursor observations, and constrained-budget cases cover
both Elasticsearch and OpenSearch; they do not establish MongoDB cursor cleanup
under network faults. Paged Scan does not open a search scroll or PIT. The
oversized Scan test requires an error without documents or a continuation cursor.
Query pagination checks use quiescent fixtures; live Scan changes are checked
against seek semantics and do not claim a snapshot across concurrent writes.
Test datasets are synthetic and live
only in the disposable qualification environment.

## Sustained fault qualification

`make test-reliability` requires a full two-hour workload with 16 clients and at
least 1,000 completed business cycles. Twelve fault cycles repeatedly kill the
worker, stop OpenSearch for 45 seconds and restart Kafka; every third cycle also
pauses Kafka during the storage outage. Cycles are separated by five minutes.
The workload must remain alive throughout every fault cycle, followed by business
reconciliation, drained groups, empty ordinary DLQs and explicit DLQ recovery.
This long run is reserved for scheduled or explicitly requested qualification.
Routine changes and releases use the shorter single-cycle production gate and
do not wait for a two-hour run. A short run is not evidence of sustained testing.

Stateful fuzzing has a separate evidence check: Go's PASS is insufficient if the
time limit expires during cached corpus replay. `scripts/test-fuzz.sh` requires
mutation-phase progress and retains each log. CI and release runs allocate three
minutes per fuzzer; an unusually large corpus requires increasing `FUZZ_TIME`.

## Run and prove the gates

```sh
SINK_SERVER_DIR=/path/to/sink make test-conformance
SINK_SERVER_DIR=/path/to/sink make test-regression-sensitivity
SINK_SERVER_DIR=/path/to/sink make test-production
```

The sensitivity gate first requires the current candidate to pass. It then
builds seven immutable pre-fix commits in separate temporary directories and
requires the corresponding incident assertion to fail on Elasticsearch. A
compilation error, missing dependency, skip or arbitrary nonzero exit is rejected
as proof. This checks the tests themselves and runs on every suite PR.

The conformance runner builds both server and tests with the race detector,
uses dynamically allocated loopback backend ports and its own Compose project,
and retains JSON test events, HTTP request traces, exact revisions, local tracked
diffs, generated server configurations, server logs and backend logs. The
runner removes only its own disposable Compose resources. Review local untracked
source separately before calling a run reproducible from a commit.

`check-test-events` rejects missing required tests, empty runs, skips and
unfinished package/test results in every integration phase, including recovery,
load, soak and DLQ recovery. The conformance gate is part of integration,
production and sustained qualification. Server PR CI runs the pinned suite's
conformance gate against the candidate checkout; release and nightly workflow
pins must be updated together. `Sink reliability gate` and `Suite reliability
gate` run even when prerequisites fail or are skipped and require every
prerequisite to pass. GitHub rules must require these statuses; adding a job does
not itself change repository rules. Release binary/image publication requires
the shorter public production qualification, independently of scheduled or
explicitly requested sustained testing.

To qualify another deployed OpenSearch version with the same assertions:

```sh
SINK_CONFORMANCE_OPENSEARCH_IMAGE=opensearchproject/opensearch:2.17.0 \
  SINK_SERVER_DIR=/path/to/sink make test-conformance
```

## Change acceptance and remaining work

Every correctness or isolation fix needs a public invariant, a deterministic
regression, and evidence that the regression detects the broken behavior. A
performance change also needs backend work bounds and state equivalence. New
completion modes, resource limits, backend versions and operation combinations
must extend the matrix. Keep new minimized failure sequences as permanent tests.

SQLite's [testing approach](https://sqlite.org/testing.html) combines independent
harnesses, anomaly tests, fuzzing, optimization comparisons and test sensitivity.
These changes adopt those practices for the incidents above; they do not certify
SQLite-level reliability. Remaining Sink-specific gaps include every individual
Kafka acknowledgement boundary, generated malformed protocol responses, arbitrary
network partitions, strict memory isolation for arbitrary scripts and broader
combinations of dependency failures with rebalance and mixed permanent/transient
records. Database elections, disk repair and backup implementation are outside
Sink's responsibility; their observable failures should be translated into Sink
contract tests, not database certification requirements.
The existing Kafka fault workload continues to check at-least-once delivery and
business reconciliation, but does not establish exactly-once delivery or a
general durable multi-node storage guarantee. Long runs need recorded successful
evidence; a short run cannot establish the results of sustained testing.
