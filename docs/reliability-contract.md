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

The conformance harness starts the candidate executable with its public YAML
configuration, sends real gRPC requests with the public SDK, and uses actual
Elasticsearch/OpenSearch storage. A recording HTTP proxy holds a selected read
or commit at a named key/occurrence. Competing writes create real revision
conflicts. The proxy does not emulate a storage engine or fabricate success.
Queue metrics establish that callers overlap in the batching queue. Five-second
deadlines bound failed assertions; the main oracle is progress while a dependency
is deliberately held, not a microbenchmark timing threshold. Backend request
counts establish amplification and retry scope independently of machine speed.

## Independent operation model

`internal/statecheck` models Create, Upsert, Replace, Merge with both missing
document modes, Lua failure after local mutation, Read and duplicate Delete
using ordinary Go values. It covers all 36 pairs of write operations from both
absent and present states, then generates 256 seeded operations on three keys.
After every RPC it checks result order, exact permanent error classes, revision
presence and persisted state through reordered and repeated reads. It checks
that failed operations leave state intact and later operations still execute.

The same model runs through:

- one-operation RPCs, default microbatching, disabled microbatching and a
  three-operation batch boundary against Elasticsearch and OpenSearch;
- all seven configured stores in the full suite, including native BSON through
  MongoDB and JSON through both search engines.

Disabling microbatching does not disable within-RPC folding. One-operation RPCs
exercise the sequential comparison path. The independent model is the oracle
for all profiles; two modes agreeing with each other is not enough.

`SINK_STATE_SEED` and `SINK_STATE_STEPS` control the conformance sequence.
Nightly runs use the workflow run ID as the seed and 4096 operations. A failure
prints its seed and operation prefix so it can be replayed. Automatic shrinking,
coverage-guided server-process fuzzing and a linearizability checker for arbitrary
concurrent histories are still future work; this generator does not claim them.

## Run and prove the gates

```sh
SINK_SERVER_DIR=/path/to/sink make test-conformance
SINK_SERVER_DIR=/path/to/sink make test-regression-sensitivity
SINK_SERVER_DIR=/path/to/sink make test-production
```

The sensitivity gate first requires the current candidate to pass. It then
builds four immutable pre-fix commits in separate temporary directories and
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
unfinished package/test results. The conformance gate is part of integration,
production and sustained qualification. Server PR CI runs the pinned suite's
conformance gate against the candidate checkout; release and nightly workflow
pins must be updated together. Merge enforcement also depends on repository
rules requiring the CI status; adding a job does not itself change GitHub rules.

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
SQLite-level reliability. Remaining qualification gaps include process crashes
at every acknowledgement/commit boundary, reply loss after commit, arbitrary
network partitions, disk exhaustion, replica elections, bounded memory under
long slow-dependency saturation, compound recovery failures and restore tests.
The existing Kafka fault workload continues to check at-least-once delivery and
business reconciliation, but does not establish exactly-once delivery or a
general durable multi-node storage guarantee. Long runs need recorded successful
evidence; a short run never substitutes for the two-hour gate.
