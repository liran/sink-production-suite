# Sink production qualification suite

This public repository qualifies [Sink](https://github.com/liran/sink) against
a representative commerce-indexing workload using only synthetic fixtures,
public dependencies, and disposable local infrastructure. It does not import
proprietary application packages, use production data, require cloud
credentials, or connect to an external Kubernetes cluster.

## Release gates

Production incidents from Sink PRs 37 through 41 are now executable public-API
contracts. The [incident matrix and reliability contract](docs/reliability-contract.md)
explain each missed invariant, its deterministic oracle, historical pre-fix
failure proof, configuration/model matrix and remaining qualification gaps.
Integration, release and sustained runs start with `make test-conformance`;
suite PRs also prove that the tests reject seven historical broken candidates.

The suite verifies:

1. Representative item and offer merge programs match the public Go reference
   model, including history limits, deduplication, timestamps, large integers,
   and replay behavior.
2. The same programs produce equal documents through the public Go client,
   multiple Sink server processes, and real storage backends, using explicit
   JSON documents for search and BSON documents for MongoDB.
3. Concurrent merges preserve every successful update while exercising real
   search-engine revision conflicts.
4. Store-owned asynchronous routing works through two independent Kafka
   clusters without changing result order.
5. Stores without Kafka remain available synchronously and reject asynchronous
   requests as retryable unavailable results without publishing anything.
6. MongoDB, Elasticsearch, and OpenSearch pass create, duplicate-create,
   concurrent merge, asynchronous write/delete, and synchronous delete checks,
   including distinct `json`/`bson` identity tags and native datetime types.
7. Accepted Kafka mutations survive worker and broker restarts and retain
   same-record ordering.
8. Active operations recover from a worker SIGKILL, a 45-second OpenSearch outage,
   and a Kafka restart using the product worker retry default. Healthy stores
   remain ready while the affected dependency fails readiness.
9. A representative concurrent load completes without failed operations or
   exhausted merge-conflict retries.
10. Every consumer group drains to zero lag and dead-letter topics remain empty
    for ordinary traffic and temporary outages.
11. The public API rejects oversized asynchronous mutations permanently, counts
    repeated read keys across stores against one output budget, and rejects
    expanded Lua aliases before changing stored data.
12. An intentional permanent CREATE conflict does not suppress the next valid
    update to the same key. The final recovery scenario inspects exactly one DLQ
    record, repairs the conflict, replays it with the Sink CLI, reconciles the
    stored business result, and verifies the original DLQ position is preserved.
13. Applied/visible completion, independent datasets, per-RPC budgets, real
    revision conflicts, queued cancellation, formatted JSON and bounded hot-key
    backend work satisfy the incident regressions with race detection.
14. An independent Go operation model checks mixed Create/Upsert/Replace/Merge,
    permanent failures, reordered/duplicate Reads and duplicate Deletes after
    every RPC across batching configurations and all seven backend stores.
15. PR #44's native Execute, Query, Count and Scan APIs run through the public
    Dataset API on all seven stores: exact pagination, projections, count
    strategies, native errors, BSON preservation and canceled scans.
16. Damaged search pages and approximate totals fail without retries; canceled
    and timed-out Scan requests release backend resources and admission slots. Lost native
    mutation acknowledgements do not replay increments. Oversized responses and
    invalid raw RPCs fail before exposing partial results or reaching storage.
17. Returned writes report each operation's committed value and revision through
    real conflicts and concurrent server replicas. Response budgets belong to
    each original RPC and reject an oversized candidate before its commit.
18. Paged Scan resumes from the last processed cursor after graceful server exit
    or SIGKILL, including a lost page response. Cursors have no expiry and hold no
    backend session between pages; scans observe live data rather than a snapshot.
19. Ascending and descending scans continue through record deletion, insertion
    before/after a checkpoint and updates to unseen records on all seven stores.
    Alternating server replicas, changing page sizes, retrying a saved checkpoint,
    cancellation and cursor corruption preserve the expected remaining records.
20. Missing timeout/shard completion evidence and inconsistent shard counts fail
    Query, Count and Scan without exposing partial output. MongoDB Query rejects
    partial shard results; unordered native writes preserve successful siblings
    while returning the original native error for a failed member.

Release qualification uses a bounded three-minute active-fault workload with a
three-minute deadline for each business cycle to reconcile. The fixture removes
its former `max_retry_attempts: 30` override and exercises Sink's default retry
rounds. Every normal-workload DLQ must remain empty before the deliberate
permanent-error scenario runs. A successful replay does not delete DLQ records.

The nightly/manual two-hour workflow uses this repository's same orchestration
and assertions at higher concurrency. Each run retains exact suite/server
revisions, resolved Compose configuration, test logs, fault timestamps, container
resource samples, Prometheus samples, and DLQ inspect/replay reports for 14 days.
The standalone script prints its local evidence directory even on failure.
The two-hour run is separate from the release gate; a passing short run does not
imply a completed long run or multi-node production certification.

## Infrastructure

Docker Compose starts all disposable dependencies on the runner:

- MongoDB 8 replica set
- Elasticsearch 8
- OpenSearch 3
- two independent Apache Kafka clusters
- two Sink servers and one Sink worker

No AWS, EKS, persistent cloud volume, KEDA, or private repository is required.
Kubernetes scheduling and autoscaling belong to deployment validation rather
than the public Sink release contract.

## Local usage

Requirements:

- Go version from `go.mod`
- Docker with Compose v2
- a local checkout of Sink

Run unit, race, and lint gates:

```bash
make test-race
make lint
```

`make fuzz` requires the Go fuzzer to finish baseline replay and start generating
mutations. Go can otherwise report PASS after spending the entire time budget
loading cached inputs. If the gate reports no mutation-based fuzzing, increase
`FUZZ_TIME` above the three-minute default, for example `FUZZ_TIME=5m make fuzz`. Each invocation retains its
log in the printed evidence directory; `FUZZ_PARALLEL` controls workers.

Run the backend matrix without load or active fault injection:

```bash
SINK_SERVER_DIR=/path/to/sink make test-integration
```

For unreleased Scan protocol changes, run conformance against matching local
server and Go client checkouts. The SDK replacement uses a temporary module file:

```bash
SINK_SERVER_DIR=/path/to/sink SINK_GO_DIR=/path/to/sink-go make test-conformance
```

The same `SINK_GO_DIR` option applies to the integration and production runners.

Run the complete non-durability release gate:

```bash
SINK_SERVER_DIR=/path/to/sink make test-production
```

Run the same backend, recovery, and reconciliation checks with two hours of
active workload in disposable infrastructure:

```bash
SINK_SERVER_DIR=/path/to/sink make test-reliability
```

The nightly workflow lives in this repository. Sink can invoke it using the
pinned reusable workflow and an explicit candidate revision. Standard release
qualification can be tuned with `SINK_SOAK_DURATION`, `SINK_SOAK_CONCURRENCY`,
`SINK_SOAK_MIN_CYCLES`, and `SINK_SOAK_TEST_TIMEOUT`; the default fault sequence
needs at least three minutes of scheduled workload.

Run the optional six-hour test against an already running compatible
environment:

```bash
SINK_ADDRESS=127.0.0.1:18080 \
SINK_SECONDARY_ADDRESS=127.0.0.1:18081 \
SINK_BACKEND_STORES='primary:async,secondary:async,sync-only:sync,elasticsearch-sync:sync,elasticsearch-async:async,mongodb-sync:sync,mongodb-async:async' \
SINK_RUN_SOAK=1 \
SINK_SOAK_DURATION=6h \
SINK_SOAK_CONCURRENCY=16 \
SINK_SOAK_MIN_CYCLES=10000 \
  go test -tags=integration ./integration \
  -run '^TestStorageBackendSoak$' \
  -count=1 \
  -timeout=7h \
  -v
```

Each integration run uses a unique Compose project and image name and waits for
`/readyz` on both servers and the worker before traffic. It removes only its own
Compose resources and volumes after the run, including failures. Local runs need
the documented fixed ports to be free; evidence files remain in the printed
temporary directory. `go test ./...` remains independent of Docker and backends;
real infrastructure tests require the `integration` build tag and the runner.

## Reusable release workflow

The public reusable workflow accepts an immutable Sink tag or commit:

```yaml
jobs:
  qualify:
    uses: liran/sink-production-suite/.github/workflows/release-qualification.yml@SUITE_COMMIT
    with:
      suite_ref: SUITE_COMMIT
      sink_ref: v0.9.0
```

It runs race tests, lint, bounded stateful fuzzing, the seven-store backend
matrix, capacity boundaries, controlled recovery, representative load, lag
checks, and dead-letter inspection/repair/replay without repository secrets. Use the same immutable suite commit for the
workflow reference and `suite_ref` so the workflow definition and test source
cannot drift independently.

## Application semantics

**The application owns business idempotence.** Sink provides at-least-once
asynchronous delivery. The fault-soak workload carries an operation ID and
reconciles persisted state; it does not claim Sink automatically deduplicates
arbitrary business operations. Lua output guards are not a strict VM heap quota.
Network partitions across fault domains, full disks, durable replica elections,
and backup restoration still require deployment-specific qualification.

The representative item and offer models intentionally contain bounded
histories and realistic merge behavior. Replay is deterministic and preserves
deduplication and bounds, but it is not byte-idempotent at capped-history
boundaries: replay may rotate bounded history entries. The suite requires exact
agreement between the Go reference and Lua results while separately enforcing
uniqueness and limits.

## License

MIT
