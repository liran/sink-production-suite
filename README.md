# Sink production qualification suite

This public repository qualifies [Sink](https://github.com/liran/sink) against
a representative commerce-indexing workload using only synthetic fixtures,
public dependencies, and disposable local infrastructure. It does not import
proprietary application packages, use production data, require cloud
credentials, or connect to an external Kubernetes cluster.

## Release gates

The suite verifies:

1. Representative item and offer merge programs match the public Go reference
   model, including history limits, deduplication, timestamps, large integers,
   and replay behavior.
2. The same programs produce equal documents through the public Go client,
   multiple Sink server processes, and real storage backends.
3. Concurrent merges preserve every successful update while exercising real
   search-engine revision conflicts.
4. Store-owned asynchronous routing works through two independent Kafka
   clusters without changing result order.
5. Stores without Kafka remain available synchronously and reject asynchronous
   requests as retryable unavailable results without publishing anything.
6. MongoDB, Elasticsearch, and OpenSearch pass create, duplicate-create,
   concurrent merge, asynchronous write/delete, and synchronous delete checks.
7. Accepted Kafka mutations survive worker and broker restarts and retain
   same-record ordering.
8. Active operations recover from controlled OpenSearch and Kafka restarts.
9. A representative concurrent load completes without failed operations or
   exhausted merge-conflict retries.
10. Every consumer group drains to zero lag and every dead-letter topic remains
    empty.

The six-hour durability test remains an explicit manual gate. Release
qualification uses a bounded two-minute active-fault test so that every Sink
release can be checked automatically on GitHub-hosted runners. An operation
already in flight during the sequential restarts has a hard three-minute
deadline to reconcile to its expected state. The disposable worker retry
budget outlasts that window, and the gate still requires every dead-letter
topic to remain empty.

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

Run the backend matrix without load or active fault injection:

```bash
SINK_SERVER_DIR=/path/to/sink make test-integration
```

Run the complete non-durability release gate:

```bash
SINK_SERVER_DIR=/path/to/sink make test-production
```

Run the optional durability test against an already running compatible
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

All Compose resources and volumes are removed after each integration run,
including failed runs.

## Reusable release workflow

The public reusable workflow accepts an immutable Sink tag or commit:

```yaml
jobs:
  qualify:
    uses: liran/sink-production-suite/.github/workflows/release-qualification.yml@SUITE_COMMIT
    with:
      suite_ref: SUITE_COMMIT
      sink_ref: v0.6.1
```

It runs race tests, lint, bounded stateful fuzzing, the seven-store backend
matrix, controlled recovery, representative load, lag checks, and dead-letter
checks without repository secrets. Use the same immutable suite commit for the
workflow reference and `suite_ref` so the workflow definition and test source
cannot drift independently.

## Application semantics

The representative item and offer models intentionally contain bounded
histories and realistic merge behavior. Replay is deterministic and preserves
deduplication and bounds, but it is not byte-idempotent at capped-history
boundaries: replay may rotate bounded history entries. The suite requires exact
agreement between the Go reference and Lua results while separately enforcing
uniqueness and limits.

## License

MIT
