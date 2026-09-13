# Recent feature coverage review

Scope: Sink `026d1d1` through `a1178ef` (September 9–11), Go SDK
`7ca6ae8` through `cbb6190` / `v0.5.1`, and suite baseline `7917c33`.

| Recent change | Existing evidence | Qualification added or repaired |
| --- | --- | --- |
| Isolated Kafka publishing admission | Deterministic server tests cover byte waiters, request/byte limits, cancellation and mixed returned writes. | Hold real search storage while publishing Write/Delete to real Kafka, with default and one-operation batches; pause Kafka and verify synchronous work, bounded publish rejection, exact topic offsets, recovery and empty DLQ. |
| Streaming synchronous merge working sets | Server unit tests cover snapshots, outputs, conflicts and per-caller budgets. Real MongoDB/OpenSearch service tests existed but their scripts never selected the service package. | Execute those service tests in both backend CI jobs; coalesce eight public RPCs and verify streamed snapshots/outputs, per-caller returned documents and persisted counters on Elasticsearch/OpenSearch. |
| MongoDB 8 conditional bulk and delta replacements | Backend integration already verifies mixed per-record results, durability errors, BSON literals, removed fields, singleton tails and bounded delta oplog entries. | Retain and rerun the existing real replica-set integration gate; avoid duplicating the storage-specific oracle in the public suite. |
| Admission fairness | Server synctest exercises older large reservations, cancellation waking the next waiter and independent stores. | Retain the race gate and public saturation matrix; the new publish tests cover the separate pool under actual dependency pressure. |
| Reused Lua library initialization | Server race tests verify fresh mutable libraries after failure, concurrent requests, request-bound time and sandbox restrictions. | Retain the race gate plus the suite's independent business reference models and mutation-based fuzzing. |
| SDK round robin and DNS refresh intervals | SDK CI already tests real loopback DNS scale changes, healthy connections, SERVFAIL and connection reuse. | Upgrade the suite from its September 9 SDK pin to v0.5.1 and require the balancing and both DNS interval test events in every conformance run. |

The local default runner also exposed a Bash 3.2 failure: an empty array expanded
under `set -u` before any Go test ran. Both runners now start their Go flag array
with `-mod=readonly`, retaining optional local SDK replacement without an empty
array or accidental dependency changes.

A new real-Kafka case also exposed an observation race in the harness: broker
Ping can succeed before the consumer-group coordinator is available. Offset
inspection now retries only coordinator startup/movement errors within its
existing five-second deadline. Failed inspection never counts as a successful
drain or a zero offset; other errors still fail immediately.

Historical sensitivity checks reject `33f8d6f` at the Kafka acceptance assertion
while synchronous storage is held, and `026d1d1` at the bounded streaming
assertion (one combined snapshot read and one combined mutation bulk). The event
checker requires these specific assertion failures, not a failed build or an
unavailable backend. Both cases are included in the ongoing regression gate.

The SDK DNS test uses loopback gRPC fixtures. It does not claim to test Kubernetes
DNS, scheduling or autoscaling. The backend qualification uses disposable local
databases and brokers; multi-node elections, disk faults, cross-region partitions
and long-duration capacity certification remain separate deployment checks.
