#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
suite_dir="$(cd "${script_dir}/.." && pwd)"
export SINK_SERVER_DIR="${SINK_SERVER_DIR:-${suite_dir}/../sink}"
artifacts="$(mktemp -d "${TMPDIR:-/tmp}/sink-qualification.XXXXXXXX")"
project="sink-qualification-$(date +%s)-$$"
export SINK_SUITE_IMAGE="${project}:local"
compose=(docker compose --env-file /dev/null --project-name "${project}" --project-directory "${suite_dir}" --file "${suite_dir}/deploy/compose.yaml")
resilience_pid=""
sampler_pid=""
exec > >(tee "${artifacts}/test.log") 2>&1

backend_stores="primary:async,secondary:async,sync-only:sync,elasticsearch-sync:sync,elasticsearch-async:async,mongodb-sync:sync,mongodb-async:async"

cleanup() {
	exit_code="$?"
	trap - EXIT
	for process in "${resilience_pid}" "${sampler_pid}"; do
		if [[ -n "${process}" ]]; then
			kill "${process}" >/dev/null 2>&1 || true
			wait "${process}" >/dev/null 2>&1 || true
		fi
	done
	"${compose[@]}" ps --all > "${artifacts}/containers.txt" 2>&1 || true
	"${compose[@]}" logs --no-color > "${artifacts}/containers.log" 2>&1 || true
	"${compose[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true
	echo "Qualification evidence: ${artifacts}"
	exit "${exit_code}"
}
trap cleanup EXIT

wait_for_readiness() {
	for port in 19090 19091 19092; do
		local ready=0
		for _ in $(seq 1 60); do
			if curl --max-time 3 --fail --silent "http://127.0.0.1:${port}/readyz" >/dev/null; then
				ready=1
				break
			fi
			sleep 2
		done
		if [[ "${ready}" != 1 ]]; then
			echo "Sink dependency readiness on port ${port} did not recover" >&2
			return 1
		fi
	done
}

record_fault() {
	printf '%s %s\n' "$(date -u +%FT%TZ)" "$1" >> "${artifacts}/faults.log"
}

wait_for_zero_group_lag() {
	local service="$1"
	local group="$2"
	local observation=""
	for _ in $(seq 1 60); do
		observation="$(
			"${compose[@]}" exec -T "${service}" \
				/opt/kafka/bin/kafka-consumer-groups.sh \
				--bootstrap-server localhost:19092 \
				--describe \
				--group "${group}" 2>/dev/null | \
				awk -v group="${group}" '
					$1 == group && $3 ~ /^[0-9]+$/ {
						count++
						if ($6 ~ /^[0-9]+$/) {
							lag += $6
						} else if (!($4 == "-" && $5 == "0" && $6 == "-")) {
							invalid++
						}
					}
					END {printf "%d:%d:%d", count, lag, invalid}
				'
		)"
		if [[ "${observation}" == "8:0:0" ]]; then
			return
		fi
		sleep 2
	done
	echo "consumer group ${group} on ${service} did not drain: ${observation}" >&2
	return 1
}

assert_empty_dlq() {
	local service="$1"
	local topic="$2"
	local observation=""
	observation="$(
		"${compose[@]}" exec -T "${service}" \
			/opt/kafka/bin/kafka-get-offsets.sh \
			--bootstrap-server localhost:19092 \
			--topic "${topic}" | \
			awk -F: '$3 ~ /^[0-9]+$/ {count++; offsets += $3} END {printf "%d:%d", count, offsets}'
	)"
	if [[ "${observation}" != "8:0" ]]; then
		echo "dead-letter topic ${topic} on ${service} is not empty: ${observation}" >&2
		return 1
	fi
}

git -C "${SINK_SERVER_DIR}" rev-parse HEAD > "${artifacts}/server-revision.txt"
git -C "${suite_dir}" rev-parse HEAD > "${artifacts}/suite-revision.txt"
"${compose[@]}" config > "${artifacts}/compose.yaml"
go test ./contract ./internal/... -count=1
"${compose[@]}" up --build --detach --wait --wait-timeout 180
wait_for_readiness
(
	while true; do
		"${compose[@]}" stats --no-stream --format json >> "${artifacts}/resources.jsonl" || true
		for port in 19090 19091 19092; do
			date -u +%FT%TZ >> "${artifacts}/metrics-${port}.txt"
			curl --max-time 3 --silent "http://127.0.0.1:${port}/metrics" >> "${artifacts}/metrics-${port}.txt" || true
		done
		sleep 30
	done
) &
sampler_pid="$!"

SINK_ADDRESS=127.0.0.1:18080 \
SINK_SECONDARY_ADDRESS=127.0.0.1:18081 \
SINK_SEARCH_ENDPOINT=http://127.0.0.1:19200 \
	go test -tags=integration ./integration -run 'Test(Product|Offer|Concurrent|Store|Reliability)' -v -count=1 -timeout=10m

SINK_ADDRESS=127.0.0.1:18080 \
SINK_SECONDARY_ADDRESS=127.0.0.1:18081 \
SINK_SEARCH_ENDPOINT=http://127.0.0.1:19200 \
SINK_BACKEND_STORES="${backend_stores}" \
	go test -tags=integration ./integration -run '^TestConfiguredStorageBackendsThroughSink$' -count=1 -timeout=10m

"${compose[@]}" stop sink-worker
recovery_suffix="$(date +%s)-$$"
recovery_index="sink-qualification-recovery-${recovery_suffix}"
recovery_key="shopify:recovery.example:${recovery_suffix}"

SINK_ADDRESS=127.0.0.1:18080 \
SINK_SECONDARY_ADDRESS=127.0.0.1:18081 \
SINK_SEARCH_ENDPOINT=http://127.0.0.1:19200 \
SINK_RECOVERY_PHASE=publish \
SINK_RECOVERY_INDEX="${recovery_index}" \
SINK_RECOVERY_KEY="${recovery_key}" \
	go test -tags=integration ./integration -run TestKafkaBacklogSurvivesWorkerRestart -count=1 -timeout=5m

"${compose[@]}" restart kafka
"${compose[@]}" up --detach --wait kafka
"${compose[@]}" start sink-worker
SINK_ADDRESS=127.0.0.1:18080 \
SINK_SECONDARY_ADDRESS=127.0.0.1:18081 \
SINK_SEARCH_ENDPOINT=http://127.0.0.1:19200 \
SINK_RECOVERY_PHASE=verify \
SINK_RECOVERY_INDEX="${recovery_index}" \
SINK_RECOVERY_KEY="${recovery_key}" \
	go test -tags=integration ./integration -run TestKafkaBacklogSurvivesWorkerRestart -count=1 -timeout=5m

if [[ "${SINK_RUN_LOAD:-0}" == "1" ]]; then
	SINK_ADDRESS=127.0.0.1:18080 \
	SINK_SECONDARY_ADDRESS=127.0.0.1:18081 \
	SINK_SEARCH_ENDPOINT=http://127.0.0.1:19200 \
	SINK_RUN_LOAD=1 \
		go test -tags=integration ./integration -run TestRepresentativeProductMergeLoad -count=1 -timeout=10m -v
fi

if [[ "${SINK_RUN_RESILIENCE:-0}" == "1" ]]; then
	SINK_ADDRESS=127.0.0.1:18080 \
	SINK_SECONDARY_ADDRESS=127.0.0.1:18081 \
	SINK_SEARCH_ENDPOINT=http://127.0.0.1:19200 \
	SINK_BACKEND_STORES="${backend_stores}" \
	SINK_RUN_SOAK=1 \
	SINK_SOAK_DURATION="${SINK_SOAK_DURATION:-3m}" \
	SINK_SOAK_CONCURRENCY="${SINK_SOAK_CONCURRENCY:-8}" \
	SINK_SOAK_MIN_CYCLES="${SINK_SOAK_MIN_CYCLES:-100}" \
		go test -tags=integration ./integration -run '^TestStorageBackendSoak$' -count=1 -timeout="${SINK_SOAK_TEST_TIMEOUT:-10m}" -v &
	resilience_pid="$!"
	sleep 15
	kill -0 "${resilience_pid}"
	record_fault worker-sigkill
	"${compose[@]}" kill --signal SIGKILL sink-worker
	"${compose[@]}" up --detach sink-worker
	sleep 15
	kill -0 "${resilience_pid}"
	record_fault opensearch-unavailable
	"${compose[@]}" stop opensearch
	# This exceeds the product's 20-second processing window and default
	# retry round, while healthy stores must keep serving.
	sleep 45
	curl --max-time 5 --fail --silent http://127.0.0.1:19090/livez >/dev/null
	curl --max-time 5 --fail --silent 'http://127.0.0.1:19090/readyz?service=sink.storage.mongodb-sync' >/dev/null
	readiness_status="$(curl --max-time 5 --silent --output /dev/null --write-out '%{http_code}' http://127.0.0.1:19090/readyz)"
	if [[ "${readiness_status}" != 503 ]]; then
		echo "unavailable OpenSearch must fail dependency readiness: ${readiness_status}" >&2
		exit 1
	fi
	record_fault opensearch-recovery
	"${compose[@]}" start opensearch
	"${compose[@]}" up --detach --wait --wait-timeout 180 opensearch
	sleep 15
	kill -0 "${resilience_pid}"
	record_fault kafka-restart
	"${compose[@]}" restart kafka
	"${compose[@]}" up --detach --wait --wait-timeout 180 kafka
	wait "${resilience_pid}"
	resilience_pid=""
	wait_for_readiness
	record_fault all-dependencies-ready
fi

wait_for_zero_group_lag kafka sink-production-workers
wait_for_zero_group_lag kafka sink-production-elasticsearch-workers
wait_for_zero_group_lag kafka-secondary sink-production-secondary-workers
wait_for_zero_group_lag kafka-secondary sink-production-mongodb-workers

assert_empty_dlq kafka sink-production-mutations.dlq
assert_empty_dlq kafka sink-production-elasticsearch-mutations.dlq
assert_empty_dlq kafka-secondary sink-production-secondary-mutations.dlq
assert_empty_dlq kafka-secondary sink-production-mongodb-mutations.dlq

total_conflicts=0
total_exhausted=0
for metrics_port in 19090 19091; do
	metrics="$(curl --fail --silent --show-error "http://127.0.0.1:${metrics_port}/metrics")"
	grep -q '^sink_merge_conflicts_total' <<<"${metrics}"
	grep -q '^sink_merge_exhausted_total' <<<"${metrics}"
	grep -q '^sink_grpc_server_requests_total' <<<"${metrics}"
	conflicts="$(awk '$1 == "sink_merge_conflicts_total" {print int($2)}' <<<"${metrics}")"
	exhausted="$(awk '$1 == "sink_merge_exhausted_total" {print int($2)}' <<<"${metrics}")"
	total_conflicts=$((total_conflicts + conflicts))
	total_exhausted=$((total_exhausted + exhausted))
done
if [[ "${total_conflicts}" -lt 1 ]]; then
	echo "cross-replica test did not exercise a revision conflict" >&2
	exit 1
fi
if [[ "${total_exhausted}" -ne 0 ]]; then
	echo "${total_exhausted} merge operations exhausted their conflict budget" >&2
	exit 1
fi

# Quarantine is expected only for this final, explicit permanent-error case.
# The earlier empty-DLQ checks still protect all normal and outage workloads.
export SINK_ADDRESS=127.0.0.1:18080
export SINK_SECONDARY_ADDRESS=127.0.0.1:18081
export SINK_SEARCH_ENDPOINT=http://127.0.0.1:19200
export SINK_DLQ_INDEX="sink-dlq-$(date +%s)-$$"
SINK_DLQ_PHASE=publish go test -tags=integration ./integration -run '^TestReliabilityDeadLetterRecovery$' -count=1 -timeout=3m -v
wait_for_zero_group_lag kafka sink-production-workers
"${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
	--bootstrap-server localhost:19092 --topic sink-production-mutations.dlq > "${artifacts}/dlq-offsets-before.txt"
dlq_summary="$(awk -F: '{count++; total += $3; if ($3 == 1) partition = $2} END {printf "%d:%d:%d", count, total, partition}' "${artifacts}/dlq-offsets-before.txt")"
if [[ "${dlq_summary}" != 8:1:* ]]; then
	echo "expected exactly one permanent failure in DLQ, got ${dlq_summary}" >&2
	exit 1
fi
dlq_partition="${dlq_summary##*:}"
export SINK_DLQ_INSPECT_REPORT="${artifacts}/dlq-inspect.jsonl"
export SINK_DLQ_REPLAY_REPORT="${artifacts}/dlq-replay.jsonl"
"${compose[@]}" exec -T sink-worker /usr/local/bin/sink dlq inspect \
	--config /etc/sink/config.yaml --store primary --partition "${dlq_partition}" --offset 0 --count 1 > "${SINK_DLQ_INSPECT_REPORT}"
SINK_DLQ_PHASE=repair go test -tags=integration ./integration -run '^TestReliabilityDeadLetterRecovery$' -count=1 -timeout=3m -v
"${compose[@]}" exec -T sink-worker /usr/local/bin/sink dlq replay \
	--config /etc/sink/config.yaml --store primary --partition "${dlq_partition}" --offset 0 --count 1 > "${SINK_DLQ_REPLAY_REPORT}"
SINK_DLQ_PHASE=verify go test -tags=integration ./integration -run '^TestReliabilityDeadLetterRecovery$' -count=1 -timeout=3m -v
wait_for_zero_group_lag kafka sink-production-workers
"${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
	--bootstrap-server localhost:19092 --topic sink-production-mutations.dlq > "${artifacts}/dlq-offsets-after.txt"
diff -u <(sort "${artifacts}/dlq-offsets-before.txt") <(sort "${artifacts}/dlq-offsets-after.txt")
wait_for_readiness
echo 'PASS permanent-failure continuation, DLQ inspection, repair, replay, and preserved DLQ offsets'
