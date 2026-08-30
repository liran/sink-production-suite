#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
suite_dir="$(cd "${script_dir}/.." && pwd)"
export SINK_SERVER_DIR="${SINK_SERVER_DIR:-${suite_dir}/../sink}"
compose=(docker compose --project-directory "${suite_dir}" --file "${suite_dir}/deploy/compose.yaml")
resilience_pid=""

backend_stores="primary:async,secondary:async,sync-only:sync,elasticsearch-sync:sync,elasticsearch-async:async,mongodb-sync:sync,mongodb-async:async"

cleanup() {
	exit_code="$?"
	trap - EXIT
	if [[ -n "${resilience_pid}" ]]; then
		kill "${resilience_pid}" >/dev/null 2>&1 || true
		wait "${resilience_pid}" >/dev/null 2>&1 || true
	fi
	if [[ "${exit_code}" -ne 0 ]]; then
		"${compose[@]}" ps --all || true
		"${compose[@]}" logs --no-color --tail 200 || true
	fi
	"${compose[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true
	exit "${exit_code}"
}
trap cleanup EXIT

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

go test ./contract ./internal/... -count=1
"${compose[@]}" up --build --detach --wait

SINK_ADDRESS=127.0.0.1:18080 \
SINK_SECONDARY_ADDRESS=127.0.0.1:18081 \
SINK_SEARCH_ENDPOINT=http://127.0.0.1:19200 \
	go test -tags=integration ./integration -run 'Test(Product|Offer|Concurrent|Store)' -count=1 -timeout=10m

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
	SINK_SOAK_DURATION=2m \
	SINK_SOAK_CONCURRENCY=8 \
	SINK_SOAK_MIN_CYCLES=100 \
		go test -tags=integration ./integration -run '^TestStorageBackendSoak$' -count=1 -timeout=7m -v &
	resilience_pid="$!"
	sleep 15
	"${compose[@]}" restart opensearch
	"${compose[@]}" up --detach --wait opensearch
	sleep 15
	"${compose[@]}" restart kafka
	"${compose[@]}" up --detach --wait kafka
	wait "${resilience_pid}"
	resilience_pid=""
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
