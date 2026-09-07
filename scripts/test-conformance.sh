#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
suite_dir="$(cd "${script_dir}/.." && pwd)"
export SINK_SERVER_DIR="${SINK_SERVER_DIR:-${suite_dir}/../sink}"
export SINK_CONFORMANCE_ARTIFACTS="$(mktemp -d "${TMPDIR:-/tmp}/sink-conformance.XXXXXXXX")"
export SINK_SERVER_BINARY="${SINK_CONFORMANCE_ARTIFACTS}/sink"
project="sink-conformance-$(date +%s)-$$"
compose=(docker compose --env-file /dev/null --project-name "${project}" --project-directory "${suite_dir}" --file "${suite_dir}/deploy/compose.yaml" --file "${SINK_CONFORMANCE_ARTIFACTS}/ports.yaml")
exec > >(tee "${SINK_CONFORMANCE_ARTIFACTS}/run.log") 2>&1

cleanup() {
  result="$?"
  trap - EXIT
  "${compose[@]}" logs --no-color > "${SINK_CONFORMANCE_ARTIFACTS}/backends.log" 2>&1 || true
  "${compose[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true
  echo "Conformance evidence: ${SINK_CONFORMANCE_ARTIFACTS}"
  exit "${result}"
}
trap cleanup EXIT

# Dynamic loopback ports isolate this runner from other qualification projects.
cat > "${SINK_CONFORMANCE_ARTIFACTS}/ports.yaml" <<'YAML'
services:
  elasticsearch:
    ports: !override
      - "127.0.0.1::9200"
  opensearch:
    image: ${SINK_CONFORMANCE_OPENSEARCH_IMAGE:-opensearchproject/opensearch:3.8.0@sha256:bcc1797519726ceb6d651d4a3e60b7c30da91793914a8dfe75fd441d4f641509}
    ports: !override
      - "127.0.0.1::9200"
YAML

git -C "${SINK_SERVER_DIR}" rev-parse HEAD > "${SINK_CONFORMANCE_ARTIFACTS}/server-revision.txt"
git -C "${suite_dir}" rev-parse HEAD > "${SINK_CONFORMANCE_ARTIFACTS}/suite-revision.txt"
# Retain changes to tracked source as well as the base revision for local runs.
git -C "${SINK_SERVER_DIR}" diff HEAD > "${SINK_CONFORMANCE_ARTIFACTS}/server.patch"
git -C "${suite_dir}" diff HEAD > "${SINK_CONFORMANCE_ARTIFACTS}/suite.patch"
"${compose[@]}" config > "${SINK_CONFORMANCE_ARTIFACTS}/compose.yaml"
go -C "${SINK_SERVER_DIR}" build -race -o "${SINK_SERVER_BINARY}" ./cmd/sink
"${compose[@]}" up --detach --wait --wait-timeout 180 elasticsearch opensearch
export SINK_CONFORMANCE_ELASTICSEARCH="http://$("${compose[@]}" port elasticsearch 9200)"
export SINK_CONFORMANCE_OPENSEARCH="http://$("${compose[@]}" port opensearch 9200)"
cd "${suite_dir}"
go test -race -tags=integration ./conformance -count=1 -timeout=15m -json | tee "${SINK_CONFORMANCE_ARTIFACTS}/tests.jsonl"
required_tests='TestHotKeyMergeAmplification,TestAppliedDoesNotInheritVisibleRefresh,TestCompletedDocumentReleasedBeforeSiblingRead,TestSuccessfulSiblingNotReplayedDuringConflict,TestVisibleDatasetsCompleteIndependently,TestReadBudgetsBelongToOriginalRPC,TestFormattedJSONBulkFraming,TestReplaceRechecksExistenceAfterConflict,TestQueuedCancellationDoesNotPoisonFollowingWrites,TestOperationStateMachine'
go run ./cmd/check-test-events --file "${SINK_CONFORMANCE_ARTIFACTS}/tests.jsonl" --require "${required_tests}"
if [[ "${SINK_PROVE_REGRESSIONS:-0}" == 1 ]]; then
  bash scripts/check-regression-sensitivity.sh
fi
