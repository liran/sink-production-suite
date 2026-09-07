#!/usr/bin/env bash
set -euo pipefail

: "${SINK_SERVER_DIR:?candidate checkout is required}"
: "${SINK_CONFORMANCE_ARTIFACTS:?run through test-conformance.sh}"
: "${SINK_CONFORMANCE_ELASTICSEARCH:?real backend is required}"

prove_regression() {
  local label="$1" revision="$2" test="$3" assertion="$4"
  local directory="${SINK_CONFORMANCE_ARTIFACTS}/${label}"
  mkdir -p "${directory}/source"
  # Use immutable public commits in isolated directories; leave the checkout alone.
  git -C "${SINK_SERVER_DIR}" archive "${revision}" -- cmd internal gen go.mod go.sum | tar -x -C "${directory}/source"
  go -C "${directory}/source" build -race -o "${directory}/sink" ./cmd/sink
  local result=0
  local pattern="${test//\//$\/^}"
  SINK_SERVER_BINARY="${directory}/sink" \
    go test -race -tags=integration ./conformance -run "^${pattern}$" -count=1 -timeout=2m -json > "${directory}/tests.jsonl" || result="$?"
  if [[ "${result}" == 0 ]]; then
    echo "${label}: old server unexpectedly passed ${test}" >&2
    return 1
  fi
  go run ./cmd/check-test-events --file "${directory}/tests.jsonl" \
    --expect-failure "${test}" --assertion "${assertion}"
  printf '%s %s %s\n' "${label}" "${revision}" "${test}" >> "${SINK_CONFORMANCE_ARTIFACTS}/sensitivity-passed.txt"
}

prove_regression before-pr37 a5dadf491b605c989faa42e90018e9edb8c7ab30 TestHotKeyMergeAmplification/elasticsearch 'hot-key amplification:'
prove_regression before-pr38 b9133f460187027c0a85de506aade3b4ed9b9c19 TestAppliedDoesNotInheritVisibleRefresh/elasticsearch 'independent operation did not complete'
prove_regression before-pr40 cee5dd559dacd3333a5b9ca46f71ae9125bd1cc3 TestReadBudgetsBelongToOriginalRPC/elasticsearch "one valid read consumed another RPC's budget"
prove_regression before-pr41 d67d1a716d9929f9557039a7eabfa270cb680519 TestCompletedDocumentReleasedBeforeSiblingRead/elasticsearch 'independent operation did not complete'
prove_regression before-bounded-shutdown 04fe67f5008ca04492bf06a802d84f9651b3e4d4 TestAcceptedMutationCrashBoundaries/shutdown-during-broker-outage 'candidate graceful shutdown exceeded five seconds'
