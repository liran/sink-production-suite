#!/usr/bin/env bash
set -euo pipefail

fuzzer="${1:?a contract fuzzer is required}"
case "${fuzzer}" in
  FuzzProductMergeSequence|FuzzOfferMergeSequence) ;;
  *) echo "Unknown contract fuzzer: ${fuzzer}" >&2; exit 1 ;;
esac
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${script_dir}/.."
artifacts="$(mktemp -d "${TMPDIR:-/tmp}/sink-fuzz.XXXXXXXX")"
echo "Fuzz evidence: ${artifacts}"

go test ./contract -run='^$' -fuzz="^${fuzzer}$" \
  -fuzztime="${FUZZ_TIME:-180s}" -parallel="${FUZZ_PARALLEL:-2}" \
  2>&1 | tee "${artifacts}/${fuzzer}.log"

# Go can report PASS when the entire time budget was spent replaying cached
# coverage inputs. Require evidence that mutation-based fuzzing actually began.
if ! grep -q 'new interesting:' "${artifacts}/${fuzzer}.log"; then
  echo "${fuzzer}: no mutation-based fuzzing observed; increase FUZZ_TIME so baseline replay can finish" >&2
  exit 1
fi
