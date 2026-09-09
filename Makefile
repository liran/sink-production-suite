.PHONY: test test-race fuzz test-integration test-production test-reliability test-conformance test-regression-sensitivity lint

STATICCHECK_VERSION := v0.8.1
FUZZ_TIME ?= 180s

test:
	go test ./... -count=1

test-race:
	go test -race ./... -count=1

fuzz:
	FUZZ_TIME=$(FUZZ_TIME) bash scripts/test-fuzz.sh FuzzProductMergeSequence
	FUZZ_TIME=$(FUZZ_TIME) bash scripts/test-fuzz.sh FuzzOfferMergeSequence

test-conformance:
	bash scripts/test-conformance.sh

test-regression-sensitivity:
	SINK_PROVE_REGRESSIONS=1 bash scripts/test-conformance.sh

test-integration: test-conformance
	bash scripts/test-integration.sh

test-production: test-conformance
	SINK_RUN_LOAD=1 SINK_RUN_RESILIENCE=1 bash scripts/test-integration.sh

test-reliability: test-conformance
	SINK_RUN_LOAD=1 SINK_RUN_RESILIENCE=1 SINK_SOAK_DURATION=2h SINK_SOAK_CONCURRENCY=16 SINK_SOAK_MIN_CYCLES=1000 SINK_SOAK_TEST_TIMEOUT=150m SINK_FAULT_CYCLES=12 SINK_FAULT_INTERVAL_SECONDS=300 bash scripts/test-integration.sh

lint:
	@test -z "$$(gofmt -l .)"
	go vet -tags=integration ./...
	go run honnef.co/go/tools/cmd/staticcheck@$(STATICCHECK_VERSION) -tags=integration -checks=all ./...
