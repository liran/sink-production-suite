.PHONY: test test-race fuzz test-integration test-production test-reliability lint

STATICCHECK_VERSION := v0.8.1
FUZZ_TIME ?= 30s

test:
	go test ./... -count=1

test-race:
	go test -race ./... -count=1

fuzz:
	go test ./contract -run='^$$' -fuzz=FuzzProductMergeSequence -fuzztime=$(FUZZ_TIME)
	go test ./contract -run='^$$' -fuzz=FuzzOfferMergeSequence -fuzztime=$(FUZZ_TIME)

test-integration:
	bash scripts/test-integration.sh

test-production:
	SINK_RUN_LOAD=1 SINK_RUN_RESILIENCE=1 bash scripts/test-integration.sh

test-reliability:
	SINK_RUN_LOAD=1 SINK_RUN_RESILIENCE=1 SINK_SOAK_DURATION=2h SINK_SOAK_CONCURRENCY=16 SINK_SOAK_MIN_CYCLES=1000 SINK_SOAK_TEST_TIMEOUT=150m bash scripts/test-integration.sh

lint:
	@test -z "$$(gofmt -l .)"
	go vet -tags=integration ./...
	go run honnef.co/go/tools/cmd/staticcheck@$(STATICCHECK_VERSION) -tags=integration -checks=all ./...
