include e2e/e2e.mk

GOBIN := $(shell pwd)/bin
MODULE  := github.com/syncgo/syncgo

.PHONY: tools generate build test run demo test-e2e test-e2e-client test-e2e-pipeline

tools:
	mkdir -p bin
	GOBIN=$(GOBIN) go install tool

generate: tools
	go generate ./...

build:
	mkdir -p bin
	go build -o bin ./...

lint: tools
	$(GOBIN)/golangci-lint run ./...

test:
	go test ./... -v -short


demo: build prepare
	./bin/syncgo --config ./e2e/config.yaml

# test-e2e-client exercises the search engine clients directly against the
# docker-compose services (elasticsearch/opensearch, no postgres/syncgo involved).
test-e2e-client:
	go test -tags e2e ./e2e/elastic/... ./e2e/opensearch/... -v -timeout 5m

# test-e2e-pipeline runs syncgo end to end, both as a locally built binary and
# as its Docker image, syncing real data from postgres into a real
# elasticsearch/opensearch. It manages its own docker-compose lifecycle.
test-e2e-pipeline:
	go test -tags e2e ./e2e/pipeline/... -v -timeout 15m

test-e2e: test-e2e-client test-e2e-pipeline
