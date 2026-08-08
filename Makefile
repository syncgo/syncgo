include e2e/e2e.mk

GOBIN := $(shell pwd)/bin
MODULE  := github.com/syncgo/syncgo

.PHONY: tools generate build test run demo

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
