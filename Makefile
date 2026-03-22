GOBIN := $(shell pwd)/bin
MODULE  := github.com/romanchechyotkin/syncgo

.PHONY: tools generate build test run \
        dev_setup dev_build dev_down dev_stop dev_start \
        pg_setup pg_generator pg_publication

tools:
	mkdir -p bin
	GOBIN=$(GOBIN) go install tool

generate: tools
	PATH=$(GOBIN):$$PATH protoc \
		--go_out=. --go_opt=module=$(MODULE) \
		--go-grpc_out=. --go-grpc_opt=module=$(MODULE) \
		--proto_path=proto \
		proto/schemas/common.proto proto/services/*.proto
	go generate ./...

build:
	mkdir -p bin
	go build -o bin ./...

run: build
	./bin/syncgo --config=dev/config.yaml

test:
	go test ./... -v -short

dev_setup:
	docker compose -f dev/docker-compose.yaml up --build -d
	sleep 15
	$(MAKE) pg_setup

dev_build:
	docker-compose -f dev/docker-compose.yaml up --build

dev_down:
	docker-compose -f dev/docker-compose.yaml down

dev_stop:
	docker-compose -f dev/docker-compose.yaml stop

dev_start:
	docker-compose -f dev/docker-compose.yaml start

pg_setup:
	docker exec dev-postgres-1 psql -U postgres -c "ALTER SYSTEM SET wal_level = logical;"
	docker exec dev-postgres-1 psql -U postgres -c "ALTER SYSTEM SET max_wal_senders = 5;"
	docker exec dev-postgres-1 psql -U postgres -c "ALTER SYSTEM SET max_replication_slots = 5;"
	docker exec dev-postgres-1 psql -U postgres -c "ALTER SYSTEM SET logical_decoding_work_mem = '64kB';"
	docker restart dev-postgres-1
	docker exec dev-postgres-1 psql -U postgres -c "CREATE DATABASE pglogrepl;"
	docker exec dev-postgres-1 psql -U postgres -d pglogrepl -c "CREATE USER pglogrepl WITH REPLICATION PASSWORD 'secret';"
	docker exec dev-postgres-1 psql -U postgres -d pglogrepl -c "CREATE TABLE t (id int primary key, name text);"
	$(MAKE) pg_publication

pg_publication:
	docker exec dev-postgres-1 psql -U postgres -d pglogrepl -c "CREATE PUBLICATION pglogrepl_demo FOR ALL TABLES;"

pg_generator:
	docker exec dev-postgres-1 psql -U postgres -d pglogrepl -c "INSERT INTO t VALUES(1, 'foo');"
	docker exec dev-postgres-1 psql -U postgres -d pglogrepl -c "UPDATE t SET name='bar';"
	docker exec dev-postgres-1 psql -U postgres -d pglogrepl -c "DELETE FROM t;"
