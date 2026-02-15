setup:
	docker compose -f dev/docker-compose.yaml up --build -d

	sleep 15

	docker exec dev-postgres-1 psql -U postgres -c "ALTER SYSTEM SET wal_level = logical;"
	docker exec dev-postgres-1 psql -U postgres -c "ALTER SYSTEM SET max_wal_senders = 5;"
	docker exec dev-postgres-1 psql -U postgres -c "ALTER SYSTEM SET max_replication_slots = 5;"
	docker exec dev-postgres-1 psql -U postgres -c "ALTER SYSTEM SET logical_decoding_work_mem = '64kB';"

	docker restart dev-postgres-1
	docker exec dev-postgres-1 psql -U postgres -c "CREATE DATABASE pglogrepl;"
	docker exec dev-postgres-1 psql -U postgres -d pglogrepl -c "CREATE USER pglogrepl WITH REPLICATION PASSWORD 'secret';"
	docker exec dev-postgres-1 psql -U postgres -d pglogrepl -c "CREATE TABLE t (id int primary key, name text);"

	docker exec dev-postgres-1 psql -U postgres -d pglogrepl -c "CREATE PUBLICATION pglogrepl_demo FOR ALL TABLES;"

generator:
	docker exec dev-postgres-1 psql -U postgres -d pglogrepl -c "INSERT INTO t VALUES(1, 'foo');"
	docker exec dev-postgres-1 psql -U postgres -d pglogrepl -c "UPDATE t SET name='bar';"
	docker exec dev-postgres-1 psql -U postgres -d pglogrepl -c "DELETE FROM t;"

publication:
	docker exec dev-postgres-1 psql -U postgres -d pglogrepl -c "CREATE PUBLICATION pglogrepl_demo FOR ALL TABLES;"

build:
	mkdir -p bin
	go build -o bin ./...

syncgo_run: build
	./bin/syncgo --config=dev/config.yaml

dev_build:
	docker-compose -f dev/docker-compose.yaml up --build

dev_down:
	docker-compose -f dev/docker-compose.yaml down

dev_stop:
	docker-compose -f dev/docker-compose.yaml stop

dev_start:
	docker-compose -f dev/docker-compose.yaml start

test:
	go test ./... -v -short

generate:
	protoc --go_out=. --go_opt=module=github.com/romanchechyotkin/syncgo --go-grpc_out=. --go-grpc_opt=module=github.com/romanchechyotkin/syncgo --proto_path=proto proto/schemas/common.proto proto/services/*.proto