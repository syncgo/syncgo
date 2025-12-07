setup:
	docker compose -f dev.docker-compose.yaml up --build -d postgres

	sleep 15

	docker exec syncgo-postgres-1 psql -U postgres -c "ALTER SYSTEM SET wal_level = logical;"
	docker exec syncgo-postgres-1 psql -U postgres -c "ALTER SYSTEM SET max_wal_senders = 5;"
	docker exec syncgo-postgres-1 psql -U postgres -c "ALTER SYSTEM SET max_replication_slots = 5;"
	docker exec syncgo-postgres-1 psql -U postgres -c "ALTER SYSTEM SET logical_decoding_work_mem = '64kB';"

	docker restart syncgo-postgres-1 
	docker exec syncgo-postgres-1 psql -U postgres -c "CREATE DATABASE pglogrepl;"
	docker exec syncgo-postgres-1 psql -U postgres -d pglogrepl -c "CREATE USER pglogrepl WITH REPLICATION PASSWORD 'secret';"
	docker exec syncgo-postgres-1 psql -U postgres -d pglogrepl -c "CREATE TABLE t (id int primary key, name text);"

	docker exec syncgo-postgres-1 psql -U postgres -d pglogrepl -c "CREATE PUBLICATION pglogrepl_demo FOR ALL TABLES;"

generator:
	docker exec syncgo-postgres-1 psql -U postgres -d pglogrepl -c "INSERT INTO t VALUES(1, 'foo');"
	docker exec syncgo-postgres-1 psql -U postgres -d pglogrepl -c "UPDATE t SET name='bar';"
	docker exec syncgo-postgres-1 psql -U postgres -d pglogrepl -c "DELETE FROM t;"

publication:
	docker exec syncgo-postgres-1 psql -U postgres -d pglogrepl -c "CREATE PUBLICATION pglogrepl_demo FOR ALL TABLES;"

build:
	mkdir -p bin
	go build -o bin ./...

syncgo_run: build
	./bin/syncgo

