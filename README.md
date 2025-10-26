# syncgo

## Setup

```bash
docker compose -f dev.docker-compose.yaml up --build -d postgres
docker exec -it syncgo-postgres-1 psql -U postgres 

ALTER SYSTEM SET wal_level = logical;
ALTER SYSTEM SET max_wal_senders = 5;
ALTER SYSTEM SET max_replication_slots = 5;
ALTER SYSTEM SET logical_decoding_work_mem = '64kB'; # for testing

docker restart syncgo-postgres-1 
docker exec -it syncgo-postgres-1 psql -U postgres 

CREATE DATABASE pglogrepl;
USE pglogrepl;
CREATE USER pglogrepl WITH REPLICATION PASSWORD 'secret';

CREATE TABLE t (id int primary key, name text);
INSERT INTO t VALUES(1, 'foo');
UPDATE t SET name='bar';
DELETE FROM t;

go run cmd/syncgo/main.go

docker exec -it syncgo-postgres-1 psql -U postgres 
CREATE PUBLICATION pglogrepl_demo FOR ALL TABLES;
```

