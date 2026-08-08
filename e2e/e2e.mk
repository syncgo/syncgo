.PHONY: prepare wait_healthy_postgres create_publication generate_data

E2E_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
E2E_COMPOSE_FILE := $(E2E_DIR)docker-compose.yml

prepare:
	docker compose -f "$(E2E_COMPOSE_FILE)" up -d

	@$(MAKE) wait_healthy_postgres

	docker exec -it e2e-postgres-1 psql -U postgres -d postgres -c "ALTER SYSTEM SET wal_level = 'logical';"
	docker exec -it e2e-postgres-1 psql -U postgres -d postgres -c "ALTER SYSTEM SET max_wal_senders = 5;"
	docker exec -it e2e-postgres-1 psql -U postgres -d postgres -c "ALTER SYSTEM SET max_replication_slots = 5;"
	docker exec -it e2e-postgres-1 psql -U postgres -d postgres -c "ALTER SYSTEM SET logical_decoding_work_mem = '64kB';"
	docker exec -it e2e-postgres-1 psql -U postgres -d postgres -c "CREATE TABLE IF NOT EXISTS test (id int primary key, name text);"

	@$(MAKE) create_publication

	docker restart e2e-postgres-1 

	@$(MAKE) wait_healthy_postgres
	
	docker exec -it e2e-postgres-1 psql -U postgres -d postgres -c 'SHOW wal_level'
	docker exec -it e2e-postgres-1 psql -U postgres -d postgres -c 'SHOW max_wal_senders'
	docker exec -it e2e-postgres-1 psql -U postgres -d postgres -c 'SHOW max_replication_slots'
	docker exec -it e2e-postgres-1 psql -U postgres -d postgres -c 'SHOW logical_decoding_work_mem'

wait_healthy_postgres:
	until [ "$$(docker inspect --format='{{.State.Health.Status}}' e2e-postgres-1)" = "healthy" ]; do \
	  status="$$(docker inspect --format='{{.State.Health.Status}}' e2e-postgres-1)"; \
	  echo "Current status: $$status"; \
	  sleep 2; \
	done;

create_publication:
	docker exec -i e2e-postgres-1 psql -U postgres -d postgres -c '\
		DO $$$$ \
		BEGIN \
			IF NOT EXISTS ( \
				SELECT 1 \
				FROM pg_catalog.pg_publication \
				WHERE pubname = '\''test_publication'\'' \
			) THEN \
				CREATE PUBLICATION test_publication \
				FOR TABLE public.test; \
			END IF; \
		END \
		$$$$;'


generate_data:
	docker exec -it e2e-postgres-1 \
	  psql -U postgres -d postgres \
	  -c "INSERT INTO test (id, name) VALUES (1, 'foo');"

	docker exec -it e2e-postgres-1 \
	  psql -U postgres -d postgres \
	  -c "INSERT INTO test (id, name) VALUES (2, 'alice');"

	docker exec -it e2e-postgres-1 \
	  psql -U postgres -d postgres \
	  -c "INSERT INTO test (id, name) VALUES (3, 'bob');"

	docker exec -it e2e-postgres-1 \
	  psql -U postgres -d postgres \
	  -c "UPDATE test SET name = 'foo-updated' WHERE id = 1;"

	docker exec -it e2e-postgres-1 \
	  psql -U postgres -d postgres \
	  -c "UPDATE test SET name = 'alice-updated' WHERE id = 2;"

	docker exec -it e2e-postgres-1 \
	  psql -U postgres -d postgres \
	  -c "DELETE FROM test WHERE id = 3;"
