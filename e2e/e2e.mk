.PHONY: prepare wait_healthy_postgres wait_healthy_search create_publication generate_data wait_slot_active binary_test docker_test

E2E_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
E2E_COMPOSE_FILE := $(E2E_DIR)docker-compose.yml
SEARCH_HEALTH_URL := http://localhost:9200
E2E_TEST := go test -v -tags e2e_pipeline -timeout 5m ./e2e/pipeline/...

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

	@$(MAKE) wait_healthy_search

wait_healthy_postgres:
	@until [ "$$(docker inspect --format='{{.State.Health.Status}}' e2e-postgres-1)" = "healthy" ]; do \
	  status="$$(docker inspect --format='{{.State.Health.Status}}' e2e-postgres-1)"; \
	  echo "Current status: $$status"; \
	  sleep 2; \
	done;

wait_healthy_search:
	@until curl -fs "$(SEARCH_HEALTH_URL)/_cluster/health" >/dev/null; \
	do echo "waiting for search engine..."; sleep 2; \
	done;
	
wait_slot_active:
	@until [ "$$(docker exec -i e2e-postgres-1 psql -U postgres -d postgres -tAc "SELECT active FROM pg_replication_slots WHERE slot_name='test_slot'" 2>/dev/null)" = "t" ]; do \
	echo "waiting for syncgo slot..."; \
	sleep 1; \
	done;


create_publication:
	docker exec -i e2e-postgres-1 psql -U postgres -d postgres -c "\
		DO \$$\$$ \
		BEGIN \
			IF NOT EXISTS ( \
				SELECT 1 \
				FROM pg_catalog.pg_publication \
				WHERE pubname = 'test_publication' \
			) THEN \
				CREATE PUBLICATION test_publication \
				FOR TABLE public.test; \
			END IF; \
		END \$$\$$;"


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


binary_test: build prepare
	@echo ">>> binary_test: launching syncgo binary";
	@set -e; \
	trap 'kill $$SYNC_PID 2>/dev/null || true; \
		docker compose -f "$(E2E_COMPOSE_FILE)" down -v' EXIT; \
	./bin/syncgo --config ./e2e/config.yaml > /tmp/syncgo-e2e.log 2>&1 & SYNC_PID=$$!; \
	$(MAKE) --no-print-directory wait_slot_active; \
	set +e; $(E2E_TEST); STATUS=$$?; set -e; \
	exit $$STATUS

docker_test: prepare
	@echo ">>> docker test: building and running syncgo image";
	@set -e; \
	docker build -t syncgo:e2e .; \
	docker rm -f syncgo 2>/dev/null || true; \
	trap 'docker rm -f syncgo 2>/dev/null || true; \
	docker compose -f "$(E2E_COMPOSE_FILE)" down -v' EXIT; \
	docker run -d --name syncgo --network e2e_syncgo-network \
	-v "$$PWD/e2e/config.docker.yaml:/etc/syncgo/config.yaml" syncgo:e2e; \
	$(MAKE) --no-print-directory wait_slot_active; \
	set +e; ${E2E_TEST}; STATUS=$$?; set -e; \
	exit $$STATUS