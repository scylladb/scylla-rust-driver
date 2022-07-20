COMPOSE := docker-compose

.PHONY: all
all: test

.PHONY: test
test:
	@bash test_in_docker.sh

.PHONY: scylla-up
scylla-up:
	@$(COMPOSE) up -d

.PHONY: scylla-down
scylla-down:
	@$(COMPOSE) down --volumes --remove-orphans

.PHONY: scylla-logs
scylla-logs:
	@$(COMPOSE) exec node tail -f /var/log/syslog

.PHONY: scylla-bash
scylla-bash:
	@$(COMPOSE) exec node bash

.PHONY: scylla-cqlsh
scylla-cqlsh:
	@$(COMPOSE) exec node cqlsh -u cassandra -p cassandra
