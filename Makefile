COMPOSE := docker-compose -f test/cluster/docker-compose.yml

.PHONY: all
all: test

.PHONY: ci
ci: fmt-check check check-without-features clippy test build

.PHONY: fmt
fmt:
	cargo fmt --all

.PHONY: fmt-check
fmt-check:
	cargo fmt --all -- --check

.PHONY: check
check:
	cargo check --examples --tests

.PHONY: check-without-features
check-without-features:
	cargo check --manifest-path "scylla/Cargo.toml" --features ""

.PHONY: clippy
clippy:
	RUSTFLAGS=-Dwarnings cargo clippy --examples --tests

.PHONY: test
test: up wait-for-cluster
	SCYLLA_URI=172.42.0.2:9042 \
	 SCYLLA_URI2=172.42.0.3:9042 \
	 SCYLLA_URI3=172.42.0.4:9042 \
	 cargo test

.PHONY: build
build:
	cargo build --verbose --examples

.PHONY: docs
docs:
	./docs/build_book.py

.PHONY: up
up:
	$(COMPOSE) up -d
	@echo
	@echo "ScyllaDB cluster is running in the background. Use 'make down' to stop it."
	@echo

.PHONY: down
down:
	$(COMPOSE) down --remove-orphans

.PHONY: logs
logs:
	$(COMPOSE) logs -f

.PHONY: cqlsh
cqlsh:
	$(COMPOSE) exec scylla1 cqlsh -u cassandra -p cassandra

.PHONY: shell
shell:
	$(COMPOSE) exec scylla1 bash

.PHONY: clean
clean: down
	cargo clean
	rm -rf docs/book

.PHONY: wait-for-cluster
wait-for-cluster:
	@test/cluster/wait.sh
