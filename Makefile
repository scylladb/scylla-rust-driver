COMPOSE := docker compose -f test/cluster/docker-compose.yml

.PHONY: all
all: test

.PHONY: ci
ci: fmt-check check check-without-features clippy test build

.PHONY: dockerized-ci
dockerized-ci: fmt-check check check-without-features clippy dockerized-test build

.PHONY: fmt
fmt:
	cargo fmt --all

.PHONY: fmt-check
fmt-check:
	cargo fmt --all -- --check

.PHONY: check
check:
	cargo check --all-targets

.PHONY: check-without-features
check-without-features:
	cargo check --manifest-path "scylla/Cargo.toml" --features "" --all-targets

.PHONY: clippy
clippy:
	RUSTFLAGS=-Dwarnings cargo clippy --all-targets -- -Aclippy::uninlined_format_args

.PHONY: test
test: up
	SCYLLA_URI=172.42.0.2:9042 \
	 SCYLLA_URI2=172.42.0.3:9042 \
	 SCYLLA_URI3=172.42.0.4:9042 \
	 cargo test

.PHONY: dockerized-test
dockerized-test: up
	test/dockerized/run.sh

.PHONY: build
build:
	cargo build --examples --benches

.PHONY: docs
docs:
	mdbook build docs

.PHONY: semver-rev
semver-rev:
	./scripts/semver-checks.sh $(if $(rev),--baseline-rev $(rev),--baseline-rev main)

.PHONY: semver-version
semver-version:
	./scripts/semver-checks.sh $(if $(version),--baseline-version $(version),)

.PHONY: up
up:
	$(COMPOSE) up -d --wait
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
