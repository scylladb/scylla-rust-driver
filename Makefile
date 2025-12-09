COMPOSE := docker compose -f test/cluster/docker-compose.yml
RUSTFLAGS := ${RUSTFLAGS} --cfg scylla_unstable
export RUSTFLAGS

.PHONY: all
all: test

.PHONY: static
static: fmt-check check check-without-features check-without-unstable check-without-unstable-and-features check-all-features clippy clippy-all-features clippy-cpp-rust

.PHONY: ci
ci: static test

.PHONY: dockerized-ci
dockerized-ci: static dockerized-test

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
	# If we pass --all-targets here, feature unification turns on some features anyway,
	# so we only check the main target.
	cargo check -p scylla

.PHONY: check-without-unstable
check-without-unstable:
	RUSTFLAGS="" cargo check -p scylla --all-targets

.PHONY: check-without-unstable-and-features
check-without-unstable-and-features:
	# If we pass --all-targets here, feature unification turns on some features anyway,
	# so we only check the main target.
	RUSTFLAGS="" cargo check -p scylla

.PHONY: check-all-features
check-all-features:
	cargo check --all-targets --all-features

.PHONY: clippy
clippy:
	RUSTFLAGS="${RUSTFLAGS} -Dwarnings" cargo clippy --all-targets

.PHONY: clippy-all-features
clippy-all-features:
	RUSTFLAGS="${RUSTFLAGS} -Dwarnings" cargo clippy --all-targets --all-features

.PHONY: clippy-cpp-rust
clippy-cpp-rust:
	RUSTFLAGS="${RUSTFLAGS} --cfg cpp_rust_unstable -Dwarnings" cargo clippy --all-targets --all-features


.PHONY: test
test: up
	SCYLLA_URI=172.42.0.2:9042 \
	 SCYLLA_URI2=172.42.0.3:9042 \
	 SCYLLA_URI3=172.42.0.4:9042 \
	 cargo test --all-features

.PHONY: ccm-test
ccm-test:
	RUSTFLAGS="${RUSTFLAGS} --cfg ccm_tests" cargo test --all-features --test integration ccm

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
