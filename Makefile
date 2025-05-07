COMPOSE := docker compose -f test/cluster/docker-compose.yml

.PHONY: all
all: test

.PHONY: static
static: fmt-check check check-without-features check-all-features clippy clippy-all-features clippy-cpp-rust

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
	cargo check -p scylla --features "" --all-targets

.PHONY: check-all-features
check-all-features:
	cargo check --all-targets --all-features

.PHONY: clippy
clippy:
	RUSTFLAGS=-Dwarnings cargo clippy --all-targets

.PHONY: clippy-all-features
clippy-all-features:
	RUSTFLAGS=-Dwarnings cargo clippy --all-targets --all-features

.PHONY: clippy-cpp-rust
clippy-cpp-rust:
	RUSTFLAGS="--cfg cpp_rust_unstable -Dwarnings" cargo clippy --all-targets --all-features


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

# Msrv-related items. Helpful when verifying changes to Cargo.toml
# and updating MSRV.
.PHONY: use_cargo_lock_msrv
use_cargo_lock_msrv:
	mv Cargo.lock Cargo.lock.bak
	mv Cargo.lock.msrv Cargo.lock

.PHONY: restore_cargo_lock
restore_cargo_lock:
	mv Cargo.lock Cargo.lock.msrv
	mv Cargo.lock.bak Cargo.lock

.PHONY: test_cargo_lock_msrv
test_cargo_lock_msrv: use_cargo_lock_msrv check restore_cargo_lock

export RUSTFLAGS=-Dwarnings

.PHONY: static_full
static_full:
	cargo fmt --all -- --check
	cargo clippy --all-targets
	cargo clippy --all-targets --all-features
	cargo clippy --all-targets -p scylla-cql --features "full-serialization"
	RUSTFLAGS="--cfg cpp_rust_unstable -Dwarnings" cargo clippy --all-targets --all-features
	cargo check --all-targets -p scylla --features ""
	cargo check --all-targets -p scylla --all-features
	cargo check --all-targets -p scylla --features "full-serialization"
	cargo check --all-targets -p scylla --features "metrics"
	cargo check --all-targets -p scylla --features "secrecy-08"
	cargo check --all-targets -p scylla --features "chrono-04"
	cargo check --all-targets -p scylla --features "time-03"
	cargo check --all-targets -p scylla --features "num-bigint-03"
	cargo check --all-targets -p scylla --features "num-bigint-04"
	cargo check --all-targets -p scylla --features "bigdecimal-04"
	cargo check --all-targets -p scylla --features "openssl-010"
	cargo check --all-targets -p scylla --features "rustls-023"
	cargo check --features "openssl-010" --features "rustls-023"

.PHONY: msrv_static
msrv_static:
	RUSTFLAGS=-Dwarnings cargo check --all-targets --all-features --locked
	RUSTFLAGS=-Dwarnings cargo check --all-targets --locked -p scylla
	RUSTFLAGS=-Dwarnings cargo check --all-targets --locked -p scylla-cql
