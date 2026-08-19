COMPOSE := docker compose -f test/cluster/docker-compose.yml
RUSTFLAGS := ${RUSTFLAGS} --cfg scylla_unstable
export RUSTFLAGS

.PHONY: all
all: test

.PHONY: static
static: fmt-check check check-without-features check-without-unstable check-without-unstable-and-features check-all-features clippy clippy-all-features check-book-tests check-rustdoc-leaks check-cql-imports

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


.PHONY: check-cql-imports
check-cql-imports:
	./scripts/check-cql-imports.sh

.PHONY: check-rustdoc-leaks
check-rustdoc-leaks:
	RUSTDOCFLAGS="-Zunstable-options" cargo +nightly rustdoc -p scylla -- --output-format json
	python3 ./scripts/check-rustdoc-cql-leaks.py target/doc/scylla.json

.PHONY: test
test: up
	# We need to run doctests separately, because nextest doesn't support them :(
	# https://github.com/nextest-rs/nextest/issues/16
	cargo test --doc --all-features
	cargo nextest run --all-features

.PHONY: ccm-test
ccm-test:
	cargo nextest run --all-features -E 'test(ccm::)' --ignore-default-filter --status-level pass

# Coverage-instrumented counterparts of test/ccm-test, using cargo-llvm-cov
# (LLVM source-based coverage -- unlike ptrace-based tools it handles async,
# multi-threaded code correctly). test-coverage starts from a clean slate;
# ccm-test-coverage deliberately does not clean, so it accumulates onto
# whatever test-coverage already collected instead of replacing it. Run
# test-coverage first, then optionally ccm-test-coverage, then coverage-report.
#
# Everything here runs on the nightly toolchain (+nightly), independently of
# whatever toolchain is the ambient default: nightly is required to include
# doctests in the coverage data (cargo-llvm-cov's doctest support is
# nightly-only), and mixing coverage data recorded by different toolchains'
# bundled LLVM versions is not something to rely on, so every instrumented
# run and every read of that data (report/clean) uses the same toolchain.
# This does not affect `test`/`ccm-test`, which keep using the default
# (stable) toolchain as before. Requires nightly + its llvm-tools-preview
# component locally: `rustup toolchain install nightly --component
# llvm-tools-preview`.
#
# --no-fail-fast matters here specifically: nextest's default is to stop the
# whole run after the first failing binary. With --no-report, that means a
# single failure can throw away coverage data for everything that would have
# run after it, not just fail that one test.
.PHONY: test-coverage
test-coverage: up
	cargo +nightly llvm-cov clean --workspace
	cargo +nightly llvm-cov nextest --all-features --no-report --no-fail-fast
	cargo +nightly llvm-cov --no-report --doc --all-features

.PHONY: ccm-test-coverage
ccm-test-coverage:
	cargo +nightly llvm-cov nextest --all-features --no-report --no-fail-fast -E 'test(ccm::)' --ignore-default-filter --status-level pass

.PHONY: coverage-report
coverage-report:
	mkdir -p target/llvm-cov
	cargo +nightly llvm-cov report
	cargo +nightly llvm-cov report --html --output-dir target/llvm-cov
	cargo +nightly llvm-cov report --lcov --output-path target/llvm-cov/lcov.info

.PHONY: clean-coverage
clean-coverage:
	cargo +nightly llvm-cov clean --workspace
	rm -rf target/llvm-cov

.PHONY: run-examples
run-examples: up
	./scripts/run-examples.sh

.PHONY: dockerized-test
dockerized-test: up
	test/dockerized/run.sh

.PHONY: build
build:
	cargo build --examples --benches

.PHONY: bench-baseline
bench-baseline: up
	# Run the driver benchmarks and store the results as the baseline named
	# "base" to compare against later (e.g. before applying your changes).
	cargo bench -p benchmarks --bench requests -- --save-baseline=base

.PHONY: bench
bench: up
	# Run the driver benchmarks and compare against the "base" baseline saved
	# by `make bench-baseline` (without overwriting it).
	cargo bench -p benchmarks --bench requests -- --baseline=base

.PHONY: docs
docs:
	mdbook build docs

.PHONY: check-book-tests
check-book-tests:
	cargo run -p generate_book_tests -- --check

.PHONY: regenerate-book-tests
regenerate-book-tests:
	cargo run -p generate_book_tests

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

.PHONY: stop
stop:
	$(COMPOSE) stop

.PHONY: print-logs
print-logs:
	$(COMPOSE) logs

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
