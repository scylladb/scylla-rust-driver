# Contributing to scylla-rust-driver

Thank you for your interest in contributing to our driver!

## Pre-review checklist

Before submitting a PR with your patch for review, make sure it will satisfy the following requirements:

- Your patch is split into commits. Each commit introduces one, logically coherent change to the codebase.
- Commit messages should clearly explain what is being changed in the commit, and what is the reason for the change.
- New features and bug fixes are covered with tests.
- Every commit in your patch compiles, passes [static checks](#static-checks) and passes all [tests](#testing).
- The description of your PR explains the reason and motivation for the patch.
- If patch fixes an issue, there should be a `Fixes: #XYZ` line at the end of PR's description.

In case any of those requirements can't be met, please include the reason for this in your PR's description. A maintainer can make an exception and merge the PR if the reason is justified.

## Review and merging

After submitting a PR which meets all criteria from the previous section, it will be reviewed by one or more maintainers. When the maintainers become satisfied with your contribution, one of them will merge it.

Currently, the list of people maintaining the driver include:

- Wojciech Przytuła (@wprzytula)
- Karol Baryła (@Lorak-mmk)

## Static checks

Currently, we require new PRs to compile without warnings, pass `cargo fmt` and a few variations of `cargo clippy` checks. You can run `make static` to execute most of those static checks.

## Testing

We run tests using [cargo-nextest](https://nexte.st/). See its website for installation instructions.
`cargo test` should work, but we don't test it in CI, and it may have different default behaviors.
For example, `cargo test` will run ccm tests (more on them below) by default.

Some tests require a live database.
Most of them live in `scylla/tests/integration`, but there are still a few leftovers in `scylla/src`.
To run them, you need a 3-node DB cluster. The simplest way to set it up locally is to use a `docker compose`.
Fortunately there is no need to invoke `docker compose` manually, everything can be handled by our `Makefile`.

To run a cargo test suite, use the command below (note that you must have Docker, Docker Compose V2 (at least v2.20), and [cargo-nextest](https://nexte.st/) installed):
```bash
make test
```

The ScyllaDB version used for testing is pinned in `scylla_version.env` in the repository root, and Renovate keeps it up to date.
That single file pins the version for both the `docker compose` cluster and the ccm tests.
It must be a full, three-component version (e.g. `2026.2.2`) rather than a truncated one (e.g. `2026.2`) - otherwise CCM would resolve the full version
on each call, making the tests take much more time.
To run the `docker compose` cluster on a different version ad hoc, set `SCYLLA_VERSION` in your environment - it takes precedence over the value from the file.

When on non-Linux machine, however, it can be impossible to connect to containerized ScyllaDB instance from outside Docker.\
If you are using macOS, we provide a `dockerized-test` make target for running tests inside another Docker container:
```bash
make dockerized-test
```
If working on Windows, run tests in WSL.

The above commands will leave a running ScyllaDB cluster in the background.
To stop it, use `make down`.\
Starting a cluster without running any test is possible with `make up`.

The above test commands will run doctests, unit tests and integration tests.
There is a third category: ccm tests. They live in `scylla/tests/integration/ccm`. Those tests setup their own clusters, in order to test
topology changes, or custom configurations. To run them you need to have [scylla-ccm](https://github.com/scylladb/scylla-ccm)
installed. Easiest way is to install it using uv: `uv tool install git+https://github.com/scylladb/scylla-ccm.git`
You can then execute those tests with `make ccm-test`.
By default they use the version from `scylla_version.env`, prefixed with `release:`.
To point them at a different version ad hoc, set `SCYLLA_TEST_CLUSTER` to a full ccm version string, e.g. `release:2026.1.0` or `unstable/master:<id>`.

### Measuring code coverage

We use [cargo-llvm-cov](https://github.com/taiki-e/cargo-llvm-cov) for code coverage: it's LLVM
source-based coverage, so unlike ptrace-based tools it handles this driver's async,
multi-threaded tests correctly. Install it (and cargo-nextest, if you haven't already) with:
```bash
cargo install cargo-llvm-cov cargo-nextest --locked
```

Run the coverage-instrumented test suites, then generate the report:
```bash
make test-coverage       # doctests, unit tests and integration tests, same scope as `make test`
make ccm-test-coverage   # ccm tests, same scope as `make ccm-test`; accumulates onto the above
make coverage-report
```
`coverage-report` prints a per-file summary and writes an HTML report to `target/llvm-cov/html/index.html`
(open it in a browser for a line-by-line view) and an lcov file to `target/llvm-cov/lcov.info`.
`make clean-coverage` resets the collected data.

Doctests aren't measured -- cargo-llvm-cov's doctest coverage support requires nightly Rust, and
this repo targets stable -- but `test-coverage` still runs them for correctness, same as `test` does.

### Writing tests that need to connect to Scylla

If you test requires connecting to Scylla, there are a few things you should consider.

1. Such tests are considered integration tests and should be placed in `scylla/tests/integration`.
2. To avoid name conflicts while creating a keyspace use `unique_keyspace_name` function from `utils` module.
3. This `utils` module (`scylla/tests/integration/utils.rs`) contains other functions that may be helpful for writing tests.
   For example `create_new_session_builder` or `test_with_3_node_cluster`.
4. To perform DDL queries (creating / altering / dropping a keyspace / table /type) use `ddl` method from the utils module.
   To do this, import the `PerformDDL` trait (`use crate::utils::PerformDDL;`). Then you can call `ddl` method on a
   `Session`.

### Tracing in tests

By default cargo captures `print!` macro's output from tests and prints them for failed tests.
This is a bit problematic for us in case of `tracing` crate logs, because traces are not printed
unless a subscriber is set. That's why we have a helper function for tests: `setup_tracing`.
It sets up a tracing subscriber with env filter (so you can filter traces using env variables)
and with a Writer that is compatible with test framework's output capturing.

Most of the tests already call this function, and any new tests should too.
If you want to see tracing output from a failing test and it doesn't call this function,
simply add the call at the beginning of the test.

## CI

Before sending a pull request, it is a good idea to run `make ci` locally (or `make dockerized-ci` if on macOS).
It will perform a format check, `cargo check`, linter check (clippy), build and `cargo nextest`.

### Semver checking

Our CI runs cargo semver-checks and labels PRs that introduce breaking changes.
If you don't intend to change public API, you can perform the checks locally,
using command `make semver-rev`. Make sure you have semver-checks installed first,
you can install it using `cargo install cargo-semver-checks`.

`make semver-rev` will check for API breaking changes using `main` branch as baseline.
To use different branch / commit call `make semver-rev rev=BRANCH`.

The tool is NOT perfect and only checks some aspect of semver-compatibility.
It is NOT a replacement for a human reviewer, it is only supposed to help them
and catch some erros that they might have missed.

Tool that we curently use: https://github.com/obi1kenobi/cargo-semver-checks

## Contributing to the book

The documentation book is written using [mdbook](https://github.com/rust-lang/mdBook)\
Book source is in `docs/source`\
This source has to be compatible with `Sphinx` so it might sometimes contain chunks like:
````
```{eval-rst}
something
```
````
But they are removed when building the book


`mdbook` can be installed using:
```shell
cargo install mdbook
```

Book build process uses preprocessor to remove Sphinx artifacts.
Due to limitation of mdbook, it can only be built either from main directory,
using `mdbook X docs` or from `docs` directory, using `mdbook X`, where
`X` is mdbook command such as `build` / `serve` / `test` etc.

If the book is built from another directory (e.g. scylla, using `mdbook build ../docs`),
preprocessor won't be found, so the result will contain Sphinx artifacts.

Build the book.
```bash
mdbook build docs
# HTML will be in docs/book
```


Or serve it on a local http server (automatically refreshes on changes)
```bash
mdbook serve docs
```

Test code examples in the book are tested as doctests in the `scylla` crate.
See `book_tests.rs` file. To run those tests:
```bash
cargo test --doc -p scylla --all-features
```

If you add, remove, move, or rename book chapters, regenerate the book test module:
```bash
make regenerate-book-tests
```

To check whether the generated file is up to date:
```bash
make check-book-tests
```
