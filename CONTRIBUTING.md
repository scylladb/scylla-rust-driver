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

Currently, we require new PRs to compile without warnings, pass `cargo fmt` and `cargo clippy` checks.

## Testing

A 3-node ScyllaDB cluster is required to run the tests.
The simplest way to set it up locally is to use a `docker compose`.
Fortunately there is no need to invoke `docker compose` manually, everything can be handled by our `Makefile`.

To run a cargo test suite, use the command below (note that you must have Docker and Docker Compose V2 installed):
```bash
make test
```
When on non-Linux machine, however, it can be impossible to connect to containerized Scylla instance from outside Docker.\
If you are using macOS, we provide a `dockerized-test` make target for running tests inside another Docker container:
```bash
make dockerized-test
```
If working on Windows, run tests in WSL.

The above commands will leave a running ScyllaDB cluster in the background.
To stop it, use `make down`.\
Starting a cluster without running any test is possible with `make up`.

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
It will perform a format check, `cargo check`, linter check (clippy), build and `cargo test`.

### min_rust workflow and Cargo.lock.msrv

There is min_rust job defined in rust.yml workflow that checks if the driver compiles with our current MSRV.
Bumping MSRV is generally not considered a breaking change, so our dependencies are free to do it,
and do it regularly. This resulted in failures in this job.
We could pin versions of problematic crates in `Cargo.toml`, but it would affect version selection
for client applications. It would also force people that use non-ancient versions of Rust to use older
versions of dependencies, which is not desirable.

We opted for a different approach. There is `Cargo.lock.msrv` file in repository, which is used only by min_rust job - 
it is renamed to `Cargo.lock` before building the driver with `--locked` flag.

This solution is good for us (because we don't have to fix breakage so often) and for users of the driver on modern versions
of Rust - for them the driver will just work.
Users on old versions of Rust will potentially have to pin indirect dependencies to specific versions - but that shouldn't be
that much of a problem, it's just few `cargo update` commands.

If your PR added / removed / updated a dependency, you will need to also update `Cargo.lock.msrv` file.
There are a few scenarios:
 - If you just bumped version of one of crates in the workspace: rename `Cargo.lock.msrv` to `Cargo.lock`,
    run `cargo check --all-targets --all-features --offline`, rename the file back.
 - If you added / removed / updated dependency in one of `Cargo.toml` files it should be enough to
    rename `Cargo.lock.msrv` to `Cargo.lock`, run `cargo check --all-targets --all-features`
    and rename the file back.
 - If previous methods didn't work or you want to recreate the file (which we should do once in a while):
      1. Switch to MSRV version (e.g. `rustup install <current-msrv> && rustup default <current-msrv>`). Check `README.md` file for current MSRV.
      2. Remove your `Cargo.lock`
      3. Run `cargo check --all-targets --all-features`
      4. If you got an error about one of dependencies not working with this version of Rust, update it's version in `Cargo.lock`,
         using command like `cargo update -p toml_datetime --precise 0.6.3` and go back to step 3.
      5. Rename `Cargo.lock` to `Cargo.lock.msrv`.

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

Test code examples (requires a running scylla instance):
```bash
# Make a clean debug build, otherwise mdbook has problems with multiple versions
cargo clean
cargo build --examples

mdbook test -L target/debug/deps/ docs
```
