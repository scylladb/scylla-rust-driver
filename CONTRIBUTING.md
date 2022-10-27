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

Currently, the list of people maintaining the fork include:

- Jan Ciołek (@cvybhu)
- Piotr Dulikowski (@piodul)

## Static checks

Currently, we require new PRs to compile without warnings, pass `cargo fmt` and `cargo clippy` checks.

## Testing

A 3-node ScyllaDB cluster is required to run the tests.
The simplest way to set it up locally is to use a `docker-compose`.
Fortunately there is no need to invoke `docker-compose` manually, everything can be handled by our `Makefile`.

To run a cargo test suite, use the command below (note that you must have docker and docker-compose installed):
```bash
make test
```
When on non-Linux machine, however, it can be impossible to connect to containerized Scylla instance from outside Docker.\
If you are using macOS, we provide a `dockerized-test` make target for running tests inside another Docker container:
```bash
make dockerized-test
````
If working on Windows, run tests in WSL.

The above commands will leave a running ScyllaDB cluster in the background.
To stop it, use `make down`.\
Starting a cluster without running any test is possible with `make up`.

## CI

Before sending a pull request, it is a good idea to run `make ci` locally (or `make dockerized-ci` if on macOS).
It will perform a format check, `cargo check`, linter check (clippy), build and `cargo test`.

## Contributing to the book

The documentation book is written using [mdbook](https://github.com/rust-lang/mdBook)\
Book source is in `docs/source`\
This source has to be compatible with `Sphinx` so it might sometimes contain chunks like:
````
```eval_rst
something
```
````
But they are removed when building the book


`mdbook` can be installed using:
```shell
cargo install mdbook
```

Build the book (simple method, contains `Sphinx` artifacts):
```bash
mdbook build docs
# HTML will be in docs/book
```

To build the release version use a script which automatically removes `Sphinx` chunks:
```bash
python3 docs/build_book.py
# HTML will be in docs/book/scriptbuild/book
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
