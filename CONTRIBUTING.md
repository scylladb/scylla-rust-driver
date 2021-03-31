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

- Piotr Sarna (@psarna)
- Piotr Dulikowski (@piodul)

## Static checks

Currently, we require new PRs to compile without warnings, pass `cargo fmt` and `cargo clippy` checks.

## Testing

The easiest way to setup a running Scylla instance is to use the [Scylla Docker image](https://hub.docker.com/r/scylladb/scylla/):
You need a running Scylla instance for the tests. 

Execute the commands below to run the tests:

```bash
# Downloads and runs Scylla in Docker
docker run --name scylla-ci -d scylladb/scylla

# Run all tests
SCYLLA_URI="$(docker inspect --format='{{ .NetworkSettings.IPAddress }}' scylla-ci):19042" cargo test -- --test-threads=1
```

## Contributing to the book

The documentation book is written using [mdbook](https://github.com/rust-lang/mdBook)  
Book source is in `book/src`

`mdbook` can be installed using:
```shell
cargo install mdbook
```

Build the book:
```bash
mdbook build book
# HTML will be in book/book
```

Or serve it on a local http server (automatically refreshes on changes)
```bash
mdbook serve
```

Test code examples (requires a running scylla instance):
```bash
# Make a clean debug build, otherwise mdbook has problems with multiple versions
cargo clean
cargo build

mdbook test -L target/debug/deps/ book
```