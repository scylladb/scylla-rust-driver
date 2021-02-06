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

Tests are split into two categories: those which don't need a running Scylla instance to run, and those that do. Tests from the second category should be marked with `#[ignored]` attribute - our CI doesn't start Scylla during the test phase, so they need to be marked as ignored for the CI check to pass. This will be fixed in the future.

To run tests which don't need the database, use `cargo test`.

To run all tests, use:

```bash
SCYLLA_URI=<ip and port of your Scylla instance> cargo test -- --ignored --test-threads=1
```

Until the CI is fixed, __please__ make sure to run all tests before submitting the PR. The easiest way to setup a running Scylla instance is to use the [Scylla Docker image](https://hub.docker.com/r/scylladb/scylla/):

```bash
# Downloads and runs Scylla in Docker
docker run --name scylla-ci -d scylladb/scylla

# Run all tests
SCYLLA_URI="$(docker inspect --format='{{ .NetworkSettings.IPAddress }}' scylla-ci):19042" cargo test -- --ignored --test-threads=1
```
