#!/usr/bin/env bash
#
# Runs every example from the `examples` package, in one pass.
#
# Most examples talk to an already-running cluster, selected by SCYLLA_URI
# (default 172.42.0.2:9042 - the cluster `make up` starts); a few bring up a
# throwaway cluster of their own through `scylla-ccm-bridge` and ignore
# SCYLLA_URI. Running the examples therefore needs both a cluster and `ccm`
# installed - but this script does not need to tell the two kinds apart, it just
# runs them all.
#
# See examples/README.md.

set -euo pipefail

cd "$(dirname "$0")/.."

if [ "$#" -gt 0 ]; then
    echo "ERROR: unexpected argument: $1" >&2
    echo "Usage: $0" >&2
    echo "  Runs every example; takes no arguments." >&2
    exit 1
fi

# A few examples start a cluster of their own, so ccm is a prerequisite of the
# whole run. Say so here: without this, the first such example dies inside the
# driver with an opaque spawn failure and nothing names the real cause.
if ! command -v ccm > /dev/null; then
    echo "ERROR: ccm is not installed, and some examples start a cluster of their own." >&2
    echo "See examples/README.md; install it with:" >&2
    echo "  uv tool install \"git+https://github.com/scylladb/scylla-ccm.git\"" >&2
    exit 1
fi

# Ask cargo for the full set of example targets instead of maintaining a list,
# so that a newly added example is run without any further wiring.
all_examples=$(cargo metadata --no-deps --format-version 1 | python3 -c '
import json
import sys

metadata = json.load(sys.stdin)
packages = [p for p in metadata["packages"] if p["name"] == "examples"]
if len(packages) != 1:
    sys.exit(
        "expected exactly one package named `examples` in cargo metadata, "
        f"found {len(packages)}"
    )

names = sorted(t["name"] for t in packages[0]["targets"] if "example" in t["kind"])
if not names:
    sys.exit("cargo metadata reported no example targets for package `examples`")

print("\n".join(names))
')
mapfile -t examples <<< "$all_examples"

# Matches both the examples' own default and the cluster from `make up`.
export SCYLLA_URI="${SCYLLA_URI:-172.42.0.2:9042}"

total=${#examples[@]}

echo "Running $total example(s); SCYLLA_URI=$SCYLLA_URI"
echo

# Build up front, so that compilation time is not attributed to the first
# example and so that a compile error is reported before anything runs.
echo "Building examples ..."
cargo build -p examples --examples
echo

ran=0
for example in "${examples[@]}"; do
    ran=$((ran + 1))
    echo "=== [$ran/$total] $example"
    # stdin from /dev/null: it keeps the examples non-interactive, and it is
    # what makes the interactive `cqlsh_rs` REPL see EOF and terminate.
    if ! cargo run -p examples --example "$example" < /dev/null; then
        echo
        echo "FAILED: example '$example' exited with a non-zero status." >&2
        echo "Ran $ran of $total example(s) before failing." >&2
        exit 1
    fi
    echo
done

echo "OK: all $total example(s) passed."
