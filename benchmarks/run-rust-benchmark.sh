#!/usr/bin/env bash

# Wrapper on the command for the drivers benchmark runner
# Usage: run-rust-benchmark.sh <binary-name> <N>
set -euo pipefail

BINARY="$1"
N="$2"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$SCRIPT_DIR/.."

CNT="$N" "$REPO_ROOT/target/release/$BINARY"
