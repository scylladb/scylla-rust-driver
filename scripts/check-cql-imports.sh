#!/bin/sh
#
# Checks that no file in scylla/src/ (except lib.rs) directly imports from
# scylla_cql:: or scylla_cql_core::. All such imports must be centralized
# as re-exports in lib.rs, and other files should use crate:: paths.

set -e

cd "$(dirname "$0")/.."

violations=$(grep -rn --include='*.rs' -E 'scylla_cql(_core)?::' scylla/src/ \
    | grep -v '^scylla/src/lib\.rs:' || true)

if [ -n "$violations" ]; then
    echo "ERROR: Found direct imports from scylla_cql or scylla_cql_core outside of lib.rs."
    echo "All imports from these crates must be centralized in scylla/src/lib.rs."
    echo "Other files should use crate:: paths instead."
    echo ""
    echo "Violations:"
    echo "$violations"
    exit 1
fi

echo "OK: No direct scylla_cql/scylla_cql_core imports found outside lib.rs."
