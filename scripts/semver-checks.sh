#!/bin/sh

set -x

cargo semver-checks -p scylla -p scylla-cql -p scylla-cql-core $@
