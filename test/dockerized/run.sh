#!/usr/bin/env bash

cd "$(dirname "$0")"

if [[ -z $(docker container ls | grep cluster_scylla) ]]; then
    echo "Scylla container must be running to execute tests (use 'make up')."
    exit 1
fi

if ! [[ $(docker image ls -q scylla_rust_driver_testing) ]]; then
    docker build -t scylla_rust_driver_testing .
fi

if [ -z "$CARGO_HOME" ]; then
    CARGO_REGISTRY=$HOME/.cargo/registry
else
    CARGO_REGISTRY=$CARGO_HOME/registry
fi

docker run --name "scylla-rust-driver-testing" \
    --network scylla_rust_driver_public \
    -v "$(pwd)/../..:/scylla_driver:Z" \
    -v "$CARGO_REGISTRY:/usr/local/cargo/registry:z" \
    -it --rm \
    --env CARGO_HOME=/scylla_driver/.cargo_home \
    --env SCYLLA_URI=172.42.0.2:9042 \
    --env SCYLLA_URI2=172.42.0.3:9042 \
    --env SCYLLA_URI3=172.42.0.4:9042 \
    -w /scylla_driver \
    scylla_rust_driver_testing \
    cargo test
