#!/usr/bin/env bash

cd "$(dirname "$0")"/../../

if [[ -z $(docker container ls | grep cluster_scylla) ]]; then
    echo "Scylla container must be running to execute tests (use 'make up')."
    exit 1
fi

IMAGE_NAME="scylla_rust_driver_testing"

# Build a new image with embeded driver source files and deletes the
# previously built image
docker tag "$IMAGE_NAME:latest" "$IMAGE_NAME:previous" &>/dev/null
if docker build -f test/dockerized/Dockerfile -t "$IMAGE_NAME:latest" . ; then
    docker rmi "$IMAGE_NAME:previous" &>/dev/null
else
    docker tag "$IMAGE_NAME:previous" "$IMAGE_NAME:latest" &>/dev/null
fi

docker run --name "scylla-rust-driver-testing" \
    --network scylla_rust_driver_public \
    -it --rm \
    --env SCYLLA_URI=172.42.0.2:9042 \
    --env SCYLLA_URI2=172.42.0.3:9042 \
    --env SCYLLA_URI3=172.42.0.4:9042 \
    "$IMAGE_NAME:latest" \
    cargo test
