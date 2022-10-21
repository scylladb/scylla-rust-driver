#!/usr/bin/env bash

set -e

echo "Waiting for the cluster to become ready."
echo " Inspect 'make logs' output if this step takes too long."

DOCKER_COMPOSE='docker-compose -f test/docker-compose.yml'
TEST_QUERY='select * from system.local'

for NODE in 'scylla1' 'scylla2' 'scylla3'; do
    until $DOCKER_COMPOSE exec "$NODE" cqlsh -e "$TEST_QUERY" > /dev/null; do
        printf '.';
        sleep 1;
    done
done

echo "Done waiting."
