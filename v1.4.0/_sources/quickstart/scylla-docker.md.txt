# Running ScyllaDB using Docker

To make queries we will need a running ScyllaDB instance. The easiest way is to use a [Docker](https://www.docker.com/) image.\
Please [install Docker](https://docs.docker.com/engine/install) if it's not installed.

### Running scylla
To start ScyllaDB run:
```bash
# on Linux sudo might be required
docker run --rm -it -p 9042:9042 scylladb/scylla --smp 2
```

Docker will download the image, then after minute or two there should be a message like:
```shell
Starting listening for CQL clients on 172.17.0.2:9042
```
This means that ScyllaDB is ready to receive queries

To stop this instance press `Ctrl + C`

### More information
More information about this image can be found on [dockerhub](https://hub.docker.com/r/scylladb/scylla)
