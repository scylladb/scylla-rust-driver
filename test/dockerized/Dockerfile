FROM rust:slim

RUN apt-get update && apt-get install -y pkg-config libssl-dev git

# Cache dependencies & crates.io index by invoking cargo fetch on freshly cloned repository
WORKDIR /tmp/driver
RUN git clone --depth 1 https://github.com/scylladb/scylla-rust-driver /tmp/driver
RUN cargo fetch
RUN rm -rf /tmp/driver

WORKDIR /driver
COPY . .
