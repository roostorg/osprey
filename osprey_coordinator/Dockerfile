# Base images are pinned by multi-arch index digest; Dependabot proposes new digests.
FROM rust:1.91@sha256:867f1d1162913c401378a8504fb17fe2032c760dc316448766f150a130204aad AS build

RUN apt-get update && apt-get install -y protobuf-compiler

ADD ./proto/ /osprey/proto/
ADD ./osprey_coordinator/ /osprey/osprey_coordinator/

WORKDIR /osprey/osprey_coordinator


RUN rustup show
RUN rustc --version

RUN cargo build --release --locked

# Same Debian release as the build stage, so the binary's glibc and OpenSSL match.
FROM debian:trixie-slim@sha256:f6b08b09a05725f9431502d3b5714f5c4576e82c1a84fa1debe0c24a7e0c3ec4

# Shared libraries the binary links against, plus the system CA store for TLS.
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates libssl3t64 zlib1g libzstd1 && \
    rm -rf /var/lib/apt/lists/*

COPY --from=build /osprey/osprey_coordinator/target/release/osprey_coordinator /osprey_coordinator_bin

ENTRYPOINT ["/osprey_coordinator_bin"]
