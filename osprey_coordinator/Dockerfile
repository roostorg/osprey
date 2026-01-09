FROM rust:1.91

RUN apt-get update && apt-get install -y protobuf-compiler

ADD ./proto/ /osprey/proto/
ADD ./osprey_coordinator/ /osprey/osprey_coordinator/

WORKDIR /osprey/osprey_coordinator


RUN rustup show
RUN rustc --version

RUN cargo build --release

RUN mv /osprey/osprey_coordinator/target/release/osprey_coordinator /osprey_coordinator_bin && \
    rm -rf /osprey

ENTRYPOINT ["/osprey_coordinator_bin"]
