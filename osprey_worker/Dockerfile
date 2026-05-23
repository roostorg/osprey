# syntax=docker/dockerfile:1
FROM python:3.11-slim
ARG TARGETPLATFORM

WORKDIR /osprey
ENV ETCD_PEERS=http://etcd:2379
ENV PYTHONPATH=/osprey

ENV PORT=5000
EXPOSE ${PORT}

RUN set -ex && \
    apt-get update && \
    apt-get install -yqq --no-install-recommends libjemalloc-dev git gcc g++ wget ssh && \
    apt-get clean && \
    apt-get -y purge wget && \
    apt-get -y autoremove && \
    rm -fr /var/cache/apt/archives/* && \
    rm -rf /var/lib/apt/lists/*

# Hack to get the right libjemalloc location
ENV LIB_ARCH=${TARGETPLATFORM##*/}
ENV LIB_ARCH=${LIB_ARCH/amd64/x86_64-linux-gnu}
ENV LIB_ARCH=${LIB_ARCH/arm64/aarch64-linux-gnu}
ENV LD_PRELOAD="/usr/lib/${LIB_ARCH}/libjemalloc.so"
ENV MALLOC_CONF="narenas:4"

# Set up python environment
ADD uv.lock /osprey/uv.lock
ADD pyproject.toml /osprey/pyproject.toml
ADD README.md /osprey/README.md
ADD LICENSE.md /osprey/LICENSE.md

# Create workspace structure with pyproject.toml files for uv sync to work
ADD osprey_rpc/pyproject.toml /osprey/osprey_rpc/pyproject.toml
ADD osprey_worker/pyproject.toml /osprey/osprey_worker/pyproject.toml
ADD example_plugins/pyproject.toml /osprey/example_plugins/pyproject.toml

# Create minimal package structure required by uv
RUN mkdir -p /osprey/osprey_worker /osprey/osprey_rpc /osprey/example_plugins/src && \
    touch /osprey/osprey_worker/__init__.py /osprey/osprey_rpc/__init__.py /osprey/example_plugins/src/__init__.py

# Install dependencies first (this layer will be cached when only source code changes)
RUN pip install --upgrade pip uv && \
    uv sync --locked --python=$(which python3.11) && \
    pip cache purge && uv cache clean

# https://tld.readthedocs.io/en/latest/#update-the-list-of-tld-names
RUN . .venv/bin/activate && update-tld-names

# Add source code after dependencies are installed
ADD example_rules /osprey/example_rules
ADD osprey_worker /osprey/osprey_worker
ADD osprey_rpc /osprey/osprey_rpc
ADD example_plugins /osprey/example_plugins

COPY entrypoint.sh /osprey/entrypoint.sh

ENTRYPOINT ["/osprey/entrypoint.sh"]
