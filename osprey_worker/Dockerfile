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

# Create workspace structure with pyproject.toml files for uv sync to work.
# osprey_async_worker is a uv workspace member, so its pyproject must be present
# for `uv sync --locked` to validate the lockfile. This (stable, gevent) image
# does NOT install it — see --no-install-package below. The experimental asyncio
# worker ships in its own image: osprey_async_worker/Dockerfile.
ADD osprey_rpc/pyproject.toml /osprey/osprey_rpc/pyproject.toml
ADD osprey_worker/pyproject.toml /osprey/osprey_worker/pyproject.toml
ADD osprey_async_worker/pyproject.toml /osprey/osprey_async_worker/pyproject.toml
ADD example_plugins/pyproject.toml /osprey/example_plugins/pyproject.toml
ADD example_atproto_plugins/pyproject.toml /osprey/example_atproto_plugins/pyproject.toml

# Install third-party dependencies only (this layer will be cached when only source code
# changes). The workspace members are installed after their sources are copied, because an
# editable install built against an empty tree maps no packages and `import osprey` then fails.
RUN pip install --upgrade pip uv && \
    uv sync --frozen --no-install-workspace --python=$(which python3.11) && \
    pip cache purge && uv cache clean

# https://tld.readthedocs.io/en/latest/#update-the-list-of-tld-names
RUN . .venv/bin/activate && update-tld-names

# Add source code after dependencies are installed (osprey_async_worker is
# intentionally omitted — it is not installed in this image).
ADD osprey_worker /osprey/osprey_worker
ADD osprey_rpc /osprey/osprey_rpc
ADD example_plugins /osprey/example_plugins
ADD example_atproto_plugins /osprey/example_atproto_plugins

# Install the workspace members. Exclude osprey_async_worker: the asyncio worker is
# experimental and lives in its own image, so the gevent worker stays free of it
# (osprey.async_worker is not importable here, so execution_context's optional async
# import cleanly falls back to None).
RUN uv sync --frozen --no-install-package osprey-async-worker --python=$(which python3.11) && \
    uv cache clean

# The editable builds write src/*.egg-info, which uv then reads as a source change, so
# without this every `uv run` rebuilds the workspace and needs PyPI at container start.
# A changed member pyproject.toml (dependencies, scripts, entry points) needs an image
# rebuild, also under the source bind mounts in docker-compose.yaml.
ENV UV_NO_SYNC=1

# Rules come after the workspace install, so a rules-only change does not rerun it.
ADD example_rules /osprey/example_rules
ADD example_atproto_rules /osprey/example_atproto_rules

COPY entrypoint.sh /osprey/entrypoint.sh

ENTRYPOINT ["/osprey/entrypoint.sh"]
