# osprey-rpc

Python protobuf and gRPC bindings for Osprey's RPC types: actions, the Osprey Coordinator's services, and the shared message types the worker and coordinator exchange.

The bindings under `src/osprey/rpc/` are generated from the `.proto` files in [`proto/osprey/rpc/`](https://github.com/roostorg/osprey/tree/main/proto/osprey/rpc) by `./gen-protos.sh` at the repository root. Don't edit the generated `*_pb2*.py` and `*_pb2*.pyi` files by hand; change the `.proto` files and regenerate.

Each [Osprey release](https://github.com/roostorg/osprey/releases) attaches this package's source distribution.
