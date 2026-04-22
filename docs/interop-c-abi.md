# Arrow C ABI Interop Guide

This guide explains how `zarrow_c` interoperates with the Arrow C Data / C Stream ABI and how to run local smoke checks.

## Scope

Public C header:

- `include/zarrow_c_api.h`

Supported object boundaries:

- `ArrowSchema`
- `ArrowArray` (with imported schema handle)
- `ArrowArrayStream`

## Streaming Semantics

`zarrow_c_import_stream` and `zarrow_c_export_stream` are streaming-preserving operations:

- no full materialization into an in-memory batch list;
- original `ArrowArrayStream` callbacks are transferred and forwarded lazily;
- this avoids large memory spikes for long streams.

## Ownership Rules

- `zarrow_c_import_*` transfers ownership of input Arrow C structs into a zarrow handle.
- `zarrow_c_export_*` transfers ownership from handle to output Arrow C structs.
- release zarrow handles with:
  - `zarrow_c_release_schema`
  - `zarrow_c_release_array`
  - `zarrow_c_release_stream`
- release exported Arrow C structs using their own `release` callback.

For stream handles, export is one-shot: exporting again after ownership is moved out returns `ZARROW_C_STATUS_RELEASED`.

## Local Smoke Checks

Run from repository root.

### 1) Build shared library

```bash
zig build c-api-lib
```

### 2) Zig roundtrip example

```bash
zig build example-c_abi_roundtrip
```

### 3) Rust smoke (`zarrow_c <-> arrow-rs`)

```bash
export LD_LIBRARY_PATH="$PWD/zig-out/lib:${LD_LIBRARY_PATH:-}"
export RUSTFLAGS="-L native=$PWD/zig-out/lib"
cargo run --manifest-path tools/interop/arrow-rs/Cargo.toml --bin c_abi_smoke
```

### 4) C++ smoke (`zarrow_c <-> Arrow C++`)

Requires Arrow C++ development environment and `pkg-config` entry for `arrow`.

```bash
mkdir -p .interop-bin
c++ -std=c++17 tools/interop/cpp/c_abi_smoke.cpp \
  -Iinclude \
  -Lzig-out/lib \
  -Wl,-rpath,"$PWD/zig-out/lib" \
  -lzarrow_c \
  -o .interop-bin/c_abi_smoke \
  $(pkg-config --cflags --libs arrow)

export LD_LIBRARY_PATH="$PWD/zig-out/lib:${LD_LIBRARY_PATH:-}"
./.interop-bin/c_abi_smoke
```

## CI Lanes

In `.github/workflows/ci.yml`:

- `interop-c-abi-cpp-smoke`
- `interop-c-abi-rs-smoke`

## Troubleshooting

`cargo run could not determine which binary to run`:

- use `--bin c_abi_smoke` or `--bin zarrow-interop-arrow-rs`.

Shared library not found at runtime:

- ensure `LD_LIBRARY_PATH` includes `$PWD/zig-out/lib`.
