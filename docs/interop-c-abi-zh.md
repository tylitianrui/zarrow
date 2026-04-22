# Arrow C ABI 互操作指南

本文说明 `zarrow_c` 与 Arrow C Data / C Stream ABI 的互通方式，以及本地 smoke 验证方法。

## 范围

公开头文件：

- `include/zarrow_c_api.h`

支持的边界对象：

- `ArrowSchema`
- `ArrowArray`（结合导入的 schema handle）
- `ArrowArrayStream`

## Streaming 语义

`zarrow_c_import_stream` 和 `zarrow_c_export_stream` 保持真正流式：

- 不会把整条流一次性物化为内存中的 batch 列表；
- 原始 `ArrowArrayStream` 回调会被转移并惰性转发；
- 对长流可避免明显的内存峰值与额外延迟。

## 所有权规则

- `zarrow_c_import_*`：输入 Arrow C 结构体所有权转移到 zarrow handle。
- `zarrow_c_export_*`：所有权从 handle 转移到输出 Arrow C 结构体。
- 释放 zarrow handle：
  - `zarrow_c_release_schema`
  - `zarrow_c_release_array`
  - `zarrow_c_release_stream`
- 导出的 Arrow C 结构体应由其自身 `release` 回调释放。

对于 stream handle，导出是一次性的：所有权转出后再次导出会返回 `ZARROW_C_STATUS_RELEASED`。

## 本地 Smoke 验证

以下命令在仓库根目录执行。

### 1) 构建共享库

```bash
zig build c-api-lib
```

### 2) Zig 回环示例

```bash
zig build example-c_abi_roundtrip
```

### 3) Rust smoke（`zarrow_c <-> arrow-rs`）

```bash
export LD_LIBRARY_PATH="$PWD/zig-out/lib:${LD_LIBRARY_PATH:-}"
export RUSTFLAGS="-L native=$PWD/zig-out/lib"
cargo run --manifest-path tools/interop/arrow-rs/Cargo.toml --bin c_abi_smoke
```

### 4) C++ smoke（`zarrow_c <-> Arrow C++`）

需要 Arrow C++ 开发环境，并且 `pkg-config` 可找到 `arrow`。

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

## CI 任务

见 `.github/workflows/ci.yml`：

- `interop-c-abi-cpp-smoke`
- `interop-c-abi-rs-smoke`

## 常见问题

`cargo run could not determine which binary to run`：

- 需要显式指定 `--bin c_abi_smoke` 或 `--bin zarrow-interop-arrow-rs`。

运行时找不到共享库：

- 确认 `LD_LIBRARY_PATH` 包含 `$PWD/zig-out/lib`。
