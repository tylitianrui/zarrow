# Changelog

## v0.0.2 - 2026-04-23

### Added

- New C ABI library surface and installable header
- New C ABI smoke tools and CI jobs
- New Arrow Go IPC interop toolchain
- New C ABI roundtrip example:
  - `examples/c_abi_roundtrip.zig`
- New interop docs:
  - `docs/interop-arrow-go.md`
  - `docs/interop-arrow-go-zh.md`
  - `docs/interop-c-abi.md`
  - `docs/interop-c-abi-zh.md`

### Changed

- C Stream path in C ABI now preserves true streaming semantics (lazy callback forwarding) instead of materializing the full stream in memory.
- Interop CI matrix expanded to full fixture coverage (`canonical`, `dict-delta`, `ree`, `ree-int16`, `ree-int64`, `complex`, `extension`, `view`; with `dict-delta` stream-only).
- Arrow Go version-smoke matrix now explicitly covers Go `1.23.x` and `1.24.x`.

### Fixed

- Fixed `cargo run` binary ambiguity in `tools/interop/arrow-rs` by adding `default-run` and explicit `--bin` usage in CI/scripts.
- Fixed C ABI smoke build/runtime integration with Arrow C++.
- Improved IPC/array validity error-path handling to avoid panic-style behavior in validation and tooling paths.
- Updated compute/interop utilities to correctly propagate primitive value access errors.

