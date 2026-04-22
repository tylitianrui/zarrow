# Arrow Go IPC Interop Guide

This document explains how to run zarrow <-> Arrow Go IPC interop locally and what coverage is enforced in CI.

## Scope

Interop tool path:

- `tools/interop/arrow-go`

Supported fixtures:

- `canonical`
- `dict-delta` (stream only)
- `ree`
- `ree-int16`
- `ree-int64`
- `complex`
- `extension`
- `view`

Supported containers:

- `stream`
- `file` (all fixtures except `dict-delta`)

## Coverage Matrix

| Fixture | stream | file |
|---|---|---|
| canonical | yes | yes |
| dict-delta | yes | no |
| ree | yes | yes |
| ree-int16 | yes | yes |
| ree-int64 | yes | yes |
| complex | yes | yes |
| extension | yes | yes |
| view | yes | yes |

`dict-delta` is stream-only because IPC file format does not support dictionary replacement across record batches.

## Local Full Verification

Run from repository root:

```bash
set -euo pipefail

mkdir -p .interop-fixtures

stream_cases=(canonical dict-delta ree ree-int16 ree-int64 complex extension view)
file_cases=(canonical ree ree-int16 ree-int64 complex extension view)

name_suffix() {
  local case_name="$1"
  if [[ "$case_name" == "canonical" ]]; then
    printf '%s' ""
  else
    printf '%s' "_${case_name//-/_}"
  fi
}

# 1) zarrow generates fixtures, Arrow Go validates.
for case_name in "${stream_cases[@]}"; do
  suffix="$(name_suffix "$case_name")"
  zig build interop-fixture-writer -- ".interop-fixtures/zarrow${suffix}.arrow" "$case_name" stream
  (cd tools/interop/arrow-go && GOCACHE=/tmp/go-build GOMODCACHE=/tmp/go-mod go run . validate "../../../.interop-fixtures/zarrow${suffix}.arrow" "$case_name" stream)
done
for case_name in "${file_cases[@]}"; do
  suffix="$(name_suffix "$case_name")"
  zig build interop-fixture-writer -- ".interop-fixtures/zarrow${suffix}_file.arrow" "$case_name" file
  (cd tools/interop/arrow-go && GOCACHE=/tmp/go-build GOMODCACHE=/tmp/go-mod go run . validate "../../../.interop-fixtures/zarrow${suffix}_file.arrow" "$case_name" file)
done

# 2) Arrow Go generates fixtures, zarrow validates.
for case_name in "${stream_cases[@]}"; do
  suffix="$(name_suffix "$case_name")"
  (cd tools/interop/arrow-go && GOCACHE=/tmp/go-build GOMODCACHE=/tmp/go-mod go run . generate "../../../.interop-fixtures/arrow_go${suffix}.arrow" "$case_name" stream)
  zig build interop-fixture-check -- ".interop-fixtures/arrow_go${suffix}.arrow" "$case_name" stream
done
for case_name in "${file_cases[@]}"; do
  suffix="$(name_suffix "$case_name")"
  (cd tools/interop/arrow-go && GOCACHE=/tmp/go-build GOMODCACHE=/tmp/go-mod go run . generate "../../../.interop-fixtures/arrow_go${suffix}_file.arrow" "$case_name" file)
  zig build interop-fixture-check -- ".interop-fixtures/arrow_go${suffix}_file.arrow" "$case_name" file
done
```

## CI Lanes

In `.github/workflows/ci.yml`:

- `interop-arrow-go`
  - Full case matrix validation.
  - Go toolchain: `1.24.x`.
- `interop-arrow-go-version-smoke`
  - Smoke check on `canonical stream`.
  - Go toolchain matrix: `1.23.x`, `1.24.x`.

## Troubleshooting

`go: cannot find main module`:

- Run Go commands under `tools/interop/arrow-go`, not repository root.

`go.mod requires go >= 1.24`:

- Verify you are on current branch and `tools/interop/arrow-go/go.mod` is up to date.
- Use Go `1.24.x` (same as main CI lane), or run version smoke locally with `1.23.x` only when module/dependencies allow it.
- If your environment pins `GOTOOLCHAIN=local`, ensure local Go version satisfies the module requirement.
