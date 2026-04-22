# Arrow Go IPC 互操作指南

本文档说明如何在本地执行 zarrow <-> Arrow Go IPC 互操作验证，以及 CI 覆盖范围。

## 适用范围

互操作工具路径：

- `tools/interop/arrow-go`

支持的 fixture case：

- `canonical`
- `dict-delta`（仅 stream）
- `ree`
- `ree-int16`
- `ree-int64`
- `complex`
- `extension`
- `view`

支持的容器格式：

- `stream`
- `file`（除 `dict-delta` 外全部支持）

## 覆盖矩阵

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

`dict-delta` 仅支持 stream，原因是 IPC file 格式不支持跨 batch 的 dictionary replacement。

## 本地全量验证

在仓库根目录执行：

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

# 1) zarrow 生成，Arrow Go 校验。
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

# 2) Arrow Go 生成，zarrow 校验。
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

## CI 任务对应

见 `.github/workflows/ci.yml`：

- `interop-arrow-go`
  - 全量 case 验证。
  - Go 版本：`1.24.x`。
- `interop-arrow-go-version-smoke`
  - `canonical stream` 烟测。
  - Go 版本矩阵：`1.23.x`、`1.24.x`。

## 常见问题

`go: cannot find main module`：

- 需要在 `tools/interop/arrow-go` 目录下运行 `go run . ...`。

`go.mod requires go >= 1.24`：

- 先确认当前分支代码已更新，`tools/interop/arrow-go/go.mod` 是最新。
- 建议先使用 Go `1.24.x`（与主互操作 CI 一致）。
- 若环境设置了 `GOTOOLCHAIN=local`，本地 Go 版本必须满足模块要求。
