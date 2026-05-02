# Compute API (zarrow-core)

This document describes the downstream-facing compute framework API in `zarrow.compute`.

It is intended for external kernel libraries (for example `zarrow-compute`) that implement concrete compute functions on top of zarrow's core execution model.

Chinese version: [compute-api-zh.md](compute-api-zh.md)

---

## 1. Scope and Boundary

`zarrow` provides the **compute framework layer**:

- function registration and dispatch;
- signature matching (arity/type/options);
- result type inference hooks;
- execution context and aggregate lifecycle abstractions;
- common helper utilities for array/scalar/chunked execution;
- chunk-aware permutation helpers.

`zarrow` does **not** aim to own the full compute function catalog in this repository.
Concrete kernels (cast/arithmetic/comparison/filter/sort/aggregate families) should live in downstream libraries.

---

## 2. Core Types

Main public framework types:

- `FunctionRegistry`
- `KernelSignature`
- `Kernel`
- `ExecContext`
- `Datum`

`Datum` currently supports all three forms:

- `array: ArrayRef`
- `chunked: ChunkedArray`
- `scalar: Scalar`

Lifecycle helpers (`retain/release`) and type access (`dataType`) are implemented for all three variants.

---

## 3. Typed Options Model

Compute options are type-safe and tag-based.

### Options families

`OptionsTag`:

- `none`
- `cast`
- `arithmetic`
- `filter`
- `sort`
- `custom`

### Payload types

- `CastOptions`
- `ArithmeticOptions`
- `FilterOptions`
- `SortOptions`
- `CustomOptions`

Use `KernelSignature.options_check` to validate allowed option families for each kernel.

### SortOptions

`SortOptions` currently includes:

- `order`: `ascending | descending`
- `null_placement`: `at_start | at_end`
- `nan_placement`: `?at_start | ?at_end`
- `stable`: `bool`

---

## 4. Signature and Type Inference

`KernelSignature` supports:

- exact arity;
- variadic minimum arity (`at_least`);
- bounded arity range (`range`);
- optional `type_check` callback;
- optional `options_check` callback;
- optional `result_type_fn` callback.

Result type APIs:

- `KernelSignature.inferResultType(args, options)`
- `FunctionRegistry.resolveResultType(...)`

When signature or options checks fail, errors are reported through `KernelError`.

---

## 5. Error Model

`KernelError` includes framework and execution-level categories such as:

- `InvalidArity`
- `InvalidOptions`
- `InvalidInput`
- `UnsupportedType`
- `InvalidCast`
- `Overflow`
- `DivideByZero`
- `NoMatchingKernel`

Helpful utility helpers are provided for common arithmetic/cast behavior:

- `intCastOrInvalidCast(...)`
- `arithmeticDivI64(...)`

---

## 6. ExecContext and Config

`ExecConfig` currently includes:

- `safe_cast`
- `overflow_mode` (`checked | wrapping | saturating`)
- `threads`
- `arena_allocator`

Context construction:

- `ExecContext.init(allocator, &registry)`
- `ExecContext.initWithConfig(allocator, &registry, config)`

For borrowed scalar string/binary data, use:

- `ExecContext.dupScalarString(...)`
- `ExecContext.dupScalarBinary(...)`

---

## 7. Aggregate Lifecycle Interface

For stateful aggregate kernels:

- `AggregateLifecycle.init`
- `AggregateLifecycle.update`
- `AggregateLifecycle.merge`
- `AggregateLifecycle.finalize`
- `AggregateLifecycle.deinit`

Runtime flow:

1. register kernel with `aggregate_lifecycle`;
2. call `ctx.beginAggregate(...)` to create `AggregateSession`;
3. call `update/merge/finalize/deinit`.

If lifecycle is missing, `beginAggregate` returns `error.MissingLifecycle`.

---

## 8. Execution and Chunk-Aware Helpers

Framework helpers include:

- null propagation: `unaryNullPropagates`, `binaryNullPropagates`, `naryNullPropagates`;
- scalar broadcast length inference: `inferBinaryExecLen`, `inferNaryExecLen`;
- chunk-aligned iterators: `UnaryExecChunkIterator`, `BinaryExecChunkIterator`, `NaryExecChunkIterator`.

Permutation-family helpers include:

- `chunkedResolveLogicalIndices`
- `datumTake`, `datumTakeNullable`
- `datumFilterSelectionIndices`
- `datumFilterChunkAware`
- `datumSelect`, `datumSelectNullable`

These are intended to help downstream kernels preserve chunked execution/output where possible, instead of pre-concatenating chunked inputs.

---

## 9. Recommended Downstream Extension Path

For new sort/take/filter kernels in downstream repos:

1. keep one logical behavior for both array and chunked inputs;
2. drive behavior through typed options (especially `SortOptions`), not hardcoded defaults;
3. reuse core chunk-aware helpers for index resolution, null handling, and gather/filter execution;
4. test array/chunked logical equivalence, including misaligned chunk-boundary cases.

---

## 10. Diagnostics

Readable mismatch diagnostics are available:

- `KernelSignature.explainMismatch`
- `KernelSignature.explainInferResultTypeFailure`
- `FunctionRegistry.explainResolveKernelFailure`
- `FunctionRegistry.explainResolveResultTypeFailure`

---

## 11. References

- Example: [`examples/compute_lifecycle.zig`](../examples/compute_lifecycle.zig)
- Readiness note: [ecosystem-readiness.md](ecosystem-readiness.md)
