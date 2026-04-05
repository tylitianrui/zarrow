# zarrow
Zig implementation of Apache Arrow.

## Goals
Build a Zig ecosystem for Apache Arrow with core data structures, IPC support, compute kernels, and integrations with other formats. Focus on correctness, performance, and usability for Zig developers working with columnar data.

## Ownership and ArrayRef
- `ArrayData` is a read-only layout description.
- `ArrayRef` owns the layout and releases shared buffers on `release()`.
- Builders return `ArrayRef` and transfer ownership by default.
- Use `ArrayRef.fromBorrowed()` for borrowed layouts.
- Use `ArrayRef.fromOwnedUnsafe()` only when the layout is allocator-owned.

