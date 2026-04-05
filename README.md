# zarrow
Zig implementation of Apache Arrow.

## Goals

zarrow aims to provide a Zig implementation of the Apache Arrow in-memory model, with a strong focus on:

- clear ownership and lifetime semantics
- correct Arrow physical layout representation
- explicit validation of array structure and buffer layout
- zero-copy slicing and shared read-only buffers
- ergonomic builders for constructing Arrow arrays in Zig

## Project Status

zarrow is currently in active development.

The ownership model, public APIs, and supported Arrow types are still evolving.  
At this stage, the goal is to make the core memory model, validation rules, and array construction semantics explicit and correct before expanding broader format and ecosystem support.
