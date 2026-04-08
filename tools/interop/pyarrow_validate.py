#!/usr/bin/env python3
"""Validate canonical IPC stream fixture semantics via PyArrow."""

from __future__ import annotations

import pathlib
import sys

import pyarrow as pa
import pyarrow.ipc as ipc


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: pyarrow_validate.py <in.arrow>", file=sys.stderr)
        return 2

    in_path = pathlib.Path(sys.argv[1])
    with ipc.open_stream(in_path) as reader:
        schema = reader.schema
        if len(schema) != 2:
            raise RuntimeError("invalid schema field count")
        if schema[0].name != "id" or schema[0].type != pa.int32():
            raise RuntimeError("invalid id field")
        if schema[1].name != "name" or schema[1].type != pa.string():
            raise RuntimeError("invalid name field")

        batches = list(reader)
    if len(batches) != 1:
        raise RuntimeError("expected exactly one batch")
    batch = batches[0]
    if batch.num_rows != 3:
        raise RuntimeError("invalid row count")

    ids = batch.column(0).to_pylist()
    names = batch.column(1).to_pylist()
    if ids != [1, 2, 3]:
        raise RuntimeError(f"invalid ids: {ids!r}")
    if names != ["alice", None, "bob"]:
        raise RuntimeError(f"invalid names: {names!r}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

