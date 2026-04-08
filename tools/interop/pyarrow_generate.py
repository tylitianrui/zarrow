#!/usr/bin/env python3
"""Generate canonical IPC stream fixture via PyArrow."""

from __future__ import annotations

import pathlib
import sys

import pyarrow as pa
import pyarrow.ipc as ipc


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: pyarrow_generate.py <out.arrow>", file=sys.stderr)
        return 2

    out_path = pathlib.Path(sys.argv[1])
    out_path.parent.mkdir(parents=True, exist_ok=True)

    schema = pa.schema(
        [
            pa.field("id", pa.int32(), nullable=False),
            pa.field("name", pa.string(), nullable=True),
        ]
    )
    batch = pa.record_batch(
        [
            pa.array([1, 2, 3], type=pa.int32()),
            pa.array(["alice", None, "bob"], type=pa.string()),
        ],
        schema=schema,
    )

    with ipc.new_stream(out_path, schema) as writer:
        writer.write_batch(batch)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

