#!/usr/bin/env python3
import subprocess
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).with_name("benchmark_ci_check.py")


def run_check(csv_text: str):
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "bench.csv"
        p.write_text(csv_text, encoding="utf-8")
        return subprocess.run(["python3", str(SCRIPT), str(p)], capture_output=True, text=True)


VALID_HEADER = "benchmark,rows,iterations,elapsed_ns,rows_per_sec,ns_per_row,checksum,git_sha,timestamp"
VALID_ROWS = [
    "primitive_builder,1000,10,1000000,1000.0,1000.0,42,abc123,1700000000",
    "record_batch_builder,1000,10,1000000,1000.0,1000.0,43,abc123,1700000000",
    "struct_builder,1000,10,1000000,1000.0,1000.0,44,abc123,1700000000",
]


def make_csv(*rows: str) -> str:
    return "\n".join(rows)


class BenchmarkCsvCheckTests(unittest.TestCase):
    def test_valid_csv_passes(self):
        r = run_check(make_csv(VALID_HEADER, *VALID_ROWS))
        self.assertEqual(0, r.returncode, r.stderr)

    # --- header validation ---

    def test_invalid_header_missing_git_sha_timestamp_fails(self):
        csv_text = make_csv(
            "benchmark,rows,iterations,elapsed_ns,rows_per_sec,ns_per_row,checksum",
            "primitive_builder,1000,10,1000000,1000.0,1000.0,42",
        )
        r = run_check(csv_text)
        self.assertNotEqual(0, r.returncode)

    def test_wrong_column_order_fails(self):
        csv_text = make_csv(
            "benchmark,rows,iterations,elapsed_ns,rows_per_sec,ns_per_row,timestamp,git_sha,checksum",
            "primitive_builder,1000,10,1000000,1000.0,1000.0,1700000000,abc123,42",
            "record_batch_builder,1000,10,1000000,1000.0,1000.0,1700000000,abc123,43",
            "struct_builder,1000,10,1000000,1000.0,1000.0,1700000000,abc123,44",
        )
        r = run_check(csv_text)
        self.assertNotEqual(0, r.returncode)

    # --- row count / completeness ---

    def test_empty_file_fails(self):
        r = run_check("")
        self.assertNotEqual(0, r.returncode)

    def test_header_only_no_data_rows_fails(self):
        r = run_check(VALID_HEADER)
        self.assertNotEqual(0, r.returncode)

    def test_missing_benchmark_fails(self):
        csv_text = make_csv(
            VALID_HEADER,
            "primitive_builder,1000,10,1000000,1000.0,1000.0,42,abc123,1700000000",
        )
        r = run_check(csv_text)
        self.assertNotEqual(0, r.returncode)

    # --- per-row column count ---

    def test_too_few_columns_fails(self):
        csv_text = make_csv(
            VALID_HEADER,
            "primitive_builder,1000,10,1000000,1000.0,1000.0,42,abc123",
            *VALID_ROWS[1:],
        )
        r = run_check(csv_text)
        self.assertNotEqual(0, r.returncode)

    def test_too_many_columns_fails(self):
        csv_text = make_csv(
            VALID_HEADER,
            "primitive_builder,1000,10,1000000,1000.0,1000.0,42,abc123,1700000000,extra",
            *VALID_ROWS[1:],
        )
        r = run_check(csv_text)
        self.assertNotEqual(0, r.returncode)

    # --- unknown / duplicate benchmark ---

    def test_unknown_benchmark_name_fails(self):
        csv_text = make_csv(
            VALID_HEADER,
            "unknown_bench,1000,10,1000000,1000.0,1000.0,42,abc123,1700000000",
            *VALID_ROWS[1:],
        )
        r = run_check(csv_text)
        self.assertNotEqual(0, r.returncode)

    # --- numeric field validation ---

    def test_zero_rows_fails(self):
        csv_text = make_csv(
            VALID_HEADER,
            "primitive_builder,0,10,1000000,1000.0,1000.0,42,abc123,1700000000",
            *VALID_ROWS[1:],
        )
        r = run_check(csv_text)
        self.assertNotEqual(0, r.returncode)

    def test_zero_iterations_fails(self):
        csv_text = make_csv(
            VALID_HEADER,
            "primitive_builder,1000,0,1000000,1000.0,1000.0,42,abc123,1700000000",
            *VALID_ROWS[1:],
        )
        r = run_check(csv_text)
        self.assertNotEqual(0, r.returncode)

    def test_zero_elapsed_ns_fails(self):
        csv_text = make_csv(
            VALID_HEADER,
            "primitive_builder,1000,10,0,1000.0,1000.0,42,abc123,1700000000",
            *VALID_ROWS[1:],
        )
        r = run_check(csv_text)
        self.assertNotEqual(0, r.returncode)

    def test_non_numeric_elapsed_fails(self):
        csv_text = make_csv(
            VALID_HEADER,
            "primitive_builder,1000,10,nan,1000.0,1000.0,42,abc123,1700000000",
            *VALID_ROWS[1:],
        )
        r = run_check(csv_text)
        self.assertNotEqual(0, r.returncode)

    def test_zero_timestamp_fails(self):
        csv_text = make_csv(
            VALID_HEADER,
            "primitive_builder,1000,10,1000000,1000.0,1000.0,42,abc123,0",
            *VALID_ROWS[1:],
        )
        r = run_check(csv_text)
        self.assertNotEqual(0, r.returncode)

    def test_negative_timestamp_fails(self):
        csv_text = make_csv(
            VALID_HEADER,
            "primitive_builder,1000,10,1000000,1000.0,1000.0,42,abc123,-1",
            *VALID_ROWS[1:],
        )
        r = run_check(csv_text)
        self.assertNotEqual(0, r.returncode)

    # --- git_sha validation ---

    def test_empty_git_sha_fails(self):
        csv_text = make_csv(
            VALID_HEADER,
            "primitive_builder,1000,10,1000000,1000.0,1000.0,42,,1700000000",
            *VALID_ROWS[1:],
        )
        r = run_check(csv_text)
        self.assertNotEqual(0, r.returncode)


if __name__ == "__main__":
    unittest.main()
