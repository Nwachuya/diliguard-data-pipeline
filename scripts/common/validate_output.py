"""CLI safety net run by CI between "produce parquet" and "upload to R2".

Independent of whatever validation a script's own code already does — this exists
so that a future edit to a script that bypasses common/parquet_io.py still gets
caught before anything reaches production. Exits non-zero (blocking the upload
step) if the file is missing, empty, or contains a known-fake sentinel.

Usage: python -m scripts.common.validate_output <path-to-parquet>
"""
import os
import sys

import duckdb

from .schema import REQUIRED_FIELDS, KNOWN_FAKE_SENTINELS


def validate(path: str) -> None:
    if not os.path.exists(path):
        print(f"FATAL: {path} does not exist", file=sys.stderr)
        sys.exit(1)

    con = duckdb.connect()
    row_count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{path}')").fetchone()[0]
    if row_count == 0:
        print(f"FATAL: {path} has zero rows — refusing to upload", file=sys.stderr)
        sys.exit(1)

    string_columns = [
        row[0] for row in con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{path}')"
        ).fetchall()
        if row[0] in REQUIRED_FIELDS and row[1] in ("VARCHAR", "STRING")
    ]
    if string_columns:
        sentinel_clause = " OR ".join(
            f"{col} ILIKE '%{sentinel}%'"
            for col in string_columns
            for sentinel in KNOWN_FAKE_SENTINELS
        )
        fake_hits = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{path}') WHERE {sentinel_clause}"
        ).fetchone()[0]
        if fake_hits > 0:
            print(f"FATAL: {path} has {fake_hits} row(s) matching a banned fake-data sentinel", file=sys.stderr)
            sys.exit(1)

    print(f"OK: {path} — {row_count} rows, no fake-data sentinels found")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python -m scripts.common.validate_output <path-to-parquet>", file=sys.stderr)
        sys.exit(1)
    validate(sys.argv[1])
