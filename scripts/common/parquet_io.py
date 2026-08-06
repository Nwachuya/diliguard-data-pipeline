"""One place that knows how to turn rows into a valid registry Parquet file."""
import sys
import polars as pl

from .schema import REQUIRED_FIELDS, PROVENANCE_FIELDS, KNOWN_FAKE_SENTINELS, finalize_rows


def _contains_fake_sentinel(rows: list[dict]) -> str | None:
    for row in rows:
        for field in REQUIRED_FIELDS:
            value = row.get(field)
            if isinstance(value, str):
                upper = value.upper()
                for sentinel in KNOWN_FAKE_SENTINELS:
                    if sentinel in upper:
                        return f"field '{field}' contains banned sentinel '{sentinel}': {value!r}"
    return None


def write_finalized_rows_parquet(rows: list[dict], output_path: str) -> int:
    """Write already-finalized rows (i.e. already passed through `finalize_rows`,
    so every row already carries REQUIRED_FIELDS + PROVENANCE_FIELDS) straight to
    Parquet, after the same fake-sentinel guard `write_registry_parquet` uses.

    Split out from `write_registry_parquet` so a script that needs to merge fresh
    rows with previously-written (already-provenance-stamped) rows — e.g. a
    checkpoint/resume crawl accumulating across runs — can do so without
    `finalize_rows` stamping a fresh `fetched_at` over historical rows that were
    genuinely fetched in an earlier run. Most scripts should keep using
    `write_registry_parquet`, which is unchanged.
    """
    fake_hit = _contains_fake_sentinel(rows)
    if fake_hit:
        print(f"FATAL: refusing to write {output_path} — {fake_hit}", file=sys.stderr)
        sys.exit(1)

    columns = REQUIRED_FIELDS + PROVENANCE_FIELDS
    df = pl.DataFrame(rows, schema=columns) if rows else pl.DataFrame({c: [] for c in columns})
    df.write_parquet(output_path)
    return len(rows)


def write_registry_parquet(rows: list[dict], output_path: str, source_url: str) -> int:
    """Validate and write rows to a Parquet file in the shared schema.

    Returns the number of rows written. Refuses to write (exits non-zero) if any
    row contains a known-fake sentinel value, as a last line of defense against
    ever re-introducing hardcoded placeholder data.
    """
    finalized = finalize_rows(rows, source_url=source_url)
    return write_finalized_rows_parquet(finalized, output_path)
