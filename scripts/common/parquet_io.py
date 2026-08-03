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


def write_registry_parquet(rows: list[dict], output_path: str, source_url: str) -> int:
    """Validate and write rows to a Parquet file in the shared schema.

    Returns the number of rows written. Refuses to write (exits non-zero) if any
    row contains a known-fake sentinel value, as a last line of defense against
    ever re-introducing hardcoded placeholder data.
    """
    finalized = finalize_rows(rows, source_url=source_url)

    fake_hit = _contains_fake_sentinel(finalized)
    if fake_hit:
        print(f"FATAL: refusing to write {output_path} — {fake_hit}", file=sys.stderr)
        sys.exit(1)

    columns = REQUIRED_FIELDS + PROVENANCE_FIELDS
    df = pl.DataFrame(finalized, schema=columns) if finalized else pl.DataFrame({c: [] for c in columns})
    df.write_parquet(output_path)
    return len(finalized)
