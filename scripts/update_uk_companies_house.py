"""Real ingestion of UK Companies House's Free Company Data Product (no auth required).

Source: https://download.companieshouse.gov.uk — a monthly bulk CSV snapshot of all
live/recently-active UK companies, free, no signup. This is separate from the live
Companies House API lookup already used in diliguard-trace's ubo_module.py; this
pipeline instead gives a queryable bulk snapshot of basic company data.

This snapshot does not include Persons with Significant Control (PSC/UBO) data —
that is a separate Companies House product/API — so `ubo_names` is left as None (a
real gap), not fabricated.

~5.7M rows / ~2GB decompressed CSV — this is done with Polars-native/lazy operations
throughout (never a Python per-row loop building 5.7M dicts, and never the full CSV
held in memory as a Python bytes object) after an earlier version OOM-killed the
GitHub Actions runner immediately after a successful download.

If the download or parse fails, this script exits non-zero and writes nothing.
"""
import os
import re
import sys
import zipfile
from datetime import datetime, timezone

import polars as pl

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common.http import download_file, get_with_retry
from common.schema import KNOWN_FAKE_SENTINELS, PIPELINE_VERSION

INDEX_URL = "https://download.companieshouse.gov.uk/en_output.html"
ZIP_LOCAL_PATH = "uk_ch_basic_company_data.zip"
CSV_LOCAL_PATH = "uk_ch_basic_company_data.csv"
OUTPUT_PATH = "uk_companies_house.parquet"

ADDRESS_FIELDS = [
    "RegAddress.AddressLine1",
    "RegAddress.AddressLine2",
    "RegAddress.PostTown",
    "RegAddress.County",
    "RegAddress.Country",
    "RegAddress.PostCode",
]


def _resolve_current_zip_url() -> str:
    """The filename is dated (e.g. BasicCompanyDataAsOneFile-2026-08-01.zip) and
    changes every month — resolve today's real link from the index page rather than
    hardcoding a date that will go stale."""
    response = get_with_retry(INDEX_URL, timeout=30)
    match = re.search(r'href="(BasicCompanyDataAsOneFile-[\d-]+\.zip)"', response.text)
    if not match:
        raise RuntimeError("Could not find BasicCompanyDataAsOneFile zip link on index page")
    return f"https://download.companieshouse.gov.uk/{match.group(1)}"


def main():
    print("Resolving current Companies House bulk snapshot URL...")
    try:
        zip_url = _resolve_current_zip_url()
    except Exception as e:
        print(f"FATAL: failed to resolve Companies House bulk snapshot URL: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"Downloading {zip_url} ...")
    try:
        download_file(zip_url, ZIP_LOCAL_PATH, timeout=300)
    except Exception as e:
        print(f"FATAL: failed to download Companies House bulk snapshot: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        print("Extracting CSV to disk (not into memory)...")
        with zipfile.ZipFile(ZIP_LOCAL_PATH) as z:
            member = z.namelist()[0]
            with z.open(member) as src, open(CSV_LOCAL_PATH, "wb") as dst:
                for chunk in iter(lambda: src.read(8 * 1024 * 1024), b""):
                    dst.write(chunk)
    except Exception as e:
        print(f"FATAL: failed to extract Companies House bulk snapshot: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if os.path.exists(ZIP_LOCAL_PATH):
            os.remove(ZIP_LOCAL_PATH)

    try:
        print("Parsing CSV with Polars (streaming, low-memory)...")
        lf = pl.scan_csv(CSV_LOCAL_PATH, infer_schema_length=0, low_memory=True)
        lf = lf.rename({c: c.strip() for c in lf.collect_schema().names()})

        def _blank_to_null(col: str) -> pl.Expr:
            stripped = pl.col(col).str.strip_chars()
            return pl.when(stripped == "").then(None).otherwise(stripped)

        address_expr = pl.concat_str(
            [_blank_to_null(f) for f in ADDRESS_FIELDS],
            separator=", ",
            ignore_nulls=True,
        )

        out = lf.select(
            pl.col("CompanyName").str.strip_chars().alias("company_name"),
            pl.col("CompanyNumber").str.strip_chars().alias("registration_number"),
            address_expr.alias("registered_address"),
            pl.col("CompanyStatus").str.strip_chars().alias("status"),
            pl.lit(None, dtype=pl.Utf8).alias("ubo_names"),
            pl.lit("GB").alias("jurisdiction"),
            pl.lit(zip_url).alias("source_url"),
            pl.lit(datetime.now(timezone.utc).isoformat()).alias("fetched_at"),
            pl.lit(PIPELINE_VERSION).alias("pipeline_version"),
        ).with_columns(
            pl.when(pl.col("registered_address") == "").then(None).otherwise(pl.col("registered_address")).alias("registered_address")
        )

        row_count = out.select(pl.len()).collect().item()
        if row_count == 0:
            print("FATAL: parsed zero rows from Companies House bulk snapshot — refusing to write an empty file", file=sys.stderr)
            sys.exit(1)

        sentinel_expr = pl.lit(False)
        for col in ("company_name", "registration_number", "status"):
            for sentinel in KNOWN_FAKE_SENTINELS:
                sentinel_expr = sentinel_expr | pl.col(col).str.contains(re.escape(sentinel), literal=False)
        fake_hits = out.filter(sentinel_expr).select(pl.len()).collect().item()
        if fake_hits > 0:
            print(f"FATAL: {fake_hits} row(s) matched a banned fake-data sentinel — refusing to ship", file=sys.stderr)
            sys.exit(1)

        out.sink_parquet(OUTPUT_PATH)
    except Exception as e:
        print(f"FATAL: failed to parse Companies House bulk snapshot: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if os.path.exists(CSV_LOCAL_PATH):
            os.remove(CSV_LOCAL_PATH)

    print(f"Wrote {row_count} real UK Companies House rows to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
