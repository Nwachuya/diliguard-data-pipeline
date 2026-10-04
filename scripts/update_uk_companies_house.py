"""Real ingestion of UK Companies House's Free Company Data Product (no auth required).

Source: https://download.companieshouse.gov.uk — a monthly bulk CSV snapshot of all
live/recently-active UK companies, free, no signup. This is separate from the live
Companies House API lookup already used in diliguard-trace's ubo_module.py; this
pipeline instead gives a queryable bulk snapshot of basic company data.

This snapshot does not include Persons with Significant Control (PSC/UBO) data —
that is a separate Companies House product/API — so `ubo_names` is left as None (a
real gap), not fabricated.

~5.7M rows / ~2GB decompressed CSV. The transform runs entirely inside DuckDB
(CSV -> COPY TO PARQUET), the same approach already proven in CI by
update_france_rne.py, because:
  * an earlier version built a Python dict per row for all 5.7M rows and
    OOM-killed the runner right after a successful download, and
  * a Polars rewrite of it used APIs that only exist in Polars 1.x while CI pins
    0.20.7 (the version the other, working country scripts depend on), so it
    could not be verified locally without changing a pin that four live scripts rely on.

If the download or parse fails, this script exits non-zero and writes nothing.
"""
import os
import re
import sys
import zipfile
from datetime import datetime, timezone

import duckdb

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


def _resolve_columns(con, csv_path: str) -> dict:
    """Map stripped column name -> the column name DuckDB actually exposes.

    The Companies House header has inconsistent leading spaces (e.g. ' CompanyNumber',
    ' RegAddress.AddressLine2'). Rather than assume whether the reader strips them,
    ask DuckDB for the names it produced and match on the stripped form.
    """
    described = con.execute(
        f"DESCRIBE SELECT * FROM read_csv_auto({_sql_str(csv_path)}, all_varchar=true, header=true, ignore_errors=true, null_padding=true)"
    ).fetchall()
    return {row[0].strip(): row[0] for row in described}


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _sql_str(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def main():
    print("Resolving current Companies House bulk snapshot URL...", flush=True)
    try:
        zip_url = _resolve_current_zip_url()
    except Exception as e:
        print(f"FATAL: failed to resolve Companies House bulk snapshot URL: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"Downloading {zip_url} ...", flush=True)
    try:
        download_file(zip_url, ZIP_LOCAL_PATH, timeout=300)
    except Exception as e:
        print(f"FATAL: failed to download Companies House bulk snapshot: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        print("Extracting CSV to disk (not into memory)...", flush=True)
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

    temp_spill = "tmp_duckdb_uk"
    os.makedirs(temp_spill, exist_ok=True)
    try:
        con = duckdb.connect()
        con.execute("PRAGMA threads=2;")
        con.execute("PRAGMA memory_limit='3.5GB';")
        con.execute(f"PRAGMA temp_directory='{temp_spill}';")

        columns = _resolve_columns(con, CSV_LOCAL_PATH)
        required = ["CompanyName", "CompanyNumber", "CompanyStatus", *ADDRESS_FIELDS]
        missing = [c for c in required if c not in columns]
        if missing:
            raise RuntimeError(f"Companies House CSV is missing expected columns: {missing}")

        def col(stripped_name: str) -> str:
            return _quote_ident(columns[stripped_name])

        # nullif(trim(...), '') so blank fields become real NULLs — concat_ws then
        # skips them instead of emitting empty ", ," gaps in the address.
        address_parts = ", ".join(f"nullif(trim({col(f)}), '')" for f in ADDRESS_FIELDS)
        fetched_at = datetime.now(timezone.utc).isoformat()

        print("Converting CSV to Parquet with DuckDB (streaming)...", flush=True)
        con.execute(f"""
            COPY (
                SELECT
                    nullif(trim({col('CompanyName')}), '') AS company_name,
                    nullif(trim({col('CompanyNumber')}), '') AS registration_number,
                    nullif(concat_ws(', ', {address_parts}), '') AS registered_address,
                    nullif(trim({col('CompanyStatus')}), '') AS status,
                    CAST(NULL AS VARCHAR) AS ubo_names,
                    'GB' AS jurisdiction,
                    {_sql_str(zip_url)} AS source_url,
                    {_sql_str(fetched_at)} AS fetched_at,
                    {_sql_str(PIPELINE_VERSION)} AS pipeline_version
                FROM read_csv_auto({_sql_str(CSV_LOCAL_PATH)}, all_varchar=true, header=true, ignore_errors=true, null_padding=true)
            ) TO {_sql_str(OUTPUT_PATH)} (FORMAT PARQUET);
        """)
        con.close()

        con_verify = duckdb.connect()
        row_count = con_verify.execute(
            f"SELECT COUNT(*) FROM read_parquet({_sql_str(OUTPUT_PATH)})"
        ).fetchone()[0]
        if row_count == 0:
            raise RuntimeError("parsed zero rows — refusing to keep an empty file")

        sentinel_clause = " OR ".join(
            f"{c} ILIKE {_sql_str('%' + s + '%')}"
            for c in ("company_name", "registration_number", "status")
            for s in KNOWN_FAKE_SENTINELS
        )
        fake_hits = con_verify.execute(
            f"SELECT COUNT(*) FROM read_parquet({_sql_str(OUTPUT_PATH)}) WHERE {sentinel_clause}"
        ).fetchone()[0]
        con_verify.close()
        if fake_hits > 0:
            raise RuntimeError(f"{fake_hits} row(s) matched a banned fake-data sentinel")
    except Exception as e:
        print(f"FATAL: failed to parse Companies House bulk snapshot: {e}", file=sys.stderr)
        if os.path.exists(OUTPUT_PATH):
            os.remove(OUTPUT_PATH)
        sys.exit(1)
    finally:
        if os.path.exists(CSV_LOCAL_PATH):
            os.remove(CSV_LOCAL_PATH)
        import shutil
        shutil.rmtree(temp_spill, ignore_errors=True)

    print(f"Wrote {row_count} real UK Companies House rows to {OUTPUT_PATH}", flush=True)


if __name__ == "__main__":
    main()
