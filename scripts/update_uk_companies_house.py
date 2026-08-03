"""Real ingestion of UK Companies House's Free Company Data Product (no auth required).

Source: https://download.companieshouse.gov.uk — a monthly bulk CSV snapshot of all
live/recently-active UK companies, free, no signup. This is separate from the live
Companies House API lookup already used in diliguard-trace's ubo_module.py; this
pipeline instead gives a queryable bulk snapshot of basic company data.

This snapshot does not include Persons with Significant Control (PSC/UBO) data —
that is a separate Companies House product/API — so `ubo_names` is left as None (a
real gap), not fabricated.

If the download or parse fails, this script exits non-zero and writes nothing.
"""
import io
import os
import re
import sys
import zipfile

import polars as pl

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common.http import download_file, get_with_retry
from common.parquet_io import write_registry_parquet

INDEX_URL = "https://download.companieshouse.gov.uk/en_output.html"
ZIP_LOCAL_PATH = "uk_ch_basic_company_data.zip"

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


def _build_address(record: dict) -> str | None:
    parts = [record.get(field, "") for field in ADDRESS_FIELDS]
    parts = [p.strip() for p in parts if p and p.strip()]
    return ", ".join(parts) if parts else None


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
        with zipfile.ZipFile(ZIP_LOCAL_PATH) as z:
            csv_bytes = z.read(z.namelist()[0])
        df = pl.read_csv(io.BytesIO(csv_bytes), infer_schema_length=0)
        df.columns = [c.strip() for c in df.columns]
    except Exception as e:
        print(f"FATAL: failed to parse Companies House bulk snapshot: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if os.path.exists(ZIP_LOCAL_PATH):
            os.remove(ZIP_LOCAL_PATH)

    rows = []
    for record in df.iter_rows(named=True):
        rows.append({
            "company_name": (record.get("CompanyName") or "").strip() or None,
            "registration_number": (record.get("CompanyNumber") or "").strip() or None,
            "registered_address": _build_address(record),
            "status": (record.get("CompanyStatus") or "").strip() or None,
            "ubo_names": None,  # PSC/UBO data is a separate Companies House product, not in this snapshot
            "jurisdiction": "GB",
        })

    if not rows:
        print("FATAL: parsed zero rows from Companies House bulk snapshot — refusing to write an empty file", file=sys.stderr)
        sys.exit(1)

    count = write_registry_parquet(rows, "uk_companies_house.parquet", source_url=zip_url)
    print(f"Wrote {count} real UK Companies House rows to uk_companies_house.parquet")


if __name__ == "__main__":
    main()
