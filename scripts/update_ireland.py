"""Real ingestion of Ireland's CRO (Companies Registration Office) daily company
snapshot — no auth required.

Source: https://opendata.cro.ie/dataset/companies (CRO's open data portal, launched
2024, replacing what used to require a paid CORE licence), refreshed daily, CC BY 4.0.
Blog announcement: https://data.gov.ie/blog/cro-open-data-portal

The "Company Records" resource is a zipped CSV (companies.csv.zip) containing one row
per registered company. Verified real column names by downloading and inspecting the
file directly: company_num, company_name, company_status_code, company_status,
company_type_code, company_type, company_reg_date, last_ar_date, company_address_1..4,
comp_dissolved_date, nard, last_accounts_date, company_status_date, nace_v2_code,
eircode, company_name_eff_date, company_type_eff_date, princ_object_code.

This dataset does not include beneficial-ownership data — that's the separately-gated
RBO (Register of Beneficial Ownership), out of scope here — so `ubo_names` is left as
None, a real gap, not fabricated.

`company_type` (e.g. "LTD - Private Company Limited by Shares") is a real field in the
source but has no dedicated column in this pipeline's shared schema
(scripts/common/schema.py only defines company_name/registration_number/
registered_address/status/ubo_names/jurisdiction). Rather than invent a new shared
column unilaterally, it is left uncaptured here.

If the download or parse fails, or zero rows are parsed, this script exits non-zero
and writes nothing.
"""
import io
import os
import sys
import zipfile

import polars as pl

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common.http import download_file
from common.parquet_io import write_registry_parquet

COMPANIES_ZIP_URL = (
    "https://opendata.cro.ie/dataset/bf6f837d-0946-4c14-9a99-82cd6980c121"
    "/resource/3fef41bc-b8f4-4b10-8434-ce51c29b1bba/download/companies.csv.zip"
)
LOCAL_ZIP = "ie_companies.csv.zip"

ADDRESS_FIELDS = ["company_address_1", "company_address_2", "company_address_3", "company_address_4"]


def main():
    print("Downloading Ireland CRO company register open data...")
    try:
        download_file(COMPANIES_ZIP_URL, LOCAL_ZIP, timeout=180)
    except Exception as e:
        print(f"FATAL: failed to download Ireland CRO open data: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        with zipfile.ZipFile(LOCAL_ZIP) as z:
            csv_bytes = z.read(z.namelist()[0])
        df = pl.read_csv(io.BytesIO(csv_bytes), infer_schema_length=0)
    except Exception as e:
        print(f"FATAL: failed to parse Ireland CRO open data: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if os.path.exists(LOCAL_ZIP):
            os.remove(LOCAL_ZIP)

    rows = []
    for record in df.iter_rows(named=True):
        address_parts = [record.get(f) for f in ADDRESS_FIELDS] + [record.get("eircode")]
        address = ", ".join(p for p in address_parts if p) or None
        status = record.get("company_status")
        rows.append({
            "company_name": record.get("company_name"),
            "registration_number": record.get("company_num"),
            "registered_address": address,
            "status": status.strip() if status else None,
            "ubo_names": None,  # not published here — RBO is a separately-gated register
            "jurisdiction": "IE",
        })

    if not rows:
        print("FATAL: parsed zero rows from Ireland CRO open data — refusing to write an empty file", file=sys.stderr)
        sys.exit(1)

    count = write_registry_parquet(rows, "ireland_reg.parquet", source_url=COMPANIES_ZIP_URL)
    print(f"Wrote {count} real Ireland CRO company rows to ireland_reg.parquet")


if __name__ == "__main__":
    main()
