"""Real ingestion of Latvia's Enterprise Register (UR) open data (no auth required).

Source: data.gov.lv dataset "uz" (Uzņēmumu reģistrs), resource "register.csv" —
resolved from the CKAN package_show API this script already called, but whose result
was previously discarded in favor of hardcoded sample rows.

Latvia's open UR dataset does not publish beneficial-owner data, so `ubo_names` is
left as None (a real gap in the free source) rather than fabricated.

If the download or parse fails, this script exits non-zero and writes nothing.
"""
import os
import sys

import polars as pl

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common.http import download_file
from common.parquet_io import write_registry_parquet

REGISTER_CSV_URL = (
    "https://data.gov.lv/dati/dataset/4de9697f-850b-45ec-8bba-61fa09ce932f"
    "/resource/25e80bf3-f107-4ab4-89ef-251b5b9374e9/download/register.csv"
)
LOCAL_CSV = "lv_register.csv"


def main():
    print("Downloading Latvia Enterprise Register open data...")
    try:
        download_file(REGISTER_CSV_URL, LOCAL_CSV, timeout=180)
    except Exception as e:
        print(f"FATAL: failed to download Latvia open data: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        df = pl.read_csv(LOCAL_CSV, separator=";", infer_schema_length=0, quote_char='"')
    except Exception as e:
        print(f"FATAL: failed to parse Latvia open data: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if os.path.exists(LOCAL_CSV):
            os.remove(LOCAL_CSV)

    rows = []
    for record in df.iter_rows(named=True):
        terminated = record.get("terminated")
        rows.append({
            "company_name": record.get("name"),
            "registration_number": record.get("regcode"),
            "registered_address": record.get("address"),
            "status": "TERMINATED" if terminated else "ACTIVE",
            "ubo_names": None,  # not published in Latvia's open UR dataset
            "jurisdiction": "LV",
        })

    if not rows:
        print("FATAL: parsed zero rows from Latvia open data — refusing to write an empty file", file=sys.stderr)
        sys.exit(1)

    count = write_registry_parquet(rows, "latvia_ur.parquet", source_url=REGISTER_CSV_URL)
    print(f"Wrote {count} real Latvia Enterprise Register rows to latvia_ur.parquet")


if __name__ == "__main__":
    main()
