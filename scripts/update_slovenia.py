"""Real ingestion of Slovenia's Business Register (Poslovni register Slovenije) open
data — no auth required.

Source: https://podatki.gov.si/dataset/poslovni-register-slovenije (AJPES, published
via the podatki.gov.si CKAN open-data catalog), refreshed twice a month ("dvotedensko"),
CC BY 4.0 licensed.

The CSV resource is UTF-16 encoded with a BOM (unusual among this pipeline's sources —
verified by hand: the raw bytes start with \\xff\\xfe and every other byte is 0x00),
so it must be decoded before Polars can parse it as CSV.

The basic register file does not publish a company "status" (active/dissolved) field
or beneficial-owner (UBO) data, so both are left as None — real gaps in this specific
open file, not fabricated. Legal form ("Pravnoorganizacijska oblika") and registration
authority ("Registrski organ") are real fields in the source but have no dedicated
column in this pipeline's shared schema (scripts/common/schema.py only defines
company_name/registration_number/registered_address/status/ubo_names/jurisdiction);
rather than invent a new column unilaterally (a cross-cutting change affecting every
other country script), they are left uncaptured here. In practice the legal form is
usually already embedded as a suffix in the company's full name (e.g. "... S.P." or
"... D.O.O.") published by AJPES.

If the download or parse fails, or zero rows are parsed, this script exits non-zero
and writes nothing.
"""
import os
import sys
import time

import polars as pl

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common.http import download_file
from common.parquet_io import write_registry_parquet

REGISTER_CSV_URL = (
    "https://podatki.gov.si/dataset/9ee1a9aa-c224-4995-b2ad-3760d7af0748"
    "/resource/beb70929-3d0d-41c6-9af2-25d525d906d3/download/opsiprs.csv"
)
LOCAL_CSV_UTF16 = "si_opsiprs_utf16.csv"
LOCAL_CSV_UTF8 = "si_opsiprs_utf8.csv"


def main():
    print("Downloading Slovenia Business Register (AJPES / podatki.gov.si) open data...")
    # The source server has been observed to reset the connection mid-stream on this
    # ~120MB file (confirmed by hand: repeated full-file re-downloads eventually
    # succeed). download_file()'s own retry logic only covers the initial request
    # before streaming starts, so retry the whole download here too before failing.
    last_error = None
    for attempt in range(1, 6):
        try:
            download_file(REGISTER_CSV_URL, LOCAL_CSV_UTF16, timeout=300, max_attempts=2)
            last_error = None
            break
        except Exception as e:
            last_error = e
            print(f"...download attempt {attempt} failed ({e}), retrying", file=sys.stderr)
            time.sleep(3)
    if last_error is not None:
        print(f"FATAL: failed to download Slovenia open data: {last_error}", file=sys.stderr)
        sys.exit(1)

    try:
        # Real source quirk: the CSV is UTF-16 (BOM-prefixed), not UTF-8/Latin-1 like
        # the other country CSVs in this pipeline — confirmed against the raw bytes.
        with open(LOCAL_CSV_UTF16, encoding="utf-16") as f:
            text = f.read()
        with open(LOCAL_CSV_UTF8, "w", encoding="utf-8") as f:
            f.write(text)
        df = pl.read_csv(LOCAL_CSV_UTF8, infer_schema_length=0)
    except Exception as e:
        print(f"FATAL: failed to parse Slovenia open data: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        for f in (LOCAL_CSV_UTF16, LOCAL_CSV_UTF8):
            if os.path.exists(f):
                os.remove(f)

    rows = []
    for record in df.iter_rows(named=True):
        address_parts = [
            record.get("Ulica"),
            record.get("Hišna št "),
            record.get("Hišna št  dodatek"),
            record.get("Naselje"),
            record.get("Poštna št "),
            record.get("Pošta"),
            record.get("Država"),
        ]
        address = ", ".join(p for p in address_parts if p) or None
        rows.append({
            "company_name": record.get("Popolno ime"),
            "registration_number": record.get("Matična številka"),
            "registered_address": address,
            "status": None,  # not published in this AJPES basic-data file
            "ubo_names": None,  # not published in Slovenia's open business register
            "jurisdiction": "SI",
        })

    if not rows:
        print("FATAL: parsed zero rows from Slovenia open data — refusing to write an empty file", file=sys.stderr)
        sys.exit(1)

    count = write_registry_parquet(rows, "slovenia_reg.parquet", source_url=REGISTER_CSV_URL)
    print(f"Wrote {count} real Slovenia Business Register rows to slovenia_reg.parquet")


if __name__ == "__main__":
    main()
