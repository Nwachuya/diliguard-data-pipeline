"""Real ingestion of Estonia's e-Business Register open data (no auth required).

Source: https://avaandmed.ariregister.rik.ee/en/downloading-open-data (RIK, updated daily).
Uses the "lihtandmed" (basic company data) CSV/ZIP for company records and the
"kasusaajad" (beneficial owners) JSON/ZIP for real UBO data — both genuine open-data
files, not a metadata-only endpoint.

If either download or parse fails, this script exits non-zero and writes nothing.
It never falls back to hardcoded/sample company records.
"""
import io
import json
import os
import sys
import zipfile

import polars as pl

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common.http import download_file
from common.parquet_io import write_registry_parquet

LIHTANDMED_URL = "https://avaandmed.ariregister.rik.ee/sites/default/files/avaandmed/ettevotja_rekvisiidid__lihtandmed.csv.zip"
KASUSAAJAD_URL = "https://avaandmed.ariregister.rik.ee/sites/default/files/avaandmed/ettevotja_rekvisiidid__kasusaajad.json.zip"

LIHTANDMED_ZIP = "ee_lihtandmed.csv.zip"
KASUSAAJAD_ZIP = "ee_kasusaajad.json.zip"


def _extract_single_member(zip_path: str) -> bytes:
    with zipfile.ZipFile(zip_path) as z:
        return z.read(z.namelist()[0])


def _load_beneficiaries(zip_path: str) -> dict[int, str]:
    """Parse the real kasusaajad (beneficial owners) file into {reg_code: 'name, name, ...'}."""
    raw = _extract_single_member(zip_path)
    companies = json.loads(raw)
    ubo_by_code: dict[int, str] = {}
    for company in companies:
        code = company.get("ariregistri_kood")
        names = []
        for beneficiary in company.get("kasusaajad") or []:
            if beneficiary.get("lopp_kpv"):  # relationship has ended
                continue
            full_name = " ".join(p for p in [beneficiary.get("eesnimi"), beneficiary.get("nimi")] if p)
            if full_name:
                names.append(full_name)
        if code and names:
            ubo_by_code[code] = ", ".join(sorted(set(names)))
    return ubo_by_code


def main():
    print("Downloading Estonia e-Business Register open data (lihtandmed + kasusaajad)...")
    try:
        download_file(LIHTANDMED_URL, LIHTANDMED_ZIP, timeout=120)
        download_file(KASUSAAJAD_URL, KASUSAAJAD_ZIP, timeout=120)
    except Exception as e:
        print(f"FATAL: failed to download Estonia open data: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        csv_bytes = _extract_single_member(LIHTANDMED_ZIP)
        df = pl.read_csv(io.BytesIO(csv_bytes), separator=";", infer_schema_length=0)
        ubo_by_code = _load_beneficiaries(KASUSAAJAD_ZIP)
    except Exception as e:
        print(f"FATAL: failed to parse Estonia open data: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        for f in (LIHTANDMED_ZIP, KASUSAAJAD_ZIP):
            if os.path.exists(f):
                os.remove(f)

    rows = []
    for record in df.iter_rows(named=True):
        reg_code = record.get("ariregistri_kood")
        try:
            reg_code_int = int(reg_code) if reg_code else None
        except ValueError:
            reg_code_int = None
        address = record.get("ads_normaliseeritud_taisaadress") or record.get("ettevotja_aadress")
        rows.append({
            "company_name": record.get("nimi"),
            "registration_number": reg_code,
            "registered_address": address,
            "status": record.get("ettevotja_staatus_tekstina"),
            "ubo_names": ubo_by_code.get(reg_code_int) if reg_code_int else None,
            "jurisdiction": "EE",
        })

    if not rows:
        print("FATAL: parsed zero rows from Estonia open data — refusing to write an empty file", file=sys.stderr)
        sys.exit(1)

    count = write_registry_parquet(rows, "estonia_reg.parquet", source_url=LIHTANDMED_URL)
    print(f"Wrote {count} real Estonia e-Business Register rows to estonia_reg.parquet")


if __name__ == "__main__":
    main()
