"""Real ingestion of Romania's ONRC (Trade Register) open data (no auth required).

Source: data.gov.ro CKAN portal, published by "Oficiul Național al Registrului
Comerțului" (ONRC). Two important findings from live research on this dataset:

1. There is a genuinely LIVE, recurring dataset — NOT the stale 09.05.2017 "onrc"
   snapshot that shows up first for a naive search. ONRC publishes a fresh dataset
   roughly monthly under a slug like "firme-DD-MM-YYYY" (e.g. "firme-08-07-2026",
   published 2026-07-08), each containing the same fixed set of resource files
   (OD_FIRME.CSV, OD_STARE_FIRMA.CSV, OD_CAEN_AUTORIZAT.CSV, etc.). Because the
   dataset *slug* changes every publish, this script discovers the current dataset
   dynamically via the CKAN package_search API rather than hardcoding a URL — the
   same way Estonia/Latvia hardcode a stable URL, Romania instead hardcodes a
   *query* and always resolves to whatever the newest real snapshot is.
2. The main companies file (OD_FIRME.CSV) does NOT contain a status column. Status
   lives in a separate OD_STARE_FIRMA.CSV (COD_INMATRICULARE -> numeric status
   code), which must be joined, and the numeric code is only meaningful once
   decoded via N_STARE_FIRMA.CSV in ONRC's separate "nomenclatoare-DD-MM-YYYY"
   dataset (also a recurring monthly slug, resolved the same way). Verified live:
   code 1048 = "funcțiune" (active/operating), 1084 = "radiată" (struck off),
   2069 = "sediu expirat" (registered office expired), etc. — confirmed by
   downloading and reading the real N_STARE_FIRMA.CSV content, not guessed.

No beneficial-ownership data is published in any of these open files, so
`ubo_names` is left as None (a real gap in the free source, like Latvia) rather
than fabricated from e.g. legal-representative data (a legal representative is
not a UBO and mapping one to the other would be dishonest).

If discovery, download, or parsing fails, or zero rows are produced, this script
exits non-zero and writes nothing. It never falls back to hardcoded/sample data.

Local verification note: OD_FIRME.CSV is ~650-700MB. In CI this script downloads
it in full via the normal `download_file` path (unbounded — no truncation). For
*local* verification only, set the env var ROMANIA_LOCAL_TEST_MAX_BYTES to cap
how much of OD_FIRME.CSV is downloaded (e.g. "5000000" for ~5MB), so the parsing
logic can be checked without waiting on the full download. This env var must
never be set in CI/production — its absence is what makes the full, real,
untruncated file get processed.
"""
import os
import sys

import polars as pl

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common.http import download_file, get_with_retry
from common.parquet_io import write_registry_parquet

CKAN_PACKAGE_SEARCH_URL = "https://data.gov.ro/api/3/action/package_search"
ONRC_ORG_TITLE = "Oficiul Național al Registrului Comerțului"

FIRME_LOCAL_CSV = "ro_od_firme.csv"
STARE_FIRMA_LOCAL_CSV = "ro_od_stare_firma.csv"
NOMENCLATOR_LOCAL_CSV = "ro_n_stare_firma.csv"


def _find_latest_package(query: str, title_prefix: str) -> dict:
    """Find the newest real ONRC dataset whose title starts with `title_prefix`.

    ONRC republishes these datasets under a new URL slug every ~month (e.g.
    "firme-08-07-2026"), so we cannot hardcode a dataset id/URL the way
    Estonia/Latvia do for their stable bulk-download URLs. Instead we query CKAN's
    package_search API and pick the most recently modified matching package.
    """
    response = get_with_retry(CKAN_PACKAGE_SEARCH_URL, params={"q": query, "rows": 100})
    data = response.json()
    if not data.get("success"):
        raise RuntimeError(f"CKAN package_search did not succeed for query {query!r}")

    candidates = [
        pkg for pkg in data["result"]["results"]
        if pkg.get("title", "").strip().lower().startswith(title_prefix.lower())
        and pkg.get("organization", {}).get("title") == ONRC_ORG_TITLE
    ]
    if not candidates:
        raise RuntimeError(f"No real ONRC dataset found matching title prefix {title_prefix!r}")

    candidates.sort(key=lambda pkg: pkg.get("metadata_modified", ""), reverse=True)
    return candidates[0]


def _resource_url(package: dict, resource_name: str) -> str:
    for resource in package.get("resources", []):
        if resource.get("name", "").strip().upper() == resource_name.upper():
            return resource["url"]
    raise RuntimeError(f"Resource {resource_name!r} not found in dataset {package.get('name')!r}")


def _download_firme_csv(url: str, local_path: str) -> None:
    """Download OD_FIRME.CSV — the full, real, current-snapshot company list.

    Unbounded (full file) unless ROMANIA_LOCAL_TEST_MAX_BYTES is set, which is a
    LOCAL-ONLY verification aid (see module docstring). CI never sets this env
    var, so CI always processes the complete real file via `download_file`.
    """
    max_bytes_raw = os.environ.get("ROMANIA_LOCAL_TEST_MAX_BYTES")
    if not max_bytes_raw:
        download_file(url, local_path, timeout=900)
        return

    max_bytes = int(max_bytes_raw)
    print(f"ROMANIA_LOCAL_TEST_MAX_BYTES set — truncating OD_FIRME.CSV download to {max_bytes} bytes "
          f"for local verification only (this must never happen in CI).", flush=True)
    response = get_with_retry(url, timeout=120, stream=True)
    downloaded = 0
    with open(local_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if not chunk:
                continue
            f.write(chunk)
            downloaded += len(chunk)
            if downloaded >= max_bytes:
                break
    print(f"Truncated download complete: {downloaded / 1024 / 1024:.1f}MB", flush=True)


def _build_address(record: dict) -> str | None:
    parts = [
        record.get("ADR_JUDET"),
        record.get("ADR_LOCALITATE"),
        record.get("ADR_DEN_STRADA"),
        record.get("ADR_NR_STRADA"),
        record.get("ADR_BLOC"),
        record.get("ADR_SCARA"),
        record.get("ADR_ETAJ"),
        record.get("ADR_APARTAMENT"),
        record.get("ADR_COD_POSTAL"),
        record.get("ADR_TARA"),
    ]
    cleaned = [p.strip() for p in parts if p and p.strip()]
    return ", ".join(cleaned) if cleaned else None


def main():
    print("Discovering current ONRC open-data datasets on data.gov.ro...")
    try:
        firme_pkg = _find_latest_package("firme registrul comertului", "Firme înregistrate la Registrul Comerțului")
        nomenclator_pkg = _find_latest_package("nomenclatoare stare firma caen", "Nomenclatoare utilizate în decodificarea")

        firme_url = _resource_url(firme_pkg, "OD_FIRME.CSV")
        stare_firma_url = _resource_url(firme_pkg, "OD_STARE_FIRMA.CSV")
        n_stare_firma_url = _resource_url(nomenclator_pkg, "N_STARE_FIRMA.CSV")

        print(f"Using firms dataset: {firme_pkg['name']} (modified {firme_pkg.get('metadata_modified')})")
        print(f"Using nomenclator dataset: {nomenclator_pkg['name']} (modified {nomenclator_pkg.get('metadata_modified')})")
    except Exception as e:
        print(f"FATAL: failed to discover current ONRC dataset resources: {e}", file=sys.stderr)
        sys.exit(1)

    print("Downloading ONRC open data (OD_FIRME, OD_STARE_FIRMA, N_STARE_FIRMA)...")
    try:
        _download_firme_csv(firme_url, FIRME_LOCAL_CSV)
        download_file(stare_firma_url, STARE_FIRMA_LOCAL_CSV, timeout=300)
        download_file(n_stare_firma_url, NOMENCLATOR_LOCAL_CSV, timeout=60)
    except Exception as e:
        print(f"FATAL: failed to download ONRC open data: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        firme_df = pl.read_csv(FIRME_LOCAL_CSV, separator="^", infer_schema_length=0, quote_char=None,
                                truncate_ragged_lines=True)
        stare_firma_df = pl.read_csv(STARE_FIRMA_LOCAL_CSV, separator="^", infer_schema_length=0, quote_char=None)
        nomenclator_df = pl.read_csv(NOMENCLATOR_LOCAL_CSV, separator="^", infer_schema_length=0, quote_char=None)

        code_to_label = {
            record["COD"]: record["DENUMIRE"]
            for record in nomenclator_df.iter_rows(named=True)
            if record.get("COD")
        }
        status_by_reg_number = {
            record["COD_INMATRICULARE"]: code_to_label.get(record.get("COD"))
            for record in stare_firma_df.iter_rows(named=True)
            if record.get("COD_INMATRICULARE")
        }
    except Exception as e:
        print(f"FATAL: failed to parse ONRC open data: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        for f in (FIRME_LOCAL_CSV, STARE_FIRMA_LOCAL_CSV, NOMENCLATOR_LOCAL_CSV):
            if os.path.exists(f):
                os.remove(f)

    rows = []
    for record in firme_df.iter_rows(named=True):
        reg_number = record.get("COD_INMATRICULARE")
        rows.append({
            "company_name": record.get("DENUMIRE"),
            "registration_number": reg_number,
            "registered_address": _build_address(record),
            "status": status_by_reg_number.get(reg_number) if reg_number else None,
            "ubo_names": None,  # not published in any real ONRC open-data file
            "jurisdiction": "RO",
        })

    if not rows:
        print("FATAL: parsed zero rows from ONRC open data — refusing to write an empty file", file=sys.stderr)
        sys.exit(1)

    count = write_registry_parquet(rows, "romania_reg.parquet", source_url=firme_url)
    print(f"Wrote {count} real ONRC Trade Register rows to romania_reg.parquet")


if __name__ == "__main__":
    main()
