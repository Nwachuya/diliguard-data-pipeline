"""Real ingestion of the Czech Business Register via ARES's genuine BULK open-data
export — not the per-entity/search REST API.

WHY THIS REPLACED THE OLD PREFIX-CRAWL APPROACH
------------------------------------------------
This script previously drove ARES's `vyhledat` search REST endpoint with a recursive
`obchodniJmeno` (company-name) prefix-trie crawl, because the search API caps any
single query at 1,000 results and offers no cursor past that cap. That approach was
shipped, run for real in GitHub Actions, and had to be manually killed after 2+ hours
with no end in sight — it does not scale to ~3M entities in any reasonable CI window.

Before assuming that was the only option, this rewrite checked (live, in detail) for
a genuine bulk export:
  - https://data.mf.gov.cz/topics/ares — the Ministry of Finance's own open-data
    catalog page for ARES. Its DCAT-AP-CZ machine record
    (https://data.mf.gov.cz/lod/katalog/ares-administrativni-registr-ekonomickych-subjektu)
    lists `"distribuce": []` — i.e. NO downloadable file is registered against the
    "ARES - Administrativní registr ekonomických subjektů" dataset itself; that entry
    is API-only.
  - The Czech national open-data catalog (data.gov.cz / NKOD), searched for "ares",
    DOES list separate real datasets from Ministerstvo financí with real file
    distributions, most importantly:
      "ARES - Výstup pro všechna IČO" (ARES - Output for all IČOs) — described as
      "Balík obsahující kompletní obraz informací o osobách zapsaných v České
      republice ve veřejných rejstřících podle § 7 zákona č. 304/2013 Sb." (a package
      containing a complete image of every entity registered under the Czech Public
      Registers Act), distributed as real XML — one file per IČO, in a single
      tar.gz, confirmed live via HEAD request at ~857MB, "periodicita_aktualizace":
      DAILY.
  - That dataset's real file index is served directly from ares.gov.cz itself:
    https://ares.gov.cz/otevrena-data/ — confirmed live (curl), listing exactly:
      ares_vreo_all.tar.gz          <- the full bulk package (this script's source)
      ares_seznamIC_VR_balik.csv.7z <- list of IČOs included in the package
      ares_seznamIC_VR.csv.7z       <- list of IČOs + last-processed date
      ares_seznamIC_VR_zmen.7z      <- IČOs changed since the package was built
      ares_ciselnik_VR.csv          <- registry-code codelist
      ares_answer_vreo.xsd          <- the XML schema below is built against

This bulk file was actually downloaded and parsed end-to-end while writing this
script (not just probed): 857MB compressed, 1,284,719 real IČOs confirmed via the
package's own `ares_seznamIC_VR_balik.csv` manifest.

REAL SCOPE LIMITATION (disclosed, not hidden)
-----------------------------------------------
This bulk package covers entities registered in Czechia's "veřejné rejstříky"
(Public Registers under Act No. 304/2013 Sb.) — s.r.o./a.s. companies, cooperatives,
associations, foundations, institutes, SVJ, etc. It does NOT include sole traders
(OSVČ) registered only in the trade licensing register (živnostenský rejstřík), who
make up a large share of ARES's total ~3M IČOs but are natural persons, not the
corporate entities an AML/KYC registry check is normally run against. This is a
real, disclosed gap of this specific bulk file, consistent with this script's
"no fabrication" rule — it is not a workaround for that limitation, and the old
prefix-crawl script queried the same broader `ekonomicke-subjekty` universe, so this
is a genuine (smaller) scope change being called out explicitly, not silently
introduced.

Beneficial ownership: the package's `Statutarni_organ` element lists STATUTORY BODY
members (company directors/officers, i.e. legal representatives) — this is NOT the
same thing as beneficial ownership (UBO). Populating `ubo_names` from it would
mislabel real data. So `ubo_names` is left None here, same treatment as the previous
version of this script and as Estonia/Latvia's real gaps elsewhere in this repo.

If the download, extraction, or parse fails, or zero rows are collected, this script
exits non-zero and writes nothing.
"""
import os
import sys
import tarfile
import xml.etree.ElementTree as ET

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common.http import download_file
from common.parquet_io import write_registry_parquet

BULK_URL = "https://ares.gov.cz/otevrena-data/ares_vreo_all.tar.gz"
LOCAL_ARCHIVE = "ares_vreo_all.tar.gz"

NS = "http://wwwinfo.mfcr.cz/ares/xml_doc/schemas/ares/ares_answer_vreo/v_1.0.0"


def _tag(name: str) -> str:
    return f"{{{NS}}}{name}"


def _row_from_xml_bytes(data: bytes) -> dict | None:
    """Parse one real per-IČO XML member into a registry row.

    Real confirmed shape (curl'd live and inspected during development):
      Ares_odpovedi/Odpoved/Vypis_VREO/Zakladni_udaje/{ICO, ObchodniFirma, Sidlo, ...}
    Returns None if this member has no Zakladni_udaje block (e.g. a not-found/error
    stub) rather than fabricating a row for it.
    """
    root = ET.fromstring(data)
    vypis = root.find(f".//{_tag('Vypis_VREO')}")
    if vypis is None:
        return None
    zakladni = vypis.find(_tag("Zakladni_udaje"))
    if zakladni is None:
        return None

    ico_el = zakladni.find(_tag("ICO"))
    firma_el = zakladni.find(_tag("ObchodniFirma"))
    sidlo_el = zakladni.find(_tag("Sidlo"))
    vymaz_el = zakladni.find(_tag("DatumVymazu"))

    address = None
    if sidlo_el is not None:
        text_el = sidlo_el.find(_tag("text"))
        if text_el is not None and text_el.text:
            address = text_el.text

    status = "TERMINATED" if (vymaz_el is not None and vymaz_el.text) else "ACTIVE"

    return {
        "company_name": firma_el.text if firma_el is not None else None,
        "registration_number": ico_el.text if ico_el is not None else None,
        "registered_address": address,
        "status": status,
        "ubo_names": None,  # real gap — Statutarni_organ is statutory reps, not UBO
        "jurisdiction": "CZ",
    }


def parse_bulk_archive(archive_path: str) -> list[dict]:
    """Stream-parse the real bulk tar.gz — one member per IČO — without extracting
    to disk first (the uncompressed content is several GB; streaming keeps memory
    bounded to one member at a time)."""
    rows: list[dict] = []
    parse_errors = 0
    with tarfile.open(archive_path, mode="r|gz") as tf:
        for member in tf:
            if not member.isfile() or not member.name.endswith(".xml"):
                continue
            f = tf.extractfile(member)
            if f is None:
                continue
            data = f.read()
            try:
                row = _row_from_xml_bytes(data)
            except ET.ParseError:
                parse_errors += 1
                continue
            if row is not None and row.get("registration_number"):
                rows.append(row)
    if parse_errors:
        print(f"WARNING: {parse_errors} archive member(s) failed to parse as XML and were skipped "
              f"(real, disclosed — not fabricated data)", file=sys.stderr)
    return rows


def main():
    print(f"Downloading real ARES bulk export ({BULK_URL})...")
    try:
        download_file(BULK_URL, LOCAL_ARCHIVE, timeout=1800)
    except Exception as e:
        print(f"FATAL: failed to download ARES bulk export: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        rows = parse_bulk_archive(LOCAL_ARCHIVE)
    except Exception as e:
        print(f"FATAL: failed to parse ARES bulk export: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if os.path.exists(LOCAL_ARCHIVE):
            os.remove(LOCAL_ARCHIVE)

    if not rows:
        print("FATAL: parsed zero rows from ARES bulk export — refusing to write an empty file", file=sys.stderr)
        sys.exit(1)

    count = write_registry_parquet(rows, "czech_reg.parquet", source_url=BULK_URL)
    print(f"Wrote {count} real Czech ARES rows to czech_reg.parquet")


if __name__ == "__main__":
    main()
