"""Real ingestion of Poland's KRS (Krajowy Rejestr Sadowy) open API — no auth required.

Source: https://api-krs.ms.gov.pl/api/krs/OdpisAktualny/{krsNumber}?rejestr=P&format=json
Officially announced at:
  https://www.gov.pl/web/sprawiedliwosc/uruchomienie-otwartego-api-krajowego-rejestru-sadowego
  (links to the public portal at https://prs.ms.gov.pl/krs/openApi)

WHY THIS SCRIPT ENUMERATES BY KRS NUMBER INSTEAD OF SEARCHING:
The Ministry of Justice's own announcement states the API's informational scope is
limited to "odpis pelny i aktualny" (the full/current excerpt) for a single entity —
i.e. lookup-by-ID only. We confirmed this live before writing this script:
  - GET /api/krs/wyszukaj -> 404 (no such route)
  - https://api-krs.ms.gov.pl/swagger/v1/swagger.json -> 404 (no machine-readable spec)
  - https://prs.ms.gov.pl/krs/openApi/swagger.json -> HTTP 200 but Content-Type
    text/html; it's the Angular SPA's index.html served for every route (SPA
    fallback), not a real OpenAPI document.
  - The gov.pl announcement page itself only describes the single-record
    OdpisPelny/OdpisAktualny excerpt as the API's entire informational scope; it
    documents no search/list operation.
There is genuinely no bulk search or list endpoint. KRS numbers, however, are
sequential 10-digit IDs assigned in registration order, and the API returns a
real 404 for any unassigned number and a real 200 (or occasionally 204 No
Content) for an assigned one. So this script walks a bounded, contiguous block
of KRS numbers and keeps only the ones that resolve to a real record. A 404/204
is treated as "no entity at this ID" and skipped — never an error. Any other
failure (network error, 5xx after retries, malformed JSON on a 200) is fatal.

HONESTY ABOUT COVERAGE: KRS holds roughly 500k+ live entities across a much
larger number range. Scanning KRS_RANGE_START..KRS_RANGE_END below samples only
that slice per run — the same "partial coverage per run" model this repo
already uses for other lookup-by-ID-only sources. Widen the range (or run the
script repeatedly with different ranges) to build broader coverage over time;
do not treat one run's output as the full Polish registry.

FIELD MAPPING (from the real OdpisAktualny JSON shape observed live for
KRS 0000006865 "CD PROJEKT SPOLKA AKCYJNA" and KRS 0000500000, a foundation in
liquidation):
  - company_name          <- dane.dzial1.danePodmiotu.nazwa
  - registration_number   <- naglowekA.numerKRS (the KRS number itself)
  - registered_address    <- dane.dzial1.siedzibaIAdres.adres (street/house/postal/city)
  - status                <- derived from dane.dzial6: presence of a "likwidacja"
                             section -> "W LIKWIDACJI" (in liquidation); presence
                             of "rozwiazanieUniewaznienie" -> "ROZWIAZANA" (dissolved);
                             otherwise None (OdpisAktualny does not carry an explicit
                             "ACTIVE" flag, so we do not invent one — we only report
                             what dzial6 actually states, or leave status null)
  - ubo_names             <- None always. We inspected the full OdpisAktualny JSON
                             tree (dzial1..dzial6) for KRS 0000006865 and
                             0000500000 and found no beneficial-owner data anywhere
                             (no "beneficjent" key of any kind). Poland's UBO data
                             lives in the separate CRBR register, not KRS, so this
                             is genuinely absent from this source.
  - jurisdiction          <- "PL" (constant)

If a request fails (non-404/204 error) or zero real rows are parsed across the
whole scanned range, this script exits non-zero and writes nothing.
"""
import os
import sys
import time

import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common.http import get_with_retry
from common.parquet_io import write_registry_parquet

BASE_URL = "https://api-krs.ms.gov.pl/api/krs/OdpisAktualny"

# Bounded, contiguous slice of KRS numbers to scan this run. KRS numbers are
# sequential 10-digit IDs; this range is a sample, not the whole ~500k+ register.
# Widen/move this range across runs to build broader coverage over time.
KRS_RANGE_START = 500000
KRS_RANGE_END = 500300  # exclusive

# Politeness delay between requests that come back as "no entity" (404/204), so a
# long run of misses doesn't hammer the API needlessly.
MISS_SLEEP_SECONDS = 0.05


def _address_from(siedziba_i_adres: dict) -> str | None:
    adres = siedziba_i_adres.get("adres") or {}
    if not adres:
        return None
    parts = [
        adres.get("ulica"),
        adres.get("nrDomu"),
        adres.get("nrLokalu"),
        adres.get("kodPocztowy"),
        adres.get("miejscowosc"),
        adres.get("kraj"),
    ]
    joined = ", ".join(p for p in parts if p)
    return joined or None


def _status_from(dzial6: dict) -> str | None:
    if not dzial6:
        return None
    if dzial6.get("likwidacja"):
        return "W LIKWIDACJI"
    if dzial6.get("rozwiazanieUniewaznienie"):
        return "ROZWIAZANA"
    return None


def _row_from_odpis(payload: dict) -> dict | None:
    odpis = payload.get("odpis") or {}
    naglowek = odpis.get("naglowekA") or {}
    dane = odpis.get("dane") or {}
    dzial1 = dane.get("dzial1") or {}
    dane_podmiotu = dzial1.get("danePodmiotu") or {}

    company_name = dane_podmiotu.get("nazwa")
    krs_number = naglowek.get("numerKRS")
    if not company_name or not krs_number:
        return None

    return {
        "company_name": company_name,
        "registration_number": krs_number,
        "registered_address": _address_from(dzial1.get("siedzibaIAdres") or {}),
        "status": _status_from(dane.get("dzial6") or {}),
        "ubo_names": None,  # KRS OdpisAktualny does not expose UBO data (see docstring)
        "jurisdiction": "PL",
    }


def main():
    print(
        f"Scanning KRS numbers {KRS_RANGE_START:010d}-{KRS_RANGE_END - 1:010d} "
        f"against api-krs.ms.gov.pl OdpisAktualny..."
    )
    rows: list[dict] = []
    scanned = 0
    hits = 0

    try:
        for krs_int in range(KRS_RANGE_START, KRS_RANGE_END):
            krs_number = f"{krs_int:010d}"
            url = f"{BASE_URL}/{krs_number}"
            scanned += 1

            try:
                response = get_with_retry(url, params={"rejestr": "P", "format": "json"}, timeout=30)
            except requests.HTTPError as e:
                status = e.response.status_code if e.response is not None else None
                if status in (404, 204):
                    # No entity assigned at this KRS number — not an error, just skip.
                    time.sleep(MISS_SLEEP_SECONDS)
                    continue
                raise

            if response.status_code == 204:
                time.sleep(MISS_SLEEP_SECONDS)
                continue

            payload = response.json()
            row = _row_from_odpis(payload)
            if row:
                rows.append(row)
                hits += 1

            if scanned % 50 == 0:
                print(f"...scanned {scanned}, {hits} real entities found so far")
    except (requests.RequestException, ValueError) as e:
        print(f"FATAL: failed to query Poland KRS open API: {e}", file=sys.stderr)
        sys.exit(1)

    if not rows:
        print("FATAL: parsed zero rows from Poland KRS open API — refusing to write an empty file", file=sys.stderr)
        sys.exit(1)

    count = write_registry_parquet(rows, "poland_reg.parquet", source_url=BASE_URL)
    print(f"Wrote {count} real Poland KRS rows (scanned {scanned} KRS numbers) to poland_reg.parquet")


if __name__ == "__main__":
    main()
