"""Real ingestion of Slovakia's Register of Public Sector Partners (RPVS) — no auth
required.

Source: https://rpvs.gov.sk/opendatav2 (Ministry of Justice, OData v4, CC0 licensed).
RPVS is the register of "public sector partners" (entities doing business with the
Slovak state above a legal threshold) and genuinely publishes beneficial-owner
("konečný užívateľ výhod") data — unlike Estonia/Latvia/France, this source has real
UBO records built in, not a gap we have to leave null.

The API paginates server-side (~20 records/page, no client-controlled page size) via
@odata.nextLink, so this script crawls the full ~53k-partner set page by page.

If the API is unreachable or a page fails to parse, this script exits non-zero and
writes nothing.
"""
import os
import sys

import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common.http import get_with_retry
from common.parquet_io import write_registry_parquet

BASE_URL = "https://rpvs.gov.sk/opendatav2/Partneri"
FIRST_PAGE_URL = f"{BASE_URL}?$expand=PartneriVerejnehoSektora,KonecniUzivateliaVyhod"


def _person_name(record: dict) -> str | None:
    parts = [record.get("Meno"), record.get("Priezvisko")]
    name = " ".join(p for p in parts if p)
    return name or None


def _pick_current_or_latest(records: list[dict]) -> dict | None:
    """Prefer a record with no end date (currently valid); else the most recently started one."""
    if not records:
        return None
    current = [r for r in records if not r.get("PlatnostDo")]
    if current:
        return current[0]
    return sorted(records, key=lambda r: r.get("PlatnostOd") or "", reverse=True)[0]


def _rows_from_page(partners: list[dict]) -> list[dict]:
    rows = []
    for partner in partners:
        sector_entries = partner.get("PartneriVerejnehoSektora") or []
        current = _pick_current_or_latest(sector_entries)
        if not current:
            continue

        company_name = current.get("ObchodneMeno") or _person_name(current)
        if not company_name:
            continue

        ubo_records = partner.get("KonecniUzivateliaVyhod") or []
        ubo_names = sorted({
            name for ubo in ubo_records
            if not ubo.get("PlatnostDo") and (name := _person_name(ubo))
        })

        rows.append({
            "company_name": company_name,
            "registration_number": current.get("Ico"),
            "registered_address": None,  # requires a second $expand=Adresa hop, out of scope for this pass
            "status": "TERMINATED" if current.get("PlatnostDo") else "ACTIVE",
            "ubo_names": ", ".join(ubo_names) if ubo_names else None,
            "jurisdiction": "SK",
        })
    return rows


def main():
    print("Crawling Slovakia RPVS open data (Partneri + PartneriVerejnehoSektora + KonecniUzivateliaVyhod)...")
    rows: list[dict] = []
    url = FIRST_PAGE_URL
    page_count = 0

    try:
        while url:
            response = get_with_retry(url, timeout=60)
            payload = response.json()
            rows.extend(_rows_from_page(payload.get("value") or []))
            url = payload.get("@odata.nextLink")
            page_count += 1
            if page_count % 100 == 0:
                print(f"...{page_count} pages, {len(rows)} rows so far")
    except (requests.RequestException, ValueError) as e:
        print(f"FATAL: failed to crawl Slovakia RPVS open data: {e}", file=sys.stderr)
        sys.exit(1)

    if not rows:
        print("FATAL: parsed zero rows from Slovakia RPVS open data — refusing to write an empty file", file=sys.stderr)
        sys.exit(1)

    count = write_registry_parquet(rows, "slovakia_rpvs.parquet", source_url=BASE_URL)
    print(f"Wrote {count} real Slovakia RPVS rows ({page_count} pages) to slovakia_rpvs.parquet")


if __name__ == "__main__":
    main()
