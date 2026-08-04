"""Real ingestion of Finland's PRH (Patentti- ja rekisterihallitus / Finnish Patent
and Registration Office) open data — the YTJ (Business Information System) API. No
key required.

Source: https://avoindata.prh.fi/opendata-ytj-api/v3/companies
Docs / landing pages: https://avoindata.prh.fi/en,
https://www.prh.fi/en/companiesandorganisations/tietopalvelut/prhopendata.html

Confirmed live (verified by direct HTTP calls against the real endpoint, not docs
alone):
  - GET /opendata-ytj-api/v3/companies?maxResults=100&page=N
  - `maxResults` is accepted but the API always returns 100 rows/page regardless of
    the value requested; pagination is purely `page`-based (1-indexed), incrementing
    sequentially by businessId. An out-of-range page returns HTTP 200 with an empty
    `companies` list (not an error) — this is the real end-of-data signal, no
    `totalResults`-based stopping needed even though the payload also carries a
    `totalResults` count (822,379 companies as of 2026-08-04).
  - Each company record has: `businessId.value` (Y-tunnus), `names[]` (with
    `endDate` marking historical names — the entry with no `endDate` is current),
    `addresses[]` (street/postCode/postOffices, `endDate` marks historical),
    `companyForms[]`, `mainBusinessLine`, and `registeredEntries[]` — a log of trade
    register events (type "1" = Registered, "4" = Ceased, "0" = Unregistered, plus
    others) each carrying human-readable `descriptions` per language code
    (`languageCode` "1" = Finnish, "2" = Swedish, "3" = English). There is no
    separate top-level "status" enum worth trusting on its own (empirically it does
    not track active/dissolved in this dataset); the real signal is the *current*
    (no `endDate`) entry in `registeredEntries` for `register == "1"` (kaupparekisteri
    / the actual Trade Register, as opposed to e.g. the VAT or employer registers
    tracked under other `register` values) — we use that entry's English
    description directly as `status` (e.g. "Registered", "Ceased", "Unregistered")
    rather than inventing our own ACTIVE/TERMINATED vocabulary.
  - No beneficial-owner (UBO) data anywhere in this API. Finland's UBO register
    (ilmoita tosiasiallisista edunsaajista) is a separate PRH product that is NOT
    part of this open dataset and is not confirmed to be openly/freely queryable
    the same way — so `ubo_names` is left `None` for every row. This is a real gap
    in the source, not a bug: never fabricate a UBO value here.

Full production crawl: loop `page` from 1 upward until an empty `companies` page is
returned (~8,224 pages for the full ~822k-company register at 100 rows/page). This
script has no artificial page cap — see MAX_PAGES below, which is None by default
and must stay that way in anything shipped.

If a page request fails after retries, or the very first page returns zero
companies (e.g. endpoint gone / schema changed), this script exits non-zero and
writes nothing.
"""
import os
import sys

import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common.http import get_with_retry
from common.parquet_io import write_registry_parquet

BASE_URL = "https://avoindata.prh.fi/opendata-ytj-api/v3/companies"
PAGE_SIZE = 100  # the API always returns 100 rows/page; maxResults is accepted but ignored

# Must be None in the shipped script — a full production run pages until the API
# itself signals end-of-data (an empty `companies` list), it is never truncated by
# a hardcoded page count. Only ever set to an int for a throwaway local smoke test,
# and never commit it set.
MAX_PAGES = None

ENGLISH = "3"


def _current_or_latest(entries: list[dict], date_field: str = "registrationDate") -> dict | None:
    """Prefer the entry with no endDate (currently valid); else the most recent by date_field."""
    if not entries:
        return None
    current = [e for e in entries if not e.get("endDate")]
    if current:
        return current[0]
    return sorted(entries, key=lambda e: e.get(date_field) or "", reverse=True)[0]


def _english_description(entry: dict | None) -> str | None:
    if not entry:
        return None
    for desc in entry.get("descriptions") or []:
        if desc.get("languageCode") == ENGLISH:
            return desc.get("description")
    return None


def _company_name(company: dict) -> str | None:
    current = _current_or_latest(company.get("names") or [])
    return current.get("name") if current else None


def _registered_address(company: dict) -> str | None:
    current = _current_or_latest(company.get("addresses") or [])
    if not current:
        return None

    city = None
    for post_office in current.get("postOffices") or []:
        if post_office.get("languageCode") == "1":  # Finnish name, matches the postCode
            city = post_office.get("city")
            break
    if city is None and current.get("postOffices"):
        city = current["postOffices"][0].get("city")

    street = current.get("street") or ""
    building_number = current.get("buildingNumber") or ""
    entrance = current.get("entrance") or ""
    street_line = " ".join(p for p in [street, building_number, entrance] if p).strip()

    parts = [p for p in [street_line, current.get("postCode"), city] if p]
    return ", ".join(parts) if parts else None


def _status(company: dict) -> str | None:
    trade_register_entries = [
        e for e in (company.get("registeredEntries") or []) if e.get("register") == "1"
    ]
    current = _current_or_latest(trade_register_entries)
    return _english_description(current)


def _rows_from_page(companies: list[dict]) -> list[dict]:
    rows = []
    for company in companies:
        business_id = (company.get("businessId") or {}).get("value")
        company_name = _company_name(company)
        if not business_id or not company_name:
            continue

        rows.append({
            "company_name": company_name,
            "registration_number": business_id,
            "registered_address": _registered_address(company),
            "status": _status(company),
            "ubo_names": None,  # PRH's UBO register is a separate product, not in this API
            "jurisdiction": "FI",
        })
    return rows


def main():
    print("Crawling Finland PRH/YTJ open data (avoindata.prh.fi/opendata-ytj-api/v3/companies)...")
    rows: list[dict] = []
    page = 1
    page_count = 0

    try:
        while True:
            if MAX_PAGES is not None and page_count >= MAX_PAGES:
                print(f"...stopping at local MAX_PAGES={MAX_PAGES} (not for production use)")
                break

            url = f"{BASE_URL}?maxResults={PAGE_SIZE}&page={page}"
            response = get_with_retry(url, timeout=60)
            payload = response.json()
            companies = payload.get("companies") or []
            if not companies:
                break

            rows.extend(_rows_from_page(companies))
            page_count += 1
            page += 1
            if page_count % 100 == 0:
                print(f"...{page_count} pages, {len(rows)} rows so far")
    except (requests.RequestException, ValueError) as e:
        print(f"FATAL: failed to crawl Finland PRH/YTJ open data: {e}", file=sys.stderr)
        sys.exit(1)

    if not rows:
        print("FATAL: parsed zero rows from Finland PRH/YTJ open data — refusing to write an empty file", file=sys.stderr)
        sys.exit(1)

    count = write_registry_parquet(rows, "finland_reg.parquet", source_url=BASE_URL)
    print(f"Wrote {count} real Finland PRH/YTJ rows ({page_count} pages) to finland_reg.parquet")


if __name__ == "__main__":
    main()
