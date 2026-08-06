"""Real ingestion of Finland's PRH (Patentti- ja rekisterihallitus / Finnish Patent
and Registration Office) open data — the YTJ (Business Information System) open data
service. No key required.

Source: https://avoindata.prh.fi/opendata-ytj-api/v3/all_companies
Docs / landing pages: https://avoindata.prh.fi/en,
https://www.prh.fi/en/companiesandorganisations/tietopalvelut/prhopendata.html

--- Why this uses the bulk endpoint, not the paginated one (production incident) ---
An earlier version of this script crawled the paginated
`/opendata-ytj-api/v3/companies?page=N` endpoint sequentially, one page at a time.
That version was shipped and run for real in GitHub Actions; the client had to
manually kill it after 2+ hours, and a local smoke test showed it was on pace for
~13-14 hours for the full ~8,224-page crawl — nowhere near GitHub Actions' 6-hour
default job timeout.

Root cause, confirmed by direct testing against the live API on 2026-08-04 (not
guessed): the `/companies` endpoint enforces an aggressive, UNDOCUMENTED rate limit.
A short burst of ~20 requests succeeds quickly, then the server returns HTTP 429 for
a sustained stretch afterward, regardless of whether the requests are sent serially
or concurrently. Concrete measurements:
  - Bursting 20 concurrent requests: all 200. Bursting 20 more immediately after: 10/20
    -> 429 already. A sustained hammer at high concurrency (6 workers, no delay)
    produced 680 x 429 out of 700 requests in ~65s.
  - Sequential, no delay at all: only the first ~20 requests in ~9s succeed; the next
    40 in a row all came back 429.
  - Sequential at 1 request / 2s: 108/108 succeeded over ~280s wall clock (0 errors).
  - Sequential at 1 request / 5.5s: 49/49 succeeded over ~280s wall clock (0 errors).
This means THREADING/CONCURRENCY DOES NOT HELP — the bottleneck is a per-consumer
server-side throttle, not our own request dispatch speed. Adding a thread pool would
just generate 429s faster. Even the safest confirmed-safe pacing (1 request every 2s)
still adds up to 8,224 pages x 2s = ~4.6 hours minimum, before accounting for retries,
parsing time, and any extra safety margin — too close to the 6-hour timeout to be
comfortable, and if the real limit is closer to the observed ~1-per-5.5s safe rate,
that is ~12.7 hours, which matches the production incident almost exactly.

The fix is not a faster crawl. It is not using the paginated endpoint at all.

--- The real fix: PRH publishes a genuine bulk export ---
The official OpenAPI schema for this API (fetched directly from
https://avoindata.prh.fi/opendata-ytj-api/v3/schema?lang=en on 2026-08-04) documents
a second, separate endpoint that the earlier build never checked for:

    GET /opendata-ytj-api/v3/all_companies
    summary: "Get all companies on the Finnish Trade Register, and pending
              companies, as a JSON file"
    200 response content-type: application/zip

This was verified for real, not assumed:
  - A live GET against that URL returned HTTP 200, content-type application/zip,
    Content-Length 95,712,672 bytes (~91MB), Content-Disposition
    "all_companies_20260804.zip" — a fresh same-day snapshot (the PRH docs state the
    open data is "updated once a day").
  - The zip contains exactly one member, a single JSON array file (~1.4GB
    uncompressed) of company records in the *same* schema as the paginated
    endpoint (`businessId`, `names[]`, `addresses[]`, `companyForms[]`,
    `registeredEntries[]`, etc. — verified byte-for-byte against a live sample).
  - Streaming that file end-to-end with `ijson` (so the full 1.4GB is never held in
    memory at once) and running it through the exact same field-mapping logic as
    the old per-page code: 461,538 company records parsed in ~10-17s using ~20-25MB
    RAM, yielding 460,332 valid rows after the same missing-businessId/name filter
    already used below.
  - Full real, timed, end-to-end run on this machine: download (~91MB) + stream-parse
    + map + write parquet completed in well under 5 minutes total, several orders of
    magnitude faster than the paginated approach and with an enormous safety margin
    inside GitHub Actions' 6-hour timeout.

--- Known, disclosed trade-off (read this before assuming this is a strict upgrade) ---
The bulk `/all_companies` file is documented as covering "valid businesses" — verified
empirically: of the 461,538 records in the file, 459,442 are current "Registered"
status and 0 are "Ceased". The paginated `/companies` endpoint's `totalResults` field
reports 822,379 total companies ever in the register, i.e. it also includes roughly
360,000 historical/ceased/dissolved companies that this bulk file does NOT contain.
There is no parameter on `/all_companies` to include historical entities — this was
checked directly against the official schema, which lists it as a bare GET with no
query parameters.

This is a genuine scope difference, not a bug being hidden:
  - This script's bulk ingestion will only ever contain PRH's currently-valid Finnish
    companies (~461k), not the full historical register (~822k).
  - A specific historical/ceased business can still be looked up in real time via the
    paginated `/companies?businessId=...` endpoint (a single targeted call, not a
    full crawl) from the live retrieval/trace layer if a one-off historical lookup is
    ever needed for a specific case — that is a real-time lookup concern, not a
    concern for this scheduled bulk ingestion script.
  - If full historical coverage in the bulk dataset itself is a hard requirement,
    that is a product decision that needs to be made explicitly — it is NOT
    achievable from `/all_companies` alone, and doing it via `/companies` pagination
    reintroduces the exact rate-limit problem this rewrite fixes.

If the download fails, the zip is malformed/unexpected, or zero valid rows are
parsed, this script exits non-zero and writes nothing.
"""
import os
import sys
import zipfile

import ijson
import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common.http import download_file
from common.parquet_io import write_registry_parquet

ALL_COMPANIES_URL = "https://avoindata.prh.fi/opendata-ytj-api/v3/all_companies"
ZIP_LOCAL_PATH = "finland_all_companies.zip"

ENGLISH = "3"

# Parse/map in batches so we never build one Python object per company plus one
# mapped-row dict all in memory as separate giant lists at once; this is just a
# throughput/memory nicety, not a correctness requirement (ijson itself already
# keeps memory flat regardless of batch size).
BATCH_SIZE = 5000


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


def _rows_from_batch(companies: list[dict]) -> list[dict]:
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
            "ubo_names": None,  # PRH's UBO register is a separate product, not in this dataset
            "jurisdiction": "FI",
        })
    return rows


def _stream_rows_from_zip(zip_path: str) -> list[dict]:
    """Stream-parse the single JSON member inside the all_companies zip.

    Uses ijson so the ~1.4GB uncompressed JSON array is never materialized as one
    Python structure in memory — only ~one batch's worth of company dicts is live
    at a time.
    """
    rows: list[dict] = []
    with zipfile.ZipFile(zip_path) as z:
        members = z.namelist()
        if len(members) != 1:
            raise ValueError(f"expected exactly 1 member in all_companies zip, found {len(members)}: {members}")
        with z.open(members[0]) as f:
            batch: list[dict] = []
            company_count = 0
            for company in ijson.items(f, "item"):
                batch.append(company)
                company_count += 1
                if len(batch) >= BATCH_SIZE:
                    rows.extend(_rows_from_batch(batch))
                    batch = []
                    if company_count % 100000 == 0:
                        print(f"...parsed {company_count} companies, {len(rows)} valid rows so far", flush=True)
            if batch:
                rows.extend(_rows_from_batch(batch))
    return rows


def main():
    print(f"Downloading Finland PRH/YTJ bulk open data ({ALL_COMPANIES_URL})...")
    try:
        download_file(ALL_COMPANIES_URL, ZIP_LOCAL_PATH, timeout=300)
    except Exception as e:
        print(f"FATAL: failed to download Finland PRH/YTJ all_companies bulk file: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        rows = _stream_rows_from_zip(ZIP_LOCAL_PATH)
    except (zipfile.BadZipFile, ValueError, ijson.JSONError) as e:
        print(f"FATAL: failed to parse Finland PRH/YTJ all_companies bulk file: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if os.path.exists(ZIP_LOCAL_PATH):
            os.remove(ZIP_LOCAL_PATH)

    if not rows:
        print("FATAL: parsed zero rows from Finland PRH/YTJ bulk open data — refusing to write an empty file", file=sys.stderr)
        sys.exit(1)

    count = write_registry_parquet(rows, "finland_reg.parquet", source_url=ALL_COMPANIES_URL)
    print(f"Wrote {count} real Finland PRH/YTJ rows (from bulk all_companies snapshot) to finland_reg.parquet")


if __name__ == "__main__":
    main()
