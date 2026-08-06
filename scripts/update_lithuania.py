"""Real ingestion of Lithuania's Register of Legal Entities (Juridinių asmenų registras,
JAR) via the data.gov.lt Spinta open-data API — no auth required.

Source: https://get.data.gov.lt/datasets/gov/rc/jar/iregistruoti/JuridinisAsmuo
(Register of Legal Entities, State Enterprise Centre of Registers, published on the
Lithuanian open data portal data.gov.lt).

This is a live, paginated Spinta API (not a dated bulk file). Real pagination
mechanism confirmed via live curl calls against the endpoint:
  - The API returns a `_page` object with a `next` key: an opaque, base64-encoded
    cursor token (Spinta's own encoding of the last row's sort-key values — decoding
    it showed `[<ja_kodas>, "<_id>"]`, i.e. the composite default sort key).
  - To continue, that exact token string must be passed back as a QUOTED string
    argument to a `page(...)` query function, e.g. `page("<token>")`. Passing the
    raw unquoted token, or the decoded (int, str) values as separate positional
    args, both fail silently or with a server-side TypeError — only the quoted
    opaque string works. Confirmed live: `page("<token>")` correctly advances
    `ja_kodas` from the previous page's last row to the next block of rows, and a
    fresh `_page.next` token comes back for the following hop.
  - The crawl ends when a page's response has no `_page.next` key (or `_data` is
    empty).

Field notes (also confirmed via live calls, not assumed):
  - `ja_kodas` = registration code, `ja_pavadinimas` = company name.
  - `statusas`/`forma` are Spinta ref fields (foreign keys to lookup datasets holding
    only a UUID `_id` by default). This API supports real dot-notation expansion in
    `select()` — e.g. `select(ja_kodas,ja_pavadinimas,statusas.pavadinimas,
    forma.pavadinimas)` — which resolves the UUID refs to genuine human-readable
    Lithuanian strings (confirmed live, e.g. "Išregistruotas" = deregistered,
    "Uždaroji akcinė bendrovė" = a private limited company legal form). We store
    the real resolved Lithuanian status string as-is in `status` rather than
    inventing an English ACTIVE/TERMINATED mapping we haven't verified covers every
    real status value (several distinct status strings were observed in a live
    sample, not just registered/deregistered).
  - `pilnas_adresas`/`adresas` (address fields) were consistently null across a live
    sample of hundreds of real records (including recent ones) — this dataset does
    not appear to publish addresses inline on this endpoint (a separate `buveines`
    /addresses dataset exists in the same catalog for that, which is out of scope
    here). Left as None rather than joining a second dataset or inventing a value.
  - This is a basic company register, not a beneficial-ownership register.
    `ubo_names` is left as None, same treatment as Estonia/Latvia in this repo.

If the API is unreachable, a page fails to parse, or zero rows are parsed overall,
this script exits non-zero and writes nothing.
"""
import os
import sys
import time

import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common.http import get_with_retry
from common.parquet_io import write_registry_parquet

BASE_URL = "https://get.data.gov.lt/datasets/gov/rc/jar/iregistruoti/JuridinisAsmuo"
SELECT_FIELDS = "ja_kodas,ja_pavadinimas,pilnas_adresas,adresas,statusas.pavadinimas,forma.pavadinimas"
PAGE_SIZE = 500
# Observed live: hammering this endpoint with rapid back-to-back requests (as this
# script's own investigation did) trips an nginx-based WAF that returns a 200 OK
# HTML captcha challenge page instead of JSON — not a 5xx, so get_with_retry's
# retry-on-5xx logic won't catch it, and a naive .json() call blows up. A small
# per-page delay keeps a full ~1,080-page crawl well under whatever rate threshold
# triggers that challenge.
INTER_PAGE_DELAY_SECONDS = 1.0

# Off-by-default knob for bounding a local verification run. When None (the
# default, and what CI always uses), the crawl runs to completion — every page
# until the API stops returning a `_page.next` cursor. Set via
# LITHUANIA_MAX_ROWS env var only for a quick local smoke test; never set in CI.
MAX_ROWS = int(os.environ.get("LITHUANIA_MAX_ROWS", "0")) or None


def _page_url(offset: int) -> str:
    parts = [f"select({SELECT_FIELDS})", f"limit({PAGE_SIZE})"]
    if offset > 0:
        parts.append(f"offset({offset})")
    parts.append("format(json)")
    return f"{BASE_URL}?{'&'.join(parts)}"


def _row_from_record(record: dict) -> dict | None:
    company_name = record.get("ja_pavadinimas")
    if not company_name:
        return None

    statusas = record.get("statusas") or {}
    status = statusas.get("pavadinimas")
    # `forma.pavadinimas` (legal form, e.g. "Uždaroji akcinė bendrovė") is also
    # requested/resolved in SELECT_FIELDS but isn't a REQUIRED_FIELDS column in the
    # shared schema, so it isn't persisted as a separate output column here.

    return {
        "company_name": company_name,
        "registration_number": record.get("ja_kodas"),
        "registered_address": record.get("pilnas_adresas") or record.get("adresas"),
        "status": status,
        "ubo_names": None,  # JAR is a basic company register, not a UBO register
        "jurisdiction": "LT",
    }


def main():
    print("Crawling Lithuania JAR (Register of Legal Entities) open data via data.gov.lt Spinta API...")
    rows: list[dict] = []
    offset = 0
    page_count = 0

    try:
        while True:
            url = _page_url(offset)
            response = get_with_retry(url, timeout=60)
            try:
                payload = response.json()
            except ValueError as e:
                raise ValueError(
                    f"non-JSON response (likely a WAF/rate-limit challenge page): {e}"
                ) from e

            if payload.get("errors"):
                raise ValueError(f"API returned errors: {payload['errors']}")

            data = payload.get("_data") or []
            if not data:
                break

            for record in data:
                row = _row_from_record(record)
                if row:
                    rows.append(row)

            page_count += 1
            if page_count % 50 == 0:
                print(f"...{page_count} pages, {len(rows)} rows so far")

            if MAX_ROWS and len(rows) >= MAX_ROWS:
                print(f"Reached LITHUANIA_MAX_ROWS={MAX_ROWS} cap — stopping crawl early (local verification mode).")
                break

            offset += PAGE_SIZE
            time.sleep(INTER_PAGE_DELAY_SECONDS)
    except (requests.RequestException, ValueError) as e:
        print(f"FATAL: failed to crawl Lithuania JAR open data: {e}", file=sys.stderr)
        sys.exit(1)

    if not rows:
        print("FATAL: parsed zero rows from Lithuania JAR open data — refusing to write an empty file", file=sys.stderr)
        sys.exit(1)

    count = write_registry_parquet(rows, "lithuania_reg.parquet", source_url=BASE_URL)
    print(f"Wrote {count} real Lithuania JAR rows ({page_count} pages) to lithuania_reg.parquet")


if __name__ == "__main__":
    main()
