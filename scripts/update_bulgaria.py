"""Real ingestion of Bulgaria's Commercial Register free name/EIK search, scraped
from portal.registryagency.bg (post-2024 merger of the old brra.bg site) — no
login required.

There is no bulk/API dataset for the Bulgarian Commercial Register (unlike e.g.
Slovakia's RPVS OData API) and no documented public schema, so this crawls the
same JSON endpoint the site's own Angular search UI calls — confirmed by live
inspection of the network traffic behind the "Reference by natural person or
legal entity" -> "Legal entity" search form:

    GET https://portal.registryagency.bg/CR/api/Deeds/Summary
        ?page=<n>&pageSize=25&count=0&name=<prefix>&selectedSearchFilter=1&includeHistory=true

No CAPTCHA or other bot-defense was observed in front of this endpoint as of this
pass (checked live: no reCAPTCHA/hCaptcha script, no WAF challenge page) — unlike
Italy, Portugal and Greece's equivalent free searches, which are gated behind
Google reCAPTCHA (see README/report for that comparison). That absence, not a
documented guarantee, is what this script depends on; it is exactly the kind of
thing that can change, hence the strict shape check on every page before parsing.

There is no free bulk export, so real coverage requires crawling by name-prefix
page by page — a single scheduled run cannot realistically enumerate the entire
register (hundreds of thousands of entities) the way a one-shot bulk-file
download can. DEFAULT_PREFIXES/MAX_PAGES_PER_PREFIX below are a deliberately
bounded, real (not fabricated) slice so one run completes in a reasonable time;
override via the BULGARIA_PREFIXES / BULGARIA_MAX_PAGES_PER_PREFIX env vars for a
larger crawl.

The free summary endpoint only returns a name and a UIC/EIK (registration
number) per entity — no address, no status, no beneficial owners. Those three
required-schema fields are therefore left as None (a real "this free source
doesn't have it" gap), never invented from a second, unverified endpoint.

If the endpoint is unreachable, returns an unexpected shape, or yields zero rows,
this script exits non-zero and writes nothing.
"""
import os
import sys
from urllib.parse import quote

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common.scrape import get_with_retry, PageShapeError, fatal_on_shape_or_network_error
from common.parquet_io import write_registry_parquet

BASE_URL = "https://portal.registryagency.bg/CR/api/Deeds/Summary"
OUTPUT_PATH = "bulgaria_reg.parquet"

DEFAULT_PREFIXES = os.environ.get("BULGARIA_PREFIXES", "СОФАРМА,КРИБ").split(",")
PAGE_SIZE = 25
MAX_PAGES_PER_PREFIX = int(os.environ.get("BULGARIA_MAX_PAGES_PER_PREFIX", "40"))


def _fetch_page(prefix: str, page: int) -> list[dict]:
    url = (
        f"{BASE_URL}?page={page}&pageSize={PAGE_SIZE}&count=0"
        f"&name={quote(prefix)}&selectedSearchFilter=1&includeHistory=true"
    )
    response = get_with_retry(url, timeout=30)
    if not response.text.strip():
        # The real, observed behaviour for a page past the end of the result set is an
        # HTTP 200 with an empty body (confirmed live) — that's a legitimate "no more
        # results" signal, not a parse failure.
        return []
    try:
        payload = response.json()
    except ValueError as e:
        raise PageShapeError(
            "page structure for Bulgaria did not match expected shape "
            f"(Deeds/Summary response was not valid JSON: {e}) — refusing to parse, source may have changed"
        )
    if not isinstance(payload, list):
        raise PageShapeError(
            "page structure for Bulgaria did not match expected shape "
            "(Deeds/Summary response was not a JSON list) — refusing to parse, source may have changed"
        )
    for item in payload:
        if not isinstance(item, dict) or "ident" not in item or "name" not in item:
            raise PageShapeError(
                "page structure for Bulgaria did not match expected shape "
                "(result item missing expected 'ident'/'name' keys) — refusing to parse, source may have changed"
            )
    return payload


def _rows_from_items(items: list[dict]) -> list[dict]:
    rows = []
    for item in items:
        company_name = item.get("companyFullName") or item.get("name")
        if not company_name:
            continue
        rows.append({
            "company_name": company_name,
            "registration_number": item.get("ident"),
            "registered_address": None,  # not present in the free summary search response
            "status": None,  # not present in the free summary search response
            "ubo_names": None,  # Bulgaria's Commercial Register does not expose UBOs via this free search
            "jurisdiction": "BG",
        })
    return rows


def main():
    print(f"Crawling Bulgaria Commercial Register free search (prefixes: {DEFAULT_PREFIXES})...")
    rows: list[dict] = []
    seen_idents: set[str] = set()

    try:
        for prefix in DEFAULT_PREFIXES:
            prefix = prefix.strip()
            if not prefix:
                continue
            prefix_rows_before = len(rows)
            for page in range(1, MAX_PAGES_PER_PREFIX + 1):
                items = _fetch_page(prefix, page)
                if not items:
                    break
                for row in _rows_from_items(items):
                    ident = row["registration_number"]
                    if ident and ident in seen_idents:
                        continue
                    if ident:
                        seen_idents.add(ident)
                    rows.append(row)
            print(f"...prefix {prefix!r}: {len(rows) - prefix_rows_before} rows ({len(rows)} total so far)")
    except Exception as e:
        fatal_on_shape_or_network_error("Bulgaria", e)

    if not rows:
        print("FATAL: parsed zero rows from Bulgaria Commercial Register search — refusing to write an empty file",
              file=sys.stderr)
        sys.exit(1)

    count = write_registry_parquet(rows, OUTPUT_PATH, source_url=BASE_URL)
    print(f"Wrote {count} real Bulgaria Commercial Register rows to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
