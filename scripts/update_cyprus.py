"""Real ingestion of Cyprus's Department of Registrar of Companies (DRCOR)
"efiling" free basic search, scraped from efiling.drcor.mcit.gov.cy — no login
required.

Confirmed live (checked for CAPTCHA/anti-bot before committing to this
approach): this is a classic ASP.NET WebForms site, no CAPTCHA anywhere in the
search flow. A "start-of-name" search's results page is reachable with a plain
GET (no session/ViewState needed for this step, only for the paid/authenticated
detail views), e.g.:

    GET https://efiling.drcor.mcit.gov.cy/DrcorPublic/SearchResults.aspx
        ?name=<term>&number=%25&searchtype=optStartMatch&index=<page>

Each results page is one of exactly three shapes, and the parser distinguishes
all three explicitly rather than treating "no rows extracted" as one bucket:
  1. a results grid (id="...GridView1") — real matches, parse them;
  2. label id="...lblNoSearchResults" — a genuine, confirmed "no matches" answer;
  3. label id="...lblResultsExceeded" — the query matched too many entities and
     the site is asking for a narrower term; this is a real "try again with a
     more specific term" signal, not a parse failure, so it's logged and skipped
     rather than treated as fatal.
Anything else (none of the three present) means the page no longer looks like
what this parser was built against, and the script fails closed instead of
silently returning zero rows.

Rows with no registration number are "Αίτηση Ονόματος" (name-reservation
applications) — not yet a registered company, so they are skipped rather than
written as a company record with a fabricated/empty registration number.

The free grid only exposes name, registration number, entity type, and two
status fields (name status / organisation status) — no address, no
directors/UBOs. Clicking into a specific company's full profile requires
additional ASP.NET postback session state tied to the exact referring search
(confirmed live: a direct/replayed request to the detail postback is rejected
with "You are not allowed direct access to this page"), so registered_address
and ubo_names are left as None here — a real gap in what the *free* search
exposes, not something this script invents.

If the search endpoint is unreachable, returns an unexpected page shape, or
yields zero real rows across all configured search terms, this script exits
non-zero and writes nothing.
"""
import os
import sys
from urllib.parse import quote

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common.scrape import get_with_retry, parse_html, assert_page_shape, PageShapeError, fatal_on_shape_or_network_error
from common.parquet_io import write_registry_parquet

BASE_URL = "https://efiling.drcor.mcit.gov.cy/DrcorPublic/SearchResults.aspx"
OUTPUT_PATH = "cyprus_reg.parquet"
GRID_ID = "ctl00_cphMyMasterCentral_GridView1"
NO_RESULTS_ID = "ctl00_cphMyMasterCentral_lblNoSearchResults"
TOO_MANY_ID = "ctl00_cphMyMasterCentral_lblResultsExceeded"

DEFAULT_SEARCH_TERMS = os.environ.get("CYPRUS_SEARCH_TERMS", "TRUSTEE SERVICES").split(",")
MAX_PAGES_PER_TERM = int(os.environ.get("CYPRUS_MAX_PAGES_PER_TERM", "20"))


def _fetch_results_page(term: str, page: int):
    url = f"{BASE_URL}?name={quote(term)}&number=%25&searchtype=optStartMatch&index={page}"
    response = get_with_retry(url, timeout=30)
    soup = parse_html(response.text)

    grid = soup.find("table", id=GRID_ID)
    no_results = soup.find(id=NO_RESULTS_ID)
    too_many = soup.find(id=TOO_MANY_ID)

    assert_page_shape(soup, country="Cyprus", checks=[
        ("results grid, 'no results' label, or 'too many results' label present",
         grid is not None or no_results is not None or too_many is not None),
    ])

    if too_many is not None:
        print(f"...Cyprus term {term!r}: too many results, site asked for a narrower term — skipping")
        return None
    if grid is None:
        return []

    header_cells = grid.find("tr").find_all(["th", "td"])
    assert_page_shape(soup, country="Cyprus", checks=[
        ("results grid header has expected 7 columns", len(header_cells) == 7),
    ])
    return grid.find_all("tr")[1:]


def _row_from_tr(tr) -> dict | None:
    cells = [td.get_text(strip=True) for td in tr.find_all("td")]
    if len(cells) != 7:
        raise PageShapeError(
            "page structure for Cyprus did not match expected shape "
            f"(results row had {len(cells)} cells, expected 7) — refusing to parse, source may have changed"
        )
    _select, name, prefix, number, entity_type, name_status, org_status = cells
    if not number:
        # "Αίτηση Ονόματος" (name-reservation application) — not a registered company yet.
        return None
    return {
        "company_name": name,
        "registration_number": f"{prefix}{number}" if prefix else number,
        "registered_address": None,  # not exposed by the free search grid
        "status": org_status or name_status or None,
        "ubo_names": None,  # not exposed by the free search grid
        "jurisdiction": "CY",
    }


def main():
    print(f"Crawling Cyprus DRCOR efiling free search (terms: {DEFAULT_SEARCH_TERMS})...")
    rows: list[dict] = []

    try:
        for term in DEFAULT_SEARCH_TERMS:
            term = term.strip()
            if not term:
                continue
            term_rows_before = len(rows)
            for page in range(1, MAX_PAGES_PER_TERM + 1):
                trs = _fetch_results_page(term, page)
                if trs is None:  # too-many-results signal for this term
                    break
                if not trs:
                    break
                for tr in trs:
                    row = _row_from_tr(tr)
                    if row:
                        rows.append(row)
            print(f"...term {term!r}: {len(rows) - term_rows_before} rows ({len(rows)} total so far)")
    except Exception as e:
        fatal_on_shape_or_network_error("Cyprus", e)

    if not rows:
        print("FATAL: parsed zero rows from Cyprus DRCOR search — refusing to write an empty file", file=sys.stderr)
        sys.exit(1)

    count = write_registry_parquet(rows, OUTPUT_PATH, source_url=BASE_URL)
    print(f"Wrote {count} real Cyprus DRCOR rows to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
