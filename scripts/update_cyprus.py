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
     the site refuses to return any rows at all, asking for a narrower term —
     a real "cannot enumerate at this specificity" signal, not a parse failure.
Anything else (none of the three present) means the page no longer looks like
what this parser was built against, and the script fails closed instead of
silently returning zero rows.

REAL FULL-CRAWL APPROACH (recursive company-name-prefix partitioning)
----------------------------------------------------------------------
A prior version of this script only searched one hardcoded example search term
("TRUSTEE SERVICES") and shipped that to production as if it were Cyprus's
registry — it was not; it was 54 rows matching one test term. That was a
critical defect and is fixed here the same way `update_czech.py` partitions
Czech ARES's name-prefix search:

1. Seed the crawl from the real Latin alphabet A-Z plus digits 0-9 (confirmed
   live: DRCOR company names are Latin-script).
2. For each prefix, fetch the first results page.
   - `lblResultsExceeded` present -> the site itself is telling us this prefix
     is not narrow enough to enumerate at all; split it into children by
     appending each alphabet character/digit to it, and recurse into each
     child (this classification happens once per prefix, on the first page,
     confirmed live to be independent of pagination — so children are only
     ever explored, never double-counted against a parent's already-collected
     rows).
   - `lblNoSearchResults` present -> a genuine 0-match leaf, nothing to collect.
   - a results grid present -> page through with `index=1,2,...` (confirmed
     live: successive `index` values return distinct rows — this is real
     pagination, not a no-op parameter) until an empty/grid-less page is
     returned, up to `MAX_PAGES_PER_TERM` as a safety cap.
3. `MAX_PREFIX_DEPTH` is a safety valve purely to guarantee termination against
   pathological cases. If `lblResultsExceeded` still appears at max depth, this
   script does NOT fabricate/skip silently — it logs a clear WARNING naming the
   prefix and moves on with zero rows for that leaf (the site is genuinely
   refusing to serve any rows at that specificity, so there is nothing to
   partially page through, unlike Czech/Bulgaria's "page-cap" case). This is a
   real, disclosed coverage gap, not invented data. Likewise if a leaf's grid
   still has more pages at `MAX_PAGES_PER_TERM`, a WARNING is logged and only
   the pages fetched so far are kept.

HONESTY ABOUT SCALE: DRCOR holds several hundred thousand registered/historic
entities. A full A-Z0-9 crawl with the subdivision this requires is a long-
running operation (see .github/workflows/update_cyprus.yml for the scheduled
CI job) — a single local/manual run should use `--max-rows`/`CYPRUS_MAX_ROWS`
or a narrowed `--seed-alphabet` to verify behaviour, not to claim full coverage.

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
yields zero real rows across the whole crawl, this script exits non-zero and
writes nothing.
"""
import argparse
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

# Real Latin alphabet + digits — confirmed live that DRCOR company names are Latin-script.
SEED_ALPHABET = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

MAX_PAGES_PER_TERM = int(os.environ.get("CYPRUS_MAX_PAGES_PER_TERM", "20"))
MAX_PREFIX_DEPTH = int(os.environ.get("CYPRUS_MAX_PREFIX_DEPTH", "4"))  # safety valve only — see docstring


def _fetch_results_page(term: str, page: int):
    """Classify one results page as ('exceeded', None), ('empty', None), or ('grid', <trs>)."""
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
        return "exceeded", None
    if grid is None:
        return "empty", None

    header_cells = grid.find("tr").find_all(["th", "td"])
    assert_page_shape(soup, country="Cyprus", checks=[
        ("results grid header has expected 7 columns", len(header_cells) == 7),
    ])
    return "grid", grid.find_all("tr")[1:]


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


def _fetch_leaf_rows(prefix: str, first_page_trs: list) -> list[dict]:
    """Page through a prefix already confirmed (via its first page) to return a real grid,
    not a 'too many results' refusal. `first_page_trs` avoids re-fetching page 1."""
    rows: list[dict] = []
    reached_cap = True
    for page in range(1, MAX_PAGES_PER_TERM + 1):
        if page == 1:
            shape, trs = "grid", first_page_trs
        else:
            shape, trs = _fetch_results_page(prefix, page)
        if shape != "grid" or not trs:
            reached_cap = False
            break
        for tr in trs:
            row = _row_from_tr(tr)
            if row:
                rows.append(row)
    if reached_cap:
        print(
            f"WARNING: prefix {prefix!r} still had a full page of results at the "
            f"{MAX_PAGES_PER_TERM}-page safety cap — some real CY entities under this prefix "
            f"will not be captured in this run. This is a disclosed coverage limitation of this "
            f"crawl's page cap, not fabricated/skipped data.",
            file=sys.stderr,
        )
    return rows


def _crawl_prefix(prefix: str, depth: int, max_rows: int | None, stats: dict) -> list[dict]:
    if max_rows is not None and stats["collected"] >= max_rows:
        return []

    shape, trs = _fetch_results_page(prefix, 1)

    if shape == "empty":
        stats["leaves"] += 1
        return []

    if shape == "grid":
        leaf_rows = _fetch_leaf_rows(prefix, trs)
        stats["collected"] += len(leaf_rows)
        stats["leaves"] += 1
        return leaf_rows

    # shape == "exceeded"
    if depth >= MAX_PREFIX_DEPTH:
        print(
            f"WARNING: prefix {prefix!r} still returns 'too many results' at max depth "
            f"({MAX_PREFIX_DEPTH}) — the site refuses to serve any rows at this specificity, "
            f"so real CY entities under this prefix are not captured in this run. This is a "
            f"disclosed source limitation (DRCOR's free search itself, not this crawler), not "
            f"fabricated/skipped data.",
            file=sys.stderr,
        )
        stats["leaves"] += 1
        return []

    rows: list[dict] = []
    for ch in SEED_ALPHABET:
        if max_rows is not None and stats["collected"] >= max_rows:
            break
        rows.extend(_crawl_prefix(prefix + ch, depth + 1, max_rows, stats))
    return rows


def crawl(seed_alphabet: list[str], max_rows: int | None, stats: dict) -> list[dict]:
    rows: list[dict] = []
    seen_numbers: set[str] = set()
    for seed in seed_alphabet:
        if max_rows is not None and stats["collected"] >= max_rows:
            break
        before = len(rows)
        for row in _crawl_prefix(seed, depth=1, max_rows=max_rows, stats=stats):
            number = row["registration_number"]
            if number and number in seen_numbers:
                # Real, expected overlap: DRCOR's "start match" search also matches a
                # company's former name(s), so the same legal entity can legitimately
                # surface once per matching prefix (current name, former name, ...).
                # Same entity, so keep only the first row seen for it.
                continue
            if number:
                seen_numbers.add(number)
            rows.append(row)
        print(f"...seed {seed!r} done — {len(rows) - before} new rows ({len(rows)} total, {stats['leaves']} leaf partitions)")
    return rows


def main(argv: list[str] | None = None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--max-rows", type=int, default=None,
        help="Optional cap on rows collected, for local/manual verification runs only. "
             "OFF (unbounded) by default — never set this in production/CI.",
    )
    parser.add_argument(
        "--seed-alphabet", type=str, default=None,
        help="Optional comma-separated override of the top-level seed prefixes, for local/manual "
             "verification runs only. Unset by default, which uses the full real Latin alphabet + digits.",
    )
    args = parser.parse_args(argv if argv is not None else sys.argv[1:])

    max_rows = args.max_rows
    env_cap = os.environ.get("CYPRUS_MAX_ROWS")
    if max_rows is None and env_cap:
        max_rows = int(env_cap)

    seed_alphabet = SEED_ALPHABET
    if args.seed_alphabet:
        seed_alphabet = [c.strip() for c in args.seed_alphabet.split(",") if c.strip()]
    elif os.environ.get("CYPRUS_SEED_ALPHABET"):
        seed_alphabet = [c.strip() for c in os.environ["CYPRUS_SEED_ALPHABET"].split(",") if c.strip()]

    if max_rows or seed_alphabet is not SEED_ALPHABET:
        print(f"Crawling Cyprus DRCOR efiling free search (bounded test run: seeds={seed_alphabet}, max_rows={max_rows})...")
    else:
        print("Crawling Cyprus DRCOR efiling free search (full recursive name-prefix crawl)...")

    stats = {"collected": 0, "leaves": 0}
    try:
        rows = crawl(seed_alphabet, max_rows, stats)
    except Exception as e:
        fatal_on_shape_or_network_error("Cyprus", e)
        return  # unreachable — fatal_on_shape_or_network_error always exits

    if not rows:
        print("FATAL: parsed zero rows from Cyprus DRCOR search — refusing to write an empty file", file=sys.stderr)
        sys.exit(1)

    count = write_registry_parquet(rows, OUTPUT_PATH, source_url=BASE_URL)
    print(f"Wrote {count} real Cyprus DRCOR rows to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
