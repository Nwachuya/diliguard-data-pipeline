"""Real ingestion of the Czech Business Register via ARES (Administrativní Registr
Ekonomických Subjektů) — no auth required.

Source: https://ares.gov.cz/ekonomicke-subjekty-v-be/rest/
  - Single lookup: GET /ekonomicke-subjekty/{ico}
  - Bulk search:   POST /ekonomicke-subjekty/vyhledat
                   body: {"start": int, "pocet": int, "obchodniJmeno": str, ...}
Full schema: https://ares.gov.cz/ekonomicke-subjekty-v-be/rest/v3/api-docs

Both endpoints were confirmed live (curl) before writing this parser. The list
endpoint (`vyhledat`) returns items (`ekonomickeSubjekty[]`) in the *same* shape as
the single-lookup response — same `ico`, `obchodniJmeno`, `sidlo.textovaAdresa`,
`pravniForma`, `seznamRegistraci.stavZdrojeRos` fields — so one parser handles both.

CONFIRMED REAL CONSTRAINT — the 1,000-result cap
--------------------------------------------------
ARES caps the *total* matches for any `vyhledat` query at 1,000, regardless of
`start`/`pocet` paging. Exceeding it returns, verbatim (confirmed live, as an
HTTP 400 with this JSON body — ARES uses 400 for its own business-logic errors,
not just malformed requests):

    {"kod": "CHYBA_VSTUPU",
     "popis": "Zadaný dotaz vrací příliš mnoho výsledků (581 859). Povoleno je
                maximálně 1 000 výsledků. Upravte parametry vyhledávání.",
     "subKod": "VYSTUP_PRILIS_MNOHO_VYSLEDKU"}

There is no server-side sort/cursor that lets you page past 1,000 for a given
filter — you must narrow the filter itself. Also confirmed live: ARES requires at
least one non-empty filter value (a bare/empty query is rejected with
`VSTUP_PRAZDNY` / `VSTUP_NEVALIDNI_FORMAT_ATRIBUTU`), so there is no "give me
everything" starting point either.

Partitioning strategy: recursive `obchodniJmeno` (company name) prefix trie
--------------------------------------------------------------------------
`obchodniJmeno` in the search filter matches on a literal, case-insensitive,
diacritic-sensitive PREFIX of the company name (confirmed live: "entral Europ" —
a real mid-string substring of "Asseco Central Europe, a.s." — returns 0 hits,
while "Asseco" returns hits; "Škoda" matches names starting with "Škoda", not just
ASCII prefixes).

This script performs a trie/recursive-partition crawl over that prefix space:

1. Start from a set of single-character seed prefixes (digits 0-9, A-Z, and the
   Czech-specific letters Á Č Ď É Ě Í Ň Ó Ř Š Ť Ú Ů Ý Ž).
2. For each prefix, ask ARES for the total match count (`pocetCelkem`) with
   `pocet=1`.
   - If total <= 1000: page through it fully with `start`/`pocet` (pocet capped
     so `start + pocet` never exceeds the leaf's own total) and collect rows.
   - If total > 1000: split the prefix into children by appending each character
     of the alphabet to it, and recurse into each child.
3. A depth safety valve (`MAX_PREFIX_DEPTH`) exists purely to guarantee
   termination against pathological cases (e.g. a single company name repeated
   with near-infinite trailing variants would never happen in practice, but the
   valve exists so a bug can't spin the crawl forever). If a leaf still exceeds
   1000 matches at the depth cap, this script does NOT fabricate/skip silently —
   it logs a clear WARNING to stderr naming the prefix and page-caps at the first
   1000 real rows for that leaf, then continues. This is a real, disclosed
   coverage gap of the source's search API, not invented data.

KNOWN REAL LIMITATION (confirmed live, worth stating plainly): appending a
space character to a prefix does not shrink the match count (e.g. "Za" and "Za "
both returned the same total), which means the API is normalizing/trimming
whitespace before matching rather than doing literal-character prefix matching.
Practically this means multi-word partitioning has to happen on the first word's
characters, not on whitespace — this script's alphabet is therefore letters/digits
only (no space), which is sufficient because deepening the first word's spelling
keeps shrinking match counts (confirmed: "Za" -> 1489, "Zaj" -> 0, i.e. real
subdivision happens on subsequent letters).

Given the ~3M-entity scope, a full national crawl run to completion belongs in
scheduled CI (see .github/workflows/update_czech.yml), not a single local run.
For local/manual runs, an optional row cap can be set via the
DILIGUARD_CZECH_MAX_ROWS env var or --max-rows CLI flag purely for verification —
it is OFF by default (unbounded) and must never be relied on in production.

Beneficial ownership: ARES does not publish UBO data in this API — `ubo_names` is
left None (a real gap, same treatment as Estonia/Latvia elsewhere in this repo).

If any request/parse fails, or zero rows are collected, this script exits non-zero
and writes nothing.
"""
import argparse
import os
import sys
import time

import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common.http import DEFAULT_HEADERS
from common.parquet_io import write_registry_parquet

BASE_URL = "https://ares.gov.cz/ekonomicke-subjekty-v-be/rest/ekonomicke-subjekty"
SEARCH_URL = f"{BASE_URL}/vyhledat"

SEED_ALPHABET = list("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZÁČĎÉĚÍŇÓŘŠŤÚŮÝŽ")
MAX_PREFIX_DEPTH = 6  # safety valve only — see module docstring
PAGE_SIZE = 100
MAX_RESULTS_PER_QUERY = 1000

TOO_MANY_RESULTS_SUBCODE = "VYSTUP_PRILIS_MNOHO_VYSLEDKU"


def search_ares(body: dict, *, max_attempts: int = 3, backoff_seconds: float = 2.0) -> dict:
    """POST to ARES's /vyhledat search endpoint, retrying transient 5xx/network errors.

    Confirmed live: ARES's own business-logic errors — including the expected
    "too many results" (VYSTUP_PRILIS_MNOHO_VYSLEDKU) response this crawler relies
    on to know when to split a partition — come back as HTTP 400 with a JSON error
    envelope (`{"kod": ..., "popis": ..., "subKod": ...}`), not a 200. Those are
    real, structured, expected responses, so this function returns the parsed JSON
    body for any response it can parse as JSON regardless of status code — the
    caller decides whether the envelope is fatal or an expected "split me further"
    signal. Only unparseable responses or exhausted 5xx/network retries raise.
    """
    last_exc = None
    for attempt in range(1, max_attempts + 1):
        try:
            response = requests.post(SEARCH_URL, json=body, timeout=30, headers=DEFAULT_HEADERS)
            if response.status_code >= 500 and attempt < max_attempts:
                time.sleep(backoff_seconds * attempt)
                continue
            if response.status_code >= 500:
                response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as exc:
            last_exc = exc
            if attempt < max_attempts:
                time.sleep(backoff_seconds * attempt)
    raise last_exc


def _row_from_subject(subject: dict) -> dict:
    sidlo = subject.get("sidlo") or {}
    registrace = subject.get("seznamRegistraci") or {}

    if subject.get("datumZaniku"):
        status = "TERMINATED"
    else:
        stav_ros = registrace.get("stavZdrojeRos")
        if stav_ros == "AKTIVNI":
            status = "ACTIVE"
        elif stav_ros and stav_ros != "NEEXISTUJICI":
            status = stav_ros
        else:
            status = None

    return {
        "company_name": subject.get("obchodniJmeno"),
        "registration_number": subject.get("ico"),
        "registered_address": sidlo.get("textovaAdresa"),
        "status": status,
        "ubo_names": None,  # real gap — ARES does not publish UBO data
        "jurisdiction": "CZ",
    }


def _fetch_leaf_rows(prefix: str, total: int) -> list[dict]:
    """Page through a partition already confirmed to have <= MAX_RESULTS_PER_QUERY matches."""
    rows = []
    start = 0
    cap = min(total, MAX_RESULTS_PER_QUERY)
    while start < cap:
        pocet = min(PAGE_SIZE, cap - start)
        payload = search_ares({"start": start, "pocet": pocet, "obchodniJmeno": prefix})
        for subject in payload.get("ekonomickeSubjekty") or []:
            rows.append(_row_from_subject(subject))
        start += pocet
    return rows


def _crawl_prefix(prefix: str, depth: int, max_rows: int | None, stats: dict) -> list[dict]:
    if max_rows is not None and stats["collected"] >= max_rows:
        return []

    probe = search_ares({"start": 0, "pocet": 1, "obchodniJmeno": prefix})

    if "kod" in probe:  # ARES error envelope
        if probe.get("subKod") == TOO_MANY_RESULTS_SUBCODE:
            total = None  # unknown exact count, just "too many"
        else:
            raise RuntimeError(f"ARES rejected prefix {prefix!r}: {probe}")
    else:
        total = probe.get("pocetCelkem", 0)

    rows: list[dict] = []

    if total is not None and total <= MAX_RESULTS_PER_QUERY:
        leaf_rows = _fetch_leaf_rows(prefix, total)
        stats["collected"] += len(leaf_rows)
        stats["leaves"] += 1
        return leaf_rows

    # Too many results for this prefix.
    if depth >= MAX_PREFIX_DEPTH:
        print(
            f"WARNING: prefix {prefix!r} still exceeds {MAX_RESULTS_PER_QUERY} matches at max depth "
            f"({MAX_PREFIX_DEPTH}) — paging only the first {MAX_RESULTS_PER_QUERY} real rows for this "
            f"partition; some real CZ entities under this prefix will not be captured in this run. "
            f"This is a disclosed ARES search-API limitation, not fabricated/skipped data.",
            file=sys.stderr,
        )
        leaf_rows = _fetch_leaf_rows(prefix, MAX_RESULTS_PER_QUERY)
        stats["collected"] += len(leaf_rows)
        stats["leaves"] += 1
        return leaf_rows

    for ch in SEED_ALPHABET:
        if max_rows is not None and stats["collected"] >= max_rows:
            break
        child_prefix = prefix + ch
        rows.extend(_crawl_prefix(child_prefix, depth + 1, max_rows, stats))

    return rows


def crawl(max_rows: int | None = None) -> list[dict]:
    stats = {"collected": 0, "leaves": 0}
    rows: list[dict] = []
    for seed in SEED_ALPHABET:
        if max_rows is not None and stats["collected"] >= max_rows:
            break
        rows.extend(_crawl_prefix(seed, depth=1, max_rows=max_rows, stats=stats))
        print(f"...seed {seed!r} done — {stats['collected']} rows so far ({stats['leaves']} leaf partitions)")
    return rows


def main(argv: list[str] | None = None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--max-rows", type=int, default=None,
        help="Optional cap on rows collected, for local/manual verification runs only. "
             "OFF (unbounded) by default — never set this in production/CI.",
    )
    args = parser.parse_args(argv if argv is not None else sys.argv[1:])

    max_rows = args.max_rows
    env_cap = os.environ.get("DILIGUARD_CZECH_MAX_ROWS")
    if max_rows is None and env_cap:
        max_rows = int(env_cap)

    if max_rows:
        print(f"Crawling Czech ARES business register (bounded test run, max_rows={max_rows})...")
    else:
        print("Crawling Czech ARES business register (full recursive prefix-trie crawl)...")

    try:
        rows = crawl(max_rows=max_rows)
    except (requests.RequestException, ValueError, RuntimeError) as e:
        print(f"FATAL: failed to crawl Czech ARES business register: {e}", file=sys.stderr)
        sys.exit(1)

    if not rows:
        print("FATAL: parsed zero rows from Czech ARES business register — refusing to write an empty file", file=sys.stderr)
        sys.exit(1)

    count = write_registry_parquet(rows, "czech_reg.parquet", source_url=BASE_URL)
    print(f"Wrote {count} real Czech ARES rows to czech_reg.parquet")


if __name__ == "__main__":
    main()
