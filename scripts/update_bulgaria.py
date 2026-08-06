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

REAL FULL-CRAWL APPROACH (recursive company-name-prefix partitioning)
----------------------------------------------------------------------
A prior version of this script only searched two hardcoded example name
prefixes ("СОФАРМА","КРИБ") and shipped that to production as if it were
Bulgaria's registry — it was not; it was ~20 rows matching two test company
names. That was a critical defect (the same class of bug as a mock-data
fallback) and is fixed here: the crawl is now seeded from the real 30-letter
Bulgarian Cyrillic alphabet plus digits 0-9 (company names can start with
either), and partitions recursively the same way `update_czech.py` partitions
Czech ARES's name-prefix search:

1. For each prefix, first probe the endpoint with `pageSize=1` and read the
   real total-match count the API returns in its `count` response header
   (confirmed live, e.g. `name=Б` -> `count: 3538`; this is the same role
   ARES's `pocetCelkem` plays for Czech).
2. If that total fits within `MAX_RESULTS_PER_PREFIX` (`PAGE_SIZE *
   MAX_PAGES_PER_PREFIX`), page through the prefix fully with `page`/`pageSize`
   and collect every real row.
3. If the total exceeds that cap, split the prefix into children by appending
   each alphabet character/digit to it, and recurse into each child — exactly
   like Czech's trie-partition crawl.
4. `MAX_PREFIX_DEPTH` is a safety valve purely to guarantee termination against
   pathological cases. If a leaf still exceeds the cap at max depth, this
   script does NOT fabricate/skip silently — it logs a clear WARNING naming the
   prefix and pages only the first `MAX_RESULTS_PER_PREFIX` real rows for that
   leaf, then continues. That is a real, disclosed coverage gap, not invented
   data.

CONFIRMED REAL CONSTRAINT — aggressive, undocumented rate limiting
-------------------------------------------------------------------
Checked live while building this crawl: the endpoint returns HTTP 429 after as
few as 5-10 requests in quick succession, with no `Retry-After` header, and
recovery took roughly 60-120 seconds in manual testing. `common/scrape.py`'s
`get_with_retry` now retries 429s with a longer backoff to make forward
progress possible, but this means a genuinely complete crawl of the whole
Cyrillic alphabet (30 letters, each very likely needing multiple levels of
subdivision — a single common letter alone can have 5,000-20,000+ matches) is
a slow, many-hours (plausibly much longer) operation, not something that
completes in a short interactive run. This is a real property of the source,
not a bug in this script, and belongs in scheduled CI (see
.github/workflows/update_bulgaria.yml) the same way Czech's ~3M-entity crawl
does — a single local/manual run should use `--max-rows`/`BULGARIA_MAX_ROWS`
or a narrowed `--seed-alphabet` to verify behaviour, not to claim full coverage.

The free summary endpoint only returns a name and a UIC/EIK (registration
number) per entity — no address, no status, no beneficial owners. Those three
required-schema fields are therefore left as None (a real "this free source
doesn't have it" gap), never invented from a second, unverified endpoint.

If the endpoint is unreachable, returns an unexpected shape, or a run yields zero
new rows with nothing previously accumulated either, this script exits non-zero
and writes nothing.

CHECKPOINT/RESUME ACROSS SCHEDULED RUNS
----------------------------------------
Because of the rate-limit constraint above, one scheduled run cannot reliably
finish a full 40-seed-prefix crawl inside GitHub Actions' 6-hour job timeout —
and without any memory of progress, a run that gets cut off partway through the
alphabet would make next week's run start over from "А" every time, so the
crawl could plausibly never progress past the first few letters. To fix that,
this script persists a small state file, `bulgaria_crawl_state.json`:

    {"completed_prefixes": ["А", "0", ...], "last_run": "<iso8601>", "cycle": 1}

On each run: load the state file if present, skip any top-level seed prefix
already marked completed, and crawl through the remaining ones IN ORDER until
either (a) `BULGARIA_CRAWL_TIME_BUDGET_SECONDS` of wall-clock time has elapsed
this run (default 5 hours — chosen to leave roughly a 1-hour buffer under GitHub
Actions' 6-hour job ceiling for checkout/pip-install/validate/upload steps), or
(b) `BULGARIA_MAX_PREFIXES_PER_RUN` seed prefixes have been attempted this run
(unset/unbounded by default in production — this knob exists mainly so a local
or test run can deterministically bound itself without waiting on a real clock).
A fixed prefix-count budget isn't used as the *primary* gate in production
because the observed rate-limit behaviour (HTTP 429 after 5-10 rapid requests,
60-120s recovery, no Retry-After) means the real cost per seed prefix varies by
well over an order of magnitude — a rare digit prefix might resolve in a
handful of requests, while a common letter (confirmed live: single letters with
3,500+ matches) can recurse several levels deep and take many multiples of
that. A time budget adapts to that variance automatically; a fixed count of
"crawl N letters per run" would either strand a slow letter mid-run or leave a
fast run under-using its time budget on quiet nights.

Once every seed prefix in the alphabet is marked completed, the *next* run
detects a fully-completed pass, resets `completed_prefixes` to empty, and
increments `cycle` — starting a fresh full pass rather than freezing forever,
since the underlying registry data changes over time and stale coverage from
months ago should eventually be refreshed.

ACCUMULATING OUTPUT ACROSS RUNS WITHOUT LOSING PRIOR PROGRESS
-----------------------------------------------------------------
`bulgaria_reg.parquet` is uploaded to R2 under one fixed, overwritten key (the
convention every script in this repo follows — see README). If each run only
crawled a handful of new seed prefixes and then wrote just *that* run's rows to
the output path, the overwrite would silently discard every previously-crawled
prefix's real data. That would be worse than doing nothing.

This script avoids that by reading the previous `bulgaria_reg.parquet` *from
disk* (if present) before writing the new one, and merging: the CI workflow
downloads the last run's `bulgaria_reg.parquet` and `bulgaria_crawl_state.json`
from R2 before invoking this script (mirroring the existing "download bulk
file, process, upload" shape every other workflow already uses — no new
storage concept, just an extra pull at the start and an extra push at the end),
so a local run without network access to R2 simply sees no previous file and
starts a fresh accumulation, which is also the correct behaviour for a from-
scratch clone. Previously-written rows keep their original `fetched_at`
provenance (never re-stamped to "now" just because they were merged) — only
genuinely newly-crawled rows get a fresh `fetched_at`. Rows are de-duplicated by
`registration_number` (preferring the newest copy when the same real entity
reappears, e.g. after a wrap-around re-crawl), and rows with no registration
number (should not happen given the shape check on every page, but is not
impossible) are kept as-is rather than being incorrectly collapsed together.

FAIL-CLOSED BEHAVIOUR
------------------------
Nothing is written or persisted — not the state file, not the output Parquet —
unless this run's entire crawl (of whichever prefixes it attempted this run)
completes without raising. A network failure or page-shape change partway
through aborts the whole run via the existing `fatal_on_shape_or_network_error`
path, exits non-zero, and leaves both the previous state file and the previous
`bulgaria_reg.parquet` completely untouched on disk — so a single bad run can
never corrupt or roll back previously-accumulated real data. The prefixes that
failed simply remain "not completed" and are retried (from the top of the
pending list) on the next run.
"""
import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from urllib.parse import quote

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common.scrape import get_with_retry, PageShapeError, fatal_on_shape_or_network_error
from common.parquet_io import write_finalized_rows_parquet
from common.schema import finalize_rows

BASE_URL = "https://portal.registryagency.bg/CR/api/Deeds/Summary"
OUTPUT_PATH = "bulgaria_reg.parquet"
STATE_PATH = "bulgaria_crawl_state.json"

# Real Bulgarian Cyrillic alphabet (30 letters) plus digits — company names can
# start with either (confirmed live: purely-numeric-prefixed legal names exist,
# e.g. cooperatives/associations named after a founding year or address number).
SEED_ALPHABET = list("АБВГДЕЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЬЮЯ0123456789")

PAGE_SIZE = 25
MAX_PAGES_PER_PREFIX = int(os.environ.get("BULGARIA_MAX_PAGES_PER_PREFIX", "40"))
MAX_RESULTS_PER_PREFIX = PAGE_SIZE * MAX_PAGES_PER_PREFIX
MAX_PREFIX_DEPTH = int(os.environ.get("BULGARIA_MAX_PREFIX_DEPTH", "4"))  # safety valve only — see docstring

# Checkpoint/resume knobs — see docstring "CHECKPOINT/RESUME ACROSS SCHEDULED RUNS".
# 5 hours leaves ~1 hour of buffer under GitHub Actions' 6-hour job timeout for
# checkout/pip-install/validate/upload.
CRAWL_TIME_BUDGET_SECONDS = int(os.environ.get("BULGARIA_CRAWL_TIME_BUDGET_SECONDS", str(5 * 3600)))
_max_prefixes_env = os.environ.get("BULGARIA_MAX_PREFIXES_PER_RUN")
MAX_PREFIXES_PER_RUN = int(_max_prefixes_env) if _max_prefixes_env else None


def _search_url(prefix: str, page: int, page_size: int) -> str:
    return (
        f"{BASE_URL}?page={page}&pageSize={page_size}&count=0"
        f"&name={quote(prefix)}&selectedSearchFilter=1&includeHistory=true"
    )


# Confirmed live: this endpoint enforces an aggressive, undocumented short-burst
# rate limit (HTTP 429 after as few as 5-10 rapid requests, no Retry-After header,
# ~60-120s observed recovery). A recursive crawl makes far more than 5-10 requests,
# so both probe and page fetches use generous retry/backoff budgets rather than the
# 3-attempt/short-backoff defaults meant for occasional transient 5xx blips.
_RATE_LIMIT_KWARGS = {"max_attempts": 6, "backoff_seconds": 5.0}


def _probe_total(prefix: str) -> int:
    """Cheap `pageSize=1` request whose `count` response header is the real total
    match count for this prefix (confirmed live) — the Bulgarian-search analogue
    of ARES's `pocetCelkem` probe in update_czech.py."""
    response = get_with_retry(_search_url(prefix, page=1, page_size=1), timeout=30, **_RATE_LIMIT_KWARGS)
    try:
        return int(response.headers.get("count", "0"))
    except (TypeError, ValueError):
        raise PageShapeError(
            "page structure for Bulgaria did not match expected shape "
            "('count' response header missing/non-numeric) — refusing to parse, source may have changed"
        )


def _fetch_page(prefix: str, page: int, page_size: int = PAGE_SIZE) -> list[dict]:
    response = get_with_retry(_search_url(prefix, page, page_size), timeout=30, **_RATE_LIMIT_KWARGS)
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


def _fetch_leaf_rows(prefix: str, total: int) -> list[dict]:
    """Page through a partition already confirmed to have <= MAX_RESULTS_PER_PREFIX matches."""
    rows: list[dict] = []
    cap = min(total, MAX_RESULTS_PER_PREFIX)
    page = 1
    while (page - 1) * PAGE_SIZE < cap:
        items = _fetch_page(prefix, page)
        if not items:
            break
        rows.extend(_rows_from_items(items))
        page += 1
    return rows


def _crawl_prefix(prefix: str, depth: int, max_rows: int | None, stats: dict) -> list[dict]:
    if max_rows is not None and stats["collected"] >= max_rows:
        return []

    total = _probe_total(prefix)

    if total == 0:
        stats["leaves"] += 1
        return []

    if total <= MAX_RESULTS_PER_PREFIX:
        leaf_rows = _fetch_leaf_rows(prefix, total)
        stats["collected"] += len(leaf_rows)
        stats["leaves"] += 1
        return leaf_rows

    if depth >= MAX_PREFIX_DEPTH:
        print(
            f"WARNING: prefix {prefix!r} still has an estimated {total} matches at max depth "
            f"({MAX_PREFIX_DEPTH}) — paging only the first {MAX_RESULTS_PER_PREFIX} real rows for this "
            f"partition; some real BG entities under this prefix will not be captured in this run. "
            f"This is a disclosed coverage limitation of this crawl's depth cap, not fabricated/skipped data.",
            file=sys.stderr,
        )
        leaf_rows = _fetch_leaf_rows(prefix, MAX_RESULTS_PER_PREFIX)
        stats["collected"] += len(leaf_rows)
        stats["leaves"] += 1
        return leaf_rows

    rows: list[dict] = []
    for ch in SEED_ALPHABET:
        if max_rows is not None and stats["collected"] >= max_rows:
            break
        rows.extend(_crawl_prefix(prefix + ch, depth + 1, max_rows, stats))
    return rows


def crawl(seed_alphabet: list[str], max_rows: int | None, stats: dict) -> list[dict]:
    """Crawl every prefix in `seed_alphabet` unconditionally (no checkpoint gating).

    Kept as-is for anything that wants an unbounded, single-shot crawl of an
    explicit list of seeds (e.g. a local `--seed-alphabet` smoke test). Production
    runs go through `crawl_pending`, which adds checkpoint/resume gating on top of
    this same per-seed crawl + within-run de-dup logic.
    """
    rows, _completed = crawl_pending(seed_alphabet, max_rows=max_rows, stats=stats,
                                      time_budget_seconds=None, max_prefixes_this_run=None)
    return rows


def crawl_pending(pending_seeds: list[str], max_rows: int | None, stats: dict,
                   time_budget_seconds: float | None, max_prefixes_this_run: int | None) -> tuple[list[dict], list[str]]:
    """Crawl seed prefixes from `pending_seeds` in order, stopping early once either
    `time_budget_seconds` of wall-clock time has elapsed this run or
    `max_prefixes_this_run` seeds have been attempted (whichever comes first; either
    may be None to disable that gate) — see docstring "CHECKPOINT/RESUME ACROSS
    SCHEDULED RUNS". Returns (rows_from_seeds_attempted_this_run, seeds_completed_this_run).

    A seed is only ever added to the returned "completed" list once `_crawl_prefix`
    returns for it without raising — a real total of 0 matches is a legitimate,
    completed result; an exception is not caught here at all, so it propagates and
    the caller's checkpoint state is left exactly as it was before this call (see
    "FAIL-CLOSED BEHAVIOUR" in the module docstring).
    """
    rows: list[dict] = []
    seen_idents: set[str] = set()
    completed: list[str] = []
    start = time.monotonic()
    for i, seed in enumerate(pending_seeds):
        if max_rows is not None and stats["collected"] >= max_rows:
            break
        if max_prefixes_this_run is not None and i >= max_prefixes_this_run:
            print(f"Reached BULGARIA_MAX_PREFIXES_PER_RUN ({max_prefixes_this_run}) for this run — "
                  f"stopping; {len(pending_seeds) - i} pending seed prefix(es) remain for next run.")
            break
        if time_budget_seconds is not None and (time.monotonic() - start) > time_budget_seconds:
            print(f"Reached crawl time budget ({time_budget_seconds:.0f}s) for this run — "
                  f"stopping; {len(pending_seeds) - i} pending seed prefix(es) remain for next run.")
            break

        seed_rows = _crawl_prefix(seed, depth=1, max_rows=max_rows, stats=stats)
        before = len(rows)
        for row in seed_rows:
            ident = row["registration_number"]
            if ident and ident in seen_idents:
                continue
            if ident:
                seen_idents.add(ident)
            rows.append(row)
        completed.append(seed)
        print(f"...seed {seed!r} done — {len(rows) - before} new rows ({len(rows)} total, {stats['leaves']} leaf partitions)")
    return rows, completed


def _load_state() -> dict:
    """Load the checkpoint state file, defaulting to a fresh/empty state if it's
    missing or unreadable. A corrupt state file is treated as "no progress yet"
    rather than a fatal error — worst case this re-crawls some already-completed
    prefixes once (wasted requests, not wrong data), which is preferable to a
    checkpoint bug permanently wedging the crawl."""
    if not os.path.exists(STATE_PATH):
        return {"completed_prefixes": [], "last_run": None, "cycle": 1}
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            state = json.load(f)
        state.setdefault("completed_prefixes", [])
        state.setdefault("last_run", None)
        state.setdefault("cycle", 1)
        return state
    except (OSError, ValueError) as e:
        print(f"WARNING: could not read {STATE_PATH} ({e}) — treating as a fresh crawl cycle", file=sys.stderr)
        return {"completed_prefixes": [], "last_run": None, "cycle": 1}


def _save_state(state: dict) -> None:
    """Write the checkpoint state file atomically (write to a temp file, then
    rename) so a crash mid-write can never leave a half-written, corrupt state
    file on disk."""
    tmp_path = f"{STATE_PATH}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=False, sort_keys=True)
    os.replace(tmp_path, STATE_PATH)


def _load_previous_finalized_rows(output_path: str) -> list[dict]:
    """Read a previously-written `bulgaria_reg.parquet` (if present on disk) back
    into a list of already-finalized row dicts, so this run can merge its newly-
    crawled rows on top without losing prior runs' accumulated coverage. Returns
    [] if no previous output file exists (first-ever run, or a local clone with no
    R2 download step) — that is a legitimate "nothing accumulated yet" state, not
    an error."""
    if not os.path.exists(output_path):
        return []
    import polars as pl
    df = pl.read_parquet(output_path)
    return df.to_dicts()


def _merge_rows(new_finalized_rows: list[dict], previous_finalized_rows: list[dict]) -> list[dict]:
    """Merge this run's freshly-finalized rows with previously-accumulated rows,
    de-duplicating by `registration_number` and preferring the NEW copy when the
    same real entity appears in both (e.g. a wrap-around re-crawl refreshing a
    prefix that was already covered in an earlier cycle). Rows with no
    registration number are kept as-is (never collapsed against each other) since
    there's no reliable key to de-duplicate them by."""
    merged: list[dict] = []
    seen_idents: set[str] = set()
    for row in new_finalized_rows:
        ident = row.get("registration_number")
        if ident:
            seen_idents.add(ident)
        merged.append(row)
    for row in previous_finalized_rows:
        ident = row.get("registration_number")
        if ident and ident in seen_idents:
            continue  # a newer copy of this same real entity was already crawled this run
        merged.append(row)
    return merged


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
             "verification runs only (e.g. a couple of rare letters to keep a run short). "
             "Unset by default, which uses the full real Bulgarian alphabet + digits.",
    )
    args = parser.parse_args(argv if argv is not None else sys.argv[1:])

    max_rows = args.max_rows
    env_cap = os.environ.get("BULGARIA_MAX_ROWS")
    if max_rows is None and env_cap:
        max_rows = int(env_cap)

    seed_alphabet = SEED_ALPHABET
    if args.seed_alphabet:
        seed_alphabet = [c.strip() for c in args.seed_alphabet.split(",") if c.strip()]
    elif os.environ.get("BULGARIA_SEED_ALPHABET"):
        seed_alphabet = [c.strip() for c in os.environ["BULGARIA_SEED_ALPHABET"].split(",") if c.strip()]

    if max_rows or seed_alphabet is not SEED_ALPHABET:
        print(f"Crawling Bulgaria Commercial Register (bounded test run: seeds={seed_alphabet}, max_rows={max_rows})...")
    else:
        print("Crawling Bulgaria Commercial Register (full recursive name-prefix crawl, checkpointed)...")

    # --- checkpoint/resume: figure out which of this run's seed prefixes are
    # still pending, resetting to a fresh cycle if the previous run finished the
    # whole alphabet — see docstring "CHECKPOINT/RESUME ACROSS SCHEDULED RUNS".
    state = _load_state()
    completed_prefixes = set(state["completed_prefixes"])
    cycle = state["cycle"]
    if seed_alphabet and completed_prefixes.issuperset(seed_alphabet):
        print(f"Cycle {cycle} fully completed ({len(seed_alphabet)}/{len(seed_alphabet)} seed prefixes) — "
              f"starting a fresh crawl cycle {cycle + 1}.")
        completed_prefixes = set()
        cycle += 1
    pending_seeds = [s for s in seed_alphabet if s not in completed_prefixes]
    if not pending_seeds:
        # seed_alphabet was empty, or (shouldn't happen given the reset above) already all done.
        print("No pending seed prefixes to crawl this run.")
        pending_seeds = []
    else:
        print(f"Cycle {cycle}: {len(completed_prefixes)}/{len(seed_alphabet)} seed prefixes already completed; "
              f"{len(pending_seeds)} pending this cycle. Attempting pending prefixes in order this run, "
              f"gated by a {CRAWL_TIME_BUDGET_SECONDS:.0f}s time budget"
              + (f" and a {MAX_PREFIXES_PER_RUN}-prefix cap" if MAX_PREFIXES_PER_RUN is not None else "") + ".")

    stats = {"collected": 0, "leaves": 0}
    try:
        new_rows, newly_completed = crawl_pending(
            pending_seeds, max_rows=max_rows, stats=stats,
            time_budget_seconds=CRAWL_TIME_BUDGET_SECONDS,
            max_prefixes_this_run=MAX_PREFIXES_PER_RUN,
        )
    except Exception as e:
        fatal_on_shape_or_network_error("Bulgaria", e)
        return  # unreachable — fatal_on_shape_or_network_error always exits

    previous_rows = _load_previous_finalized_rows(OUTPUT_PATH)

    if not new_rows and not previous_rows:
        print("FATAL: parsed zero rows from Bulgaria Commercial Register search and no previously "
              "accumulated data exists — refusing to write an empty file", file=sys.stderr)
        sys.exit(1)
    if not new_rows:
        print(f"WARNING: this run's crawled prefix(es) yielded zero new rows; keeping "
              f"{len(previous_rows)} previously accumulated row(s) unchanged.")

    finalized_new_rows = finalize_rows(new_rows, source_url=BASE_URL)
    merged_rows = _merge_rows(finalized_new_rows, previous_rows)
    count = write_finalized_rows_parquet(merged_rows, OUTPUT_PATH)
    print(f"Wrote {count} real Bulgaria Commercial Register rows to {OUTPUT_PATH} "
          f"({len(finalized_new_rows)} newly crawled this run, {count - len(finalized_new_rows)} carried "
          f"over from previous runs).")

    # Only persist checkpoint progress after a fully successful crawl+write this run —
    # see "FAIL-CLOSED BEHAVIOUR" in the module docstring.
    state["completed_prefixes"] = sorted(completed_prefixes | set(newly_completed))
    state["cycle"] = cycle
    state["last_run"] = datetime.now(timezone.utc).isoformat()
    _save_state(state)
    print(f"Checkpoint saved: {len(state['completed_prefixes'])}/{len(seed_alphabet)} seed prefixes "
          f"completed in cycle {cycle} ({STATE_PATH}).")


if __name__ == "__main__":
    main()
