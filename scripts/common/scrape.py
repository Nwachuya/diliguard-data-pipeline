"""Shared helper for country scripts that must scrape an HTML page instead of hitting
a bulk file or a documented JSON/OData API (see http.py for that case).

Same philosophy as common/http.py: an honest User-Agent that identifies this
pipeline (never spoof a browser to get around a site's anti-bot posture), retry on
transient 5xx, and propagate exceptions rather than swallowing them so a script's
own FATAL/exit-1 handling stays in control.

The critical addition for HTML scraping specifically: a scraped source has no
contract at all (no versioned schema, no changelog) — a redesign can silently
turn "0 results" (a real, legitimate answer) into "we parsed a wrapper div that no
longer contains what we think it contains" (a fake, undetectable answer). Every
script built on this helper MUST call `assert_page_shape(...)` (or an equivalent
explicit structural check of its own) on a fetched page *before* parsing rows out
of it, and treat a failed check as fatal:

    FATAL: page structure for <country> did not match expected shape — refusing to
    parse, source may have changed

never as "zero rows" — that distinction is the whole point of this module.
"""
import sys
import time

import requests
from bs4 import BeautifulSoup

DEFAULT_TIMEOUT = 30
DEFAULT_HEADERS = {
    "User-Agent": "DiliguardDataPipeline/1.0 (+https://github.com/diliguard/diliguard-data-pipeline)"
}


class PageShapeError(RuntimeError):
    """Raised when a scraped page's structure doesn't match what the parser expects.

    Distinct from "the source returned zero real results" — this means the page
    itself looks unlike what the script was built against (missing form fields,
    missing result table/columns, an error/CAPTCHA interstitial, etc.), so we
    cannot trust any rows we might otherwise be able to scrape out of it.
    """


def get_with_retry(url: str, *, timeout: int = DEFAULT_TIMEOUT, max_attempts: int = 3,
                    backoff_seconds: float = 2.0, session: requests.Session | None = None,
                    **kwargs) -> requests.Response:
    """GET an HTML page, retrying on transient 5xx/network errors. Raises on final failure.

    Mirrors common.http.get_with_retry's contract exactly; kept as a separate
    function (rather than importing that one) so scrape-specific concerns —
    session/cookie reuse across a paginated crawl, an HTML-flavoured default
    Accept header — can evolve independently of the bulk-file/API helper.

    Also retries on HTTP 429 (confirmed live against Bulgaria's Commercial
    Register search endpoint, which enforces a real, undocumented short-burst
    rate limit with no `Retry-After` header — a handful of rapid requests is
    enough to trigger it, and it clears within roughly a minute or two). When a
    `Retry-After` header IS present, it's honoured; otherwise this backs off
    with a longer, growing delay than the 5xx case, since 429 recovery in
    practice takes longer than a transient 5xx blip.
    """
    getter = session.get if session is not None else requests.get
    last_exc = None
    for attempt in range(1, max_attempts + 1):
        try:
            headers = {"Accept": "text/html,application/xhtml+xml", **DEFAULT_HEADERS, **kwargs.pop("headers", {})}
            response = getter(url, timeout=timeout, headers=headers, **kwargs)
            if response.status_code == 429 and attempt < max_attempts:
                retry_after = response.headers.get("Retry-After")
                delay = float(retry_after) if retry_after and retry_after.isdigit() else backoff_seconds * attempt * 5
                time.sleep(delay)
                continue
            if response.status_code >= 500 and attempt < max_attempts:
                time.sleep(backoff_seconds * attempt)
                continue
            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            last_exc = exc
            if attempt < max_attempts:
                time.sleep(backoff_seconds * attempt)
    raise last_exc


def post_with_retry(url: str, *, data: dict, timeout: int = DEFAULT_TIMEOUT, max_attempts: int = 3,
                     backoff_seconds: float = 2.0, session: requests.Session | None = None,
                     **kwargs) -> requests.Response:
    """POST an HTML form, retrying on transient 5xx/network errors. Raises on final failure."""
    poster = session.post if session is not None else requests.post
    last_exc = None
    for attempt in range(1, max_attempts + 1):
        try:
            headers = {"Accept": "text/html,application/xhtml+xml", **DEFAULT_HEADERS, **kwargs.pop("headers", {})}
            response = poster(url, data=data, timeout=timeout, headers=headers, **kwargs)
            if response.status_code >= 500 and attempt < max_attempts:
                time.sleep(backoff_seconds * attempt)
                continue
            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            last_exc = exc
            if attempt < max_attempts:
                time.sleep(backoff_seconds * attempt)
    raise last_exc


def parse_html(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "lxml")


def assert_page_shape(soup: BeautifulSoup, *, country: str, checks: list[tuple[str, bool]]) -> None:
    """Verify a scraped page looks like what the parser was built against.

    `checks` is a list of (description, passed) tuples — e.g.
        assert_page_shape(soup, country="Cyprus", checks=[
            ("result grid present", soup.find("table", id="ctl00_cphMyMasterCentral_GridView1") is not None),
        ])

    Raises PageShapeError (never returns a silent partial result) the moment any
    check fails, so a source redesign is loud instead of looking like "0 results".
    """
    for description, passed in checks:
        if not passed:
            raise PageShapeError(
                f"page structure for {country} did not match expected shape "
                f"({description} — failed) — refusing to parse, source may have changed"
            )


def fatal_on_shape_or_network_error(country: str, exc: Exception) -> None:
    """Standard FATAL message + exit(1) for the two ways a scrape script must fail closed:
    the network/HTTP layer breaking, or the page shape no longer matching expectations.
    """
    if isinstance(exc, PageShapeError):
        print(f"FATAL: {exc}", file=sys.stderr)
    else:
        print(f"FATAL: failed to fetch/parse {country} source: {exc}", file=sys.stderr)
    sys.exit(1)
