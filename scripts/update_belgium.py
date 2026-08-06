"""Real ingestion of Belgium's KBO/CBE (Kruispuntbank van Ondernemingen / Crossroads
Bank for Enterprises) Open Data bulk export.

Source: https://economie.fgov.be/en/themes/enterprises/crossroads-bank-enterprises/services-everyone/cbe-open-data
Portal: https://kbopub.economie.fgov.be/kbo-open-data (free, but requires a registered
account — unlike every other script in this pipeline).

*** BLOCKED: this script is UNTESTED. No KBO Open Data account exists yet. ***
Obi must sign up for a free account at
https://kbopub.economie.fgov.be/kbo-open-data/signup?form and accept the terms of use
before this script can run for real. Do not treat a clean exit of this script as proof
it works end-to-end until that has happened and it has been run against the live site.

Verified by hand (2026-08-04, unauthenticated):
  - The login form at https://kbopub.economie.fgov.be/kbo-open-data/login?lang=en is a
    plain Spring Security form: POST to
    /kbo-open-data/static/j_spring_security_check with fields `j_username` and
    `j_password` (confirmed from the real page HTML — <input name="j_username">,
    <input name="j_password">, <form method="POST" action="...">).
  - Any open-data page (e.g. /kbo-open-data/affiliation/xml/files) 302-redirects to
    /kbo-open-data/login when unauthenticated, i.e. there is no anonymous fallback.
  - Everything past the login wall (the exact "list available files" page, and the
    real download URL / filename pattern for the current full export) could NOT be
    verified without a real account, and is NOT guessed at here beyond the documented,
    publicly-known KBO Open Data file structure (a single ZIP containing meta.csv,
    code.csv, enterprise.csv, establishment.csv, branch.csv, activity.csv, address.csv,
    contact.csv, denomination.csv — described in KBO's public "cookbook" PDF).

Credentials are read from KBO_USERNAME / KBO_PASSWORD environment variables (set as
GitHub Actions secrets once an account exists) — never hardcoded. If they are missing,
or the login/download/parse fails, or zero rows are parsed, this script exits non-zero
and writes nothing. It never falls back to hardcoded/sample company records.

KBO Open Data does not include UBO (beneficial owner) data — that is Belgium's
separate UBO register at the Ministry of Finance, out of scope here — so `ubo_names`
is left as None.
"""
import io
import os
import re
import sys
import zipfile

import polars as pl
import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common.http import DEFAULT_HEADERS
from common.parquet_io import write_registry_parquet

LOGIN_PAGE_URL = "https://kbopub.economie.fgov.be/kbo-open-data/login?lang=en"
LOGIN_POST_URL = "https://kbopub.economie.fgov.be/kbo-open-data/static/j_spring_security_check"
# Unverified without an account — best-known real landing page for the "full export"
# file list, per KBO Open Data's documented flow. If this has changed, the script
# fails closed with a clear error rather than guessing at a fallback URL.
FILES_LIST_URL = "https://kbopub.economie.fgov.be/kbo-open-data/affiliation/xml/files"

LOCAL_ZIP = "be_kbo_open_data.zip"

# Address type code for the enterprise's registered office in KBO's address.csv,
# per KBO's published Open Data cookbook.
REGISTERED_OFFICE_TYPE = "REGO"
# Denomination type code for the enterprise's official (legal) name.
LEGAL_NAME_TYPE = "001"


def _login(session: requests.Session, username: str, password: str) -> None:
    session.get(LOGIN_PAGE_URL, headers=DEFAULT_HEADERS, timeout=30)
    response = session.post(
        LOGIN_POST_URL,
        data={"j_username": username, "j_password": password},
        headers=DEFAULT_HEADERS,
        timeout=30,
        allow_redirects=True,
    )
    response.raise_for_status()
    if "j_spring_security_check" in response.url or "login" in response.url.lower():
        raise RuntimeError("KBO login did not succeed — check KBO_USERNAME/KBO_PASSWORD")


def _resolve_full_export_url(session: requests.Session) -> str:
    response = session.get(FILES_LIST_URL, headers=DEFAULT_HEADERS, timeout=30)
    response.raise_for_status()
    match = re.search(r'href="([^"]*KboOpenData_\d+_\w+_Full\.zip)"', response.text)
    if not match:
        raise RuntimeError(
            "Could not find a 'KboOpenData_*_Full.zip' link on the KBO files page — "
            "the page layout may have changed since this script was written"
        )
    url = match.group(1)
    if url.startswith("/"):
        url = "https://kbopub.economie.fgov.be" + url
    return url


def _read_csv_member(z: zipfile.ZipFile, name: str) -> pl.DataFrame:
    with z.open(name) as f:
        return pl.read_csv(f, infer_schema_length=0)


def main():
    username = os.environ.get("KBO_USERNAME")
    password = os.environ.get("KBO_PASSWORD")
    if not username or not password:
        print(
            "FATAL: KBO_USERNAME / KBO_PASSWORD environment variables are not set. "
            "Belgium KBO Open Data requires a free registered account "
            "(https://kbopub.economie.fgov.be/kbo-open-data/signup?form) — "
            "refusing to run without real credentials.",
            file=sys.stderr,
        )
        sys.exit(1)

    session = requests.Session()

    print("Logging in to KBO Open Data portal...")
    try:
        _login(session, username, password)
    except Exception as e:
        print(f"FATAL: failed to log in to KBO Open Data: {e}", file=sys.stderr)
        sys.exit(1)

    print("Resolving current full-export download URL...")
    try:
        export_url = _resolve_full_export_url(session)
    except Exception as e:
        print(f"FATAL: failed to resolve KBO full-export URL: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"Downloading {export_url} ...")
    try:
        with session.get(export_url, headers=DEFAULT_HEADERS, timeout=600, stream=True) as response:
            response.raise_for_status()
            with open(LOCAL_ZIP, "wb") as out:
                for chunk in response.iter_content(chunk_size=8192 * 1024):
                    if chunk:
                        out.write(chunk)
    except Exception as e:
        print(f"FATAL: failed to download KBO Open Data export: {e}", file=sys.stderr)
        if os.path.exists(LOCAL_ZIP):
            os.remove(LOCAL_ZIP)
        sys.exit(1)

    try:
        with zipfile.ZipFile(LOCAL_ZIP) as z:
            members = {os.path.basename(n).lower(): n for n in z.namelist()}
            for required in ("enterprise.csv", "denomination.csv", "address.csv"):
                if required not in members:
                    raise RuntimeError(f"KBO export zip is missing expected member {required!r}")

            enterprise = _read_csv_member(z, members["enterprise.csv"])
            denomination = _read_csv_member(z, members["denomination.csv"])
            address = _read_csv_member(z, members["address.csv"])
    except Exception as e:
        print(f"FATAL: failed to parse KBO Open Data export: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if os.path.exists(LOCAL_ZIP):
            os.remove(LOCAL_ZIP)

    legal_names = {
        r["EntityNumber"]: r["Denomination"]
        for r in denomination.filter(pl.col("TypeOfDenomination") == LEGAL_NAME_TYPE).iter_rows(named=True)
        if r.get("EntityNumber")
    }

    def _format_address(r: dict) -> str | None:
        parts = [
            f"{r.get('StreetNL') or r.get('StreetFR') or ''} {r.get('HouseNumber') or ''}".strip(),
            r.get("Box"),
            r.get("Zipcode"),
            r.get("MunicipalityNL") or r.get("MunicipalityFR"),
            r.get("CountryNL") or r.get("CountryFR"),
        ]
        joined = ", ".join(p for p in parts if p)
        return joined or None

    addresses = {
        r["EntityNumber"]: _format_address(r)
        for r in address.filter(pl.col("TypeOfAddress") == REGISTERED_OFFICE_TYPE).iter_rows(named=True)
        if r.get("EntityNumber")
    }

    rows = []
    for record in enterprise.iter_rows(named=True):
        enterprise_number = record.get("EnterpriseNumber")
        rows.append({
            "company_name": legal_names.get(enterprise_number),
            "registration_number": enterprise_number,
            "registered_address": addresses.get(enterprise_number),
            "status": record.get("Status"),
            "ubo_names": None,  # KBO Open Data does not publish UBO data
            "jurisdiction": "BE",
        })

    if not rows:
        print("FATAL: parsed zero rows from KBO Open Data export — refusing to write an empty file", file=sys.stderr)
        sys.exit(1)

    count = write_registry_parquet(rows, "belgium_reg.parquet", source_url=export_url)
    print(f"Wrote {count} real Belgium KBO rows to belgium_reg.parquet")


if __name__ == "__main__":
    main()
