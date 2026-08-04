"""Real ingestion of KVK's (Dutch Chamber of Commerce) Open Dataset "Basic Company
Information" (Basis Bedrijfsgegevens) — free, no signup, no API key required.

RESEARCH FINDING (see report for full detail): the KVK "subscription" APIs (Zoeken,
Basisprofiel, Vestigingsprofiel, Naamgeving — the ones with company name/address) are
NOT self-serve. https://developers.kvk.nl/apply-for-apis states verbatim: "Without a
KVK number you cannot request access. Exceptions to this are foreign governments from
the EEA." That requires an existing Dutch KVK registration to even apply, plus signing
a subscription agreement — genuinely blocked for us.

The KVK Open Dataset ("HVDS" / High Value Data Set) is a *separate*, genuinely public
product. It is available two ways, both unauthenticated:
  1. A single-lookup API at https://opendata.kvk.nl/api/v1/hvds/basisbedrijfsgegevens/kvknummer/{kvkNummer}
     (verified live, no apikey header sent, got structured JSON business-logic errors
     and eventual 429s — not 401/403 — confirming no auth wall). Rate limit is ~1
     request/minute per the docs, which makes it useless for bulk ingestion since we
     have no list of valid KVK numbers to iterate.
  2. A daily bulk CSV download at
     https://www.kvk.nl/download/kvk-open-dataset-basis-bedrijfsgegevens.zip
     ("You can use the Open Dataset Basic Company Information for free" — no
     registration, updated every working day). This is the practical ingestion path
     and what this script uses.

CRITICAL SCHEMA CAVEAT — this is a bigger gap than "no name/address":
The HVDS bulk file (and even the single-record API's OUTPUT fields) contain NO
identifier at all — no KVK number, no company name, no address, nothing that
distinguishes one row from another except its (often duplicated) attribute values.
KVK does this deliberately for GDPR/re-identification reasons (see
https://developers.kvk.nl/documentation/open-dataset-basis-bedrijfsgegevens-api :
"the available dataset does not contain all data fields described in Implementing
Regulation 2023/138... the KVK number... cannot be regarded as personal data in all
cases... KVK does not make this data available as part of the HVDS"). Each row here
is real anonymised statistical microdata about one BV/NV registration, but it is NOT
"keyed by KVK number" the way the task brief assumed — `registration_number` is
genuinely None for every row, not a fabricated placeholder. This makes the dataset
useful for aggregate stats/benchmarking but NOT for per-company KYC lookups by
registration number.

Fields provided (Dutch CSV headers -> meaning):
  Datum aanvang     -> registration start date (used only for context, not schema)
  Actief             -> J/N active indicator
  Insolventie        -> FAIL (bankruptcy) / SSAN (debt restructuring) / SURS
                        (suspension of payments) / empty
  Rechtsvorm         -> BV or NV only (no other legal forms, no sole proprietorships)
  Postcode regio     -> first two digits of the visiting-address postcode
  SBI activiteiten   -> comma-separated SBI activity codes
  Hoofdactiviteiten  -> main SBI activity code
  Lidstaat           -> always NL

If the download or parse fails, or the parsed dataset is empty, this script exits
non-zero and writes nothing.
"""
import os
import sys
import zipfile
from datetime import datetime, timezone

import duckdb

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common.http import download_file
from common.schema import KNOWN_FAKE_SENTINELS, PIPELINE_VERSION

ZIP_URL = "https://www.kvk.nl/download/kvk-open-dataset-basis-bedrijfsgegevens.zip"
ZIP_LOCAL_PATH = "netherlands_hvds.zip"
CSV_LOCAL_PATH = "netherlands_hvds.csv"
OUTPUT_PATH = "netherlands_reg.parquet"


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _sql_str(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def main():
    print(f"Downloading KVK Open Dataset bulk file from {ZIP_URL} ...", flush=True)
    try:
        download_file(ZIP_URL, ZIP_LOCAL_PATH, timeout=180)
    except Exception as e:
        print(f"FATAL: failed to download KVK Open Dataset bulk file: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        print("Extracting CSV to disk...", flush=True)
        with zipfile.ZipFile(ZIP_LOCAL_PATH) as z:
            member = z.namelist()[0]
            with z.open(member) as src, open(CSV_LOCAL_PATH, "wb") as dst:
                for chunk in iter(lambda: src.read(8 * 1024 * 1024), b""):
                    dst.write(chunk)
    except Exception as e:
        print(f"FATAL: failed to extract KVK Open Dataset bulk file: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if os.path.exists(ZIP_LOCAL_PATH):
            os.remove(ZIP_LOCAL_PATH)

    try:
        con = duckdb.connect()
        con.execute("SET memory_limit='4GB';")
        con.execute("SET temp_directory='tmp_duckdb_nl';")

        described = con.execute(
            f"DESCRIBE SELECT * FROM read_csv_auto({_sql_str(CSV_LOCAL_PATH)}, "
            f"all_varchar=true, header=true, delim=';')"
        ).fetchall()
        columns = {row[0].strip(): row[0] for row in described}

        required = ["Actief", "Insolventie", "Rechtsvorm", "Postcode regio"]
        missing = [c for c in required if c not in columns]
        if missing:
            raise RuntimeError(f"KVK Open Dataset CSV is missing expected columns: {missing}")

        def col(stripped_name: str) -> str:
            return _quote_ident(columns[stripped_name])

        fetched_at = datetime.now(timezone.utc).isoformat()

        print("Converting CSV to Parquet with DuckDB (streaming)...", flush=True)
        con.execute(f"""
            COPY (
                SELECT
                    CAST(NULL AS VARCHAR) AS company_name,
                    CAST(NULL AS VARCHAR) AS registration_number,
                    CASE
                        WHEN nullif(trim({col('Postcode regio')}), '') IS NULL THEN NULL
                        ELSE 'postcode area: ' || trim({col('Postcode regio')})
                    END AS registered_address,
                    CASE
                        WHEN nullif(trim({col('Insolventie')}), '') IS NOT NULL THEN trim({col('Insolventie')})
                        WHEN trim({col('Actief')}) = 'J' THEN 'ACTIVE'
                        WHEN trim({col('Actief')}) = 'N' THEN 'INACTIVE'
                        ELSE NULL
                    END AS status,
                    CAST(NULL AS VARCHAR) AS ubo_names,
                    'NL' AS jurisdiction,
                    {_sql_str(ZIP_URL)} AS source_url,
                    {_sql_str(fetched_at)} AS fetched_at,
                    {_sql_str(PIPELINE_VERSION)} AS pipeline_version
                FROM read_csv_auto({_sql_str(CSV_LOCAL_PATH)}, all_varchar=true, header=true, delim=';')
            ) TO {_sql_str(OUTPUT_PATH)} (FORMAT PARQUET);
        """)

        row_count = con.execute(
            f"SELECT COUNT(*) FROM read_parquet({_sql_str(OUTPUT_PATH)})"
        ).fetchone()[0]
        if row_count == 0:
            raise RuntimeError("parsed zero rows — refusing to keep an empty file")

        sentinel_clause = " OR ".join(
            f"{c} ILIKE {_sql_str('%' + s + '%')}"
            for c in ("company_name", "registration_number", "registered_address", "status")
            for s in KNOWN_FAKE_SENTINELS
        )
        fake_hits = con.execute(
            f"SELECT COUNT(*) FROM read_parquet({_sql_str(OUTPUT_PATH)}) WHERE {sentinel_clause}"
        ).fetchone()[0]
        if fake_hits > 0:
            raise RuntimeError(f"{fake_hits} row(s) matched a banned fake-data sentinel")
    except Exception as e:
        print(f"FATAL: failed to parse KVK Open Dataset bulk file: {e}", file=sys.stderr)
        if os.path.exists(OUTPUT_PATH):
            os.remove(OUTPUT_PATH)
        sys.exit(1)
    finally:
        if os.path.exists(CSV_LOCAL_PATH):
            os.remove(CSV_LOCAL_PATH)

    print(f"Wrote {row_count} real KVK Open Dataset rows (NL, BV/NV only, no per-row "
          f"identifier by design) to {OUTPUT_PATH}", flush=True)


if __name__ == "__main__":
    main()
