"""Real ingestion of French company registry data (no auth required).

The INPI RNE bulk export itself requires an SFTP account ("Mes accès API/SFTP") we
don't have — it is not anonymously automatable, confirming what this script's old
comment already suspected. The real, free, no-auth alternative is INSEE's Base
Sirene (data.gouv.fr), which is the authoritative open dataset for French company
identity/status and is refreshed continuously. We use its "StockUniteLegale" file,
already published as Parquet, and filter to administratively active legal units.

Sirene does not publish beneficial-owner data, so `ubo_names` is left as None (a
real gap) and `registered_address` is left as None (address lives in the separate,
much larger StockEtablissement file, out of scope for this pass) — neither is
fabricated.

If the remote source is unreachable or yields zero rows, this script exits non-zero
and writes nothing.
"""
import os
import sys

import duckdb
import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common.schema import PIPELINE_VERSION, KNOWN_FAKE_SENTINELS

SIRENE_DATASET_API = (
    "https://www.data.gouv.fr/api/1/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret/"
)
OUTPUT_PATH = "france_rne.parquet"


def get_latest_sirene_parquet_url() -> str:
    """Fetch the latest active StockUniteLegale Parquet URL dynamically from data.gouv.fr API."""
    print("Resolving latest active Sirene StockUniteLegale Parquet URL from data.gouv.fr API...")
    r = requests.get(SIRENE_DATASET_API, timeout=30)
    r.raise_for_status()
    data = r.json()
    for res in data.get("resources", []):
        url = res.get("url", "")
        fmt = (res.get("format") or "").lower()
        if fmt == "parquet" or url.endswith(".parquet"):
            if "stockunitelegale" in url.lower() and "historique" not in url.lower():
                print(f"Discovered active Sirene Parquet URL: {url}")
                return url
    raise ValueError("Could not find StockUniteLegale parquet resource in data.gouv.fr dataset API")


def main():
    sirene_url = get_latest_sirene_parquet_url()
    print("Querying INSEE Base Sirene (StockUniteLegale) for active French legal units...")
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")

    query = f"""
        COPY (
            SELECT
                COALESCE(denominationUniteLegale, concat_ws(' ', prenom1UniteLegale, nomUniteLegale)) AS company_name,
                siren AS registration_number,
                CAST(NULL AS VARCHAR) AS registered_address,
                CASE etatAdministratifUniteLegale
                    WHEN 'A' THEN 'ACTIVE'
                    WHEN 'C' THEN 'CEASED'
                    ELSE etatAdministratifUniteLegale
                END AS status,
                CAST(NULL AS VARCHAR) AS ubo_names,
                'FR' AS jurisdiction,
                '{sirene_url}' AS source_url,
                strftime(now(), '%Y-%m-%dT%H:%M:%S+00:00') AS fetched_at,
                '{PIPELINE_VERSION}' AS pipeline_version
            FROM read_parquet('{sirene_url}')
            WHERE etatAdministratifUniteLegale = 'A'
              AND (denominationUniteLegale IS NOT NULL OR nomUniteLegale IS NOT NULL)
        ) TO '{OUTPUT_PATH}' (FORMAT PARQUET);
    """
    try:
        con.execute(query)
    except Exception as e:
        print(f"FATAL: failed to query/convert France Sirene data: {e}", file=sys.stderr)
        sys.exit(1)

    row_count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{OUTPUT_PATH}')").fetchone()[0]
    if row_count == 0:
        print("FATAL: query against Sirene returned zero rows — refusing to keep an empty file", file=sys.stderr)
        os.remove(OUTPUT_PATH)
        sys.exit(1)

    sentinel_filter = " OR ".join(
        f"company_name ILIKE '%{s}%'" for s in KNOWN_FAKE_SENTINELS
    )
    fake_hits = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{OUTPUT_PATH}') WHERE {sentinel_filter}"
    ).fetchone()[0]
    if fake_hits > 0:
        print(f"FATAL: {fake_hits} row(s) matched a banned fake-data sentinel — refusing to ship", file=sys.stderr)
        os.remove(OUTPUT_PATH)
        sys.exit(1)

    print(f"Wrote {row_count} real France Sirene legal-unit rows to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
