"""Single source of truth for the registry-row schema written by every update_*.py script.

Every script must build rows as dicts with (at least) the REQUIRED_FIELDS keys, then
pass them through `finalize_rows` before writing to Parquet. This guarantees every
output file has the same column set/order and carries provenance metadata, instead of
each script re-typing its own column list.
"""
from datetime import datetime, timezone

# Columns every row must be able to provide (missing values are allowed to be None,
# but never a fabricated placeholder value).
REQUIRED_FIELDS = [
    "company_name",
    "registration_number",
    "registered_address",
    "status",
    "ubo_names",
    "jurisdiction",
]

# Provenance columns stamped onto every row so downstream consumers (and CI checks)
# can tell real ingested data apart from anything else.
PROVENANCE_FIELDS = [
    "source_url",
    "fetched_at",
    "pipeline_version",
]

PIPELINE_VERSION = "2026.08.03"

# Sentinel substrings that must never appear in shipped data. Used by the CI lint step
# and by each script's own sanity check before writing output.
#
# These are deliberately the exact annotation markers the old hardcoded-fallback code
# used to flag its own fabricated rows (e.g. "SAP SE" / "HRB 719915 (MOCK)"), not real
# company names or bare words like "MOCK"/"SAMPLE" — those collide with genuine data
# (e.g. the real Latvian company `SIA "MOCKBA WASHINGTON"`, or "Société Générale",
# which is a real bank that could legitimately appear in a real dataset).
KNOWN_FAKE_SENTINELS = [
    "(MOCK)",
    "(INFERRED)",
    "GMBH (INFERRED)",
    "HRB 123456",
]


def finalize_rows(rows: list[dict], source_url: str) -> list[dict]:
    """Fill in required/provenance columns with a consistent shape for every row.

    Does not fabricate business data: missing REQUIRED_FIELDS values are left as
    None (a real "we don't have this field from this source" signal), never a
    placeholder string.
    """
    fetched_at = datetime.now(timezone.utc).isoformat()
    finalized = []
    for row in rows:
        out = {field: row.get(field) for field in REQUIRED_FIELDS}
        out["source_url"] = source_url
        out["fetched_at"] = fetched_at
        out["pipeline_version"] = PIPELINE_VERSION
        finalized.append(out)
    return finalized
