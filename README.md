# Diliguard Data Pipeline

This standalone repository hosts the automated data ingestion, cleaning, and processing pipelines that fuel the Diliguard OSINT and intelligence engine.

## Overview

The primary function of this pipeline is to fetch global sanctions and watchlists, compress and optimize the data, and securely store it in our Cloudflare R2 storage bucket so the main Diliguard Backend (`diliguard-trace`) can query it rapidly using DuckDB.

## Workflows

### 1. OpenSanctions Daily Update (`update_sanctions.yml`)
Runs automatically every day at **2:00 AM UTC** (or manually via GitHub Actions workflow dispatch).

**What it does:**
1. **Fetches the OpenSanctions Index**: Resolves the latest URLs for the dataset.
2. **Downloads Raw Data**: 
   - `targets.simple.csv` (for quick screening)
   - `entities.ftm.json` (FollowTheMoney format for deep diligence traces)
3. **Converts to Parquet**:
   - Uses **Polars** to quickly convert the CSV into `targets.simple.parquet`.
   - Uses **DuckDB** to extract the `id` and `json` from the FollowTheMoney dataset and export it as `entities.ftm.parquet`.
4. **Uploads to Cloudflare R2**: Uses the AWS CLI to sync the optimized `.parquet` files to our Cloudflare R2 bucket (`diliguard-sanctions-test`).

### 2. Estonia e-Business Register Update (`update_estonia.yml`)
Runs daily at **5:00 AM UTC**. Downloads RIK's real open-data bulk files (no auth required):
- `ettevotja_rekvisiidid__lihtandmed.csv.zip` — company basics (name, reg. code, status, address).
- `ettevotja_rekvisiidid__kasusaajad.json.zip` — real beneficial-owner (UBO) records, joined in by registration code.

Uploads `estonia_reg.parquet`. Exits non-zero and uploads nothing if the real source is unreachable or unparsable — it never falls back to placeholder data.

### 3. Latvia Enterprise Register (UR) Update (`update_latvia_ur.yml`)
Runs daily at **4:00 AM UTC**. Downloads the real open-data CSV resource from `data.gov.lv`'s "Uzņēmumu reģistrs" dataset. Latvia's open dataset does not publish UBO data, so `ubo_names` is genuinely `null` rather than fabricated. Uploads `latvia_ur.parquet`.

### 4. France Company Registry Update (`update_france_rne.yml`)
Runs monthly, on the **3rd at 3:00 AM UTC** — INSEE republishes the Sirene stock files around the 1st of each month, so daily runs would just re-fetch the same 700MB file with nothing new. The INPI RNE bulk export requires an SFTP account we don't have, so this uses INSEE's Base Sirene (data.gouv.fr) — the real, free, no-auth open dataset for French company identity/status — queried directly via DuckDB's remote-Parquet support, filtered to administratively active legal units. `registered_address` and `ubo_names` are `null` (Sirene doesn't carry those fields in this table) rather than fabricated. Uploads `france_rne.parquet`.

### 5. Slovakia RPVS Update (`update_slovakia_rpvs.yml`)
Runs weekly, every **Monday at 6:00 AM UTC**. RPVS is a live register with no fixed publish cadence (unlike the dated bulk files above), so this is a judgment call: weekly balances freshness against not hammering a government OData API with a ~2,500-page crawl every single day. Crawls the Ministry of Justice's real, free, no-auth OData v4 API (`rpvs.gov.sk/opendatav2`, CC0 licensed) for the full ~53k-partner Register of Public Sector Partners, joining in real beneficial-owner ("konečný užívateľ výhod") records — this is one of the few sources here that genuinely publishes UBO data, not a gap. Uploads `slovakia_rpvs.parquet`.

Every non-sanctions script above shares `scripts/common/` (schema, HTTP retry helper, Parquet writer with a fake-data sentinel guard) and is followed in its workflow by a `Validate output before upload` step (`scripts/common/validate_output.py`) that re-checks row count and sentinel-freeness before the R2 upload runs.

### 6. UK Companies House Update (`update_uk_companies_house.yml`)
Runs monthly, on the **7th at 7:00 AM UTC** — Companies House publishes this snapshot within 5 working days of month-end, so the 7th gives it a safety margin. Downloads Companies House's real, free, no-signup "Free Company Data Product" bulk CSV (`download.companieshouse.gov.uk`) — resolving the current dated filename from the index page rather than hardcoding it. ~5.7M companies. This snapshot doesn't include PSC/UBO data (that's a separate Companies House product, already used live via the API in `diliguard-trace`), so `ubo_names` is `null` here rather than fabricated. Uploads `uk_companies_house.parquet`.

### Cadence and storage strategy

Every workflow's cron cadence is matched to how often its real source actually republishes data (checked via each source's own metadata, not guessed): daily for OpenSanctions/Estonia/Latvia (their sources update daily), monthly for France/UK (their sources publish monthly stock files), weekly for Slovakia (a live register with no fixed cadence, crawled considerately rather than daily). Storage follows the same convention as the original `update_sanctions.yml`: every run overwrites the same fixed R2 key per country — no dated/versioned snapshots. `workflow_dispatch` is enabled on every workflow for on-demand manual runs regardless of cron.

### Known gap: Denmark (CVR)

Confirmed genuinely blocked, not just under-researched. Every real bulk path requires an account:
- System-to-system CVR access: free, open to anyone, but requires emailing `cvrselvbetjening@erst.dk` and waiting for a username/password.
- Datafordeler (Denmark's national data distributor): requires either a MitID Erhverv account (Danish business digital ID) or the same Erhvervsstyrelsen email request, with OAuth on top.

There is no anonymous, instant, self-service bulk path — so no `update_denmark_cvr.py` exists yet rather than shipping a script that would either always fail or fabricate data. Once Diliguard has real CVR system-to-system credentials (the free email-request route above), this can be built following the same `scripts/common/` pattern as the other scripts.

## Testing

Run the test suite (fixture-based parsing tests, fail-closed-on-network-failure tests, and a source-wide scan for banned fake-data sentinel strings):
```bash
pip install -r requirements.txt
python -m pytest tests/ -v
```

## Local Development

If you need to test the pipeline locally:

1. Create a virtual environment and install the dependencies:
   ```bash
   python -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

2. Run the update script directly:
   ```bash
   python scripts/update_sanctions.py
   ```

*(Note: The script will download large files and output `.parquet` files into your current directory. It cleans up the raw `.csv` and `.json` files automatically to save space.)*

## Secrets Configuration

For the GitHub Actions workflow to successfully upload to Cloudflare R2, the following repository secrets must be configured in GitHub:

- `R2_ACCOUNT_ID`: Your Cloudflare Account ID.
- `R2_ACCESS_KEY_ID`: Your R2 API Token Access Key.
- `R2_SECRET_ACCESS_KEY`: Your R2 API Token Secret Key.
