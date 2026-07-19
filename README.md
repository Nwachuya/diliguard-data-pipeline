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
