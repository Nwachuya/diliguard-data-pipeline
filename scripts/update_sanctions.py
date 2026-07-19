import requests
import duckdb
import os
import shutil

INDEX_URL = "https://data.opensanctions.org/datasets/latest/default/index.json"

def download_file(url, local_filename):
    print(f"Downloading {url} to {local_filename}...")
    # Stream the download to avoid blowing up memory
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, 'wb') as f:
            shutil.copyfileobj(r.raw, f)
    print(f"Downloaded {local_filename}")

def main():
    print("Fetching OpenSanctions index...")
    response = requests.get(INDEX_URL)
    response.raise_for_status()
    data = response.json()
    
    # Extract URLs for the specific files we want
    for resource in data.get("resources", []):
        if resource.get("name") == "targets.simple.csv":
            csv_url = resource.get("url")
            
    if not csv_url:
        raise ValueError("Could not find targets.simple.csv URL in index.json")

    # 1. Download simplified CSV for quick screening
    download_file(csv_url, "targets.simple.csv")
    
    # 3. Convert CSV to Parquet using DuckDB
    print("Converting targets.simple.csv to Parquet format...")
    # Configure DuckDB to stay within GitHub Actions 7GB memory limit and spool to disk
    duckdb.sql("SET memory_limit='4GB';")
    duckdb.sql("SET temp_directory='tmp.duckdb';")
    
    query = """
    COPY (
        SELECT * FROM read_csv_auto('targets.simple.csv', all_varchar=True)
    ) TO 'targets.simple.parquet' (FORMAT PARQUET);
    """
    duckdb.sql(query)
    print("Conversion successful. Output: targets.simple.parquet")

    # Cleanup the raw CSV to save disk space before Wrangler upload
    os.remove("targets.simple.csv")
    print("Cleaned up temporary CSV file.")

if __name__ == "__main__":
    main()
