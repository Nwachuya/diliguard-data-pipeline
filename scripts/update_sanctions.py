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
    csv_url = None
    json_url = None
    
    for resource in data.get("resources", []):
        if resource.get("name") == "targets.simple.csv":
            csv_url = resource.get("url")
        elif resource.get("name") == "entities.ftm.json":
            json_url = resource.get("url")
            
    if not csv_url or not json_url:
        raise ValueError("Could not find required resource URLs in index.json")

    # 1. Download simplified CSV for quick screening
    download_file(csv_url, "targets.simple.csv")
    
    # 2. Download the rich FTM JSON for AI/Graphs
    download_file(json_url, "entities.ftm.json")
    
    # 3. Convert CSV to Parquet using DuckDB
    print("Converting targets.simple.csv to Parquet format...")
    # DuckDB will seamlessly read the CSV into memory in optimized chunks and write out a highly compressed Parquet file
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
