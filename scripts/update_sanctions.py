import requests
import duckdb
import os
import shutil

INDEX_URL = "https://data.opensanctions.org/datasets/latest/default/index.json"

def download_file(url, local_filename):
    print(f"Downloading {url} to {local_filename}...")
    # Stream the download using iter_content so requests automatically decompresses GZIP encoding
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192 * 1024): 
                if chunk:
                    f.write(chunk)
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
    
    # 3. Convert CSV to Parquet using Polars
    print("Converting targets.simple.csv to Parquet format using Polars...")
    import polars as pl
    
    # Read the CSV entirely into memory (the 500MB file will comfortably fit in the 7GB runner RAM) and write to Parquet
    df = pl.read_csv('targets.simple.csv', ignore_errors=True, infer_schema_length=0)
    df.write_parquet('targets.simple.parquet')
    
    print("Conversion successful. Output: targets.simple.parquet")

    # Cleanup the raw CSV to save disk space before Wrangler upload
    os.remove("targets.simple.csv")
    print("Cleaned up temporary CSV file.")

if __name__ == "__main__":
    main()
