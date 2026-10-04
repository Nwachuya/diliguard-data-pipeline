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
    csv_url = None
    ftm_url = None
    for resource in data.get("resources", []):
        if resource.get("name") == "targets.simple.csv":
            csv_url = resource.get("url")
        elif resource.get("name") == "entities.ftm.json":
            ftm_url = resource.get("url")
            
    if not csv_url:
        raise ValueError("Could not find targets.simple.csv URL in index.json")

    # 1. Download simplified CSV for quick screening
    download_file(csv_url, "targets.simple.csv")
    
    # 2. Download FollowTheMoney JSON for deep diligence traces
    if ftm_url:
        download_file(ftm_url, "entities.ftm.json")
    
    # 3. Convert CSV to Parquet using Polars
    print("Converting targets.simple.csv to Parquet format using Polars...")
    import polars as pl
    import gc
    
    # Read the CSV entirely into memory and write to Parquet
    df = pl.read_csv('targets.simple.csv', ignore_errors=True, infer_schema_length=0)
    df.write_parquet('targets.simple.parquet')
    del df
    gc.collect()
    
    print("Conversion successful. Output: targets.simple.parquet")

    # Cleanup the raw CSV to save disk space before Wrangler upload
    os.remove("targets.simple.csv")
    
    # 4. Convert FTM JSON to a Key-Value Parquet file
    if ftm_url and os.path.exists("entities.ftm.json"):
        print("Converting entities.ftm.json to a Key-Value Parquet file...")
        spill_dir = "/tmp/duckdb_sanctions_spill"
        os.makedirs(spill_dir, exist_ok=True)
        
        con = duckdb.connect()
        con.execute("PRAGMA threads=2;")
        con.execute("PRAGMA memory_limit='3.5GB';")
        con.execute(f"PRAGMA temp_directory='{spill_dir}';")
        
        query = """
        COPY (
            SELECT 
                json_extract_string(json, '$.id') AS id, 
                json AS data 
            FROM read_json_objects('entities.ftm.json', format='newline_delimited')
        ) TO 'entities.ftm.parquet' (FORMAT PARQUET);
        """
        con.execute(query)
        con.close()
        
        print("FTM JSON conversion successful. Output: entities.ftm.parquet")
        os.remove("entities.ftm.json")
        shutil.rmtree(spill_dir, ignore_errors=True)
        print("Cleaned up temporary FTM JSON and spill files.")

if __name__ == "__main__":
    main()
