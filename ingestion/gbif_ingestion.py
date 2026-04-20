import dlt
import requests
import time
import os
import zipfile
import pandas as pd
from dotenv import load_dotenv

# Load secrets
load_dotenv()

GBIF_USER = os.getenv("GBIF_USER")
GBIF_PASSWORD = os.getenv("GBIF_PASSWORD")
GBIF_EMAIL = os.getenv("GBIF_EMAIL")

def trigger_download():
    """Starts the bulk download request for Cetaceans (733)."""
    url = "https://api.gbif.org/v1/occurrence/download/request"
    
    payload = {
        "creator": GBIF_USER,
        "notificationAddresses": [GBIF_EMAIL],
        "sendNotification": True,
        "format": "SIMPLE_CSV",
        "predicate": {
            "type": "and",
            "predicates": [
                {"type": "equals", "key": "TAXON_KEY", "value": "733"},
                {"type": "greaterThanOrEquals", "key": "YEAR", "value": "2021"},
                {"type": "lessThanOrEquals", "key": "YEAR", "value": "2026"},
                {"type": "equals", "key": "OCCURRENCE_STATUS", "value": "PRESENT"}
            ]
        }
    }
    
    print(f"🚀 Requesting Bulk Download for CETACEANS from GBIF...")
    response = requests.post(
        url, 
        json=payload, 
        auth=(GBIF_USER, GBIF_PASSWORD),
        headers={"Content-Type": "application/json"}
    )
    
    if response.status_code != 201:
        print(f"❌ Error triggering download: {response.status_code} - {response.text}")
        response.raise_for_status()
        
    download_key = response.text
    print(f"✅ Download requested! Key: {download_key}")
    return download_key

def wait_for_download(download_key):
    """Polls the GBIF API until the download is ready."""
    url = f"https://api.gbif.org/v1/occurrence/download/{download_key}"
    
    print(f"⏳ Waiting for GBIF to prepare the Cetacean records...")
    while True:
        response = requests.get(url, auth=(GBIF_USER, GBIF_PASSWORD))
        data = response.json()
        status = data.get("status")
        
        if status == "SUCCEEDED":
            print(f"\n🏁 Download is READY!")
            return data.get("downloadLink")
        elif status in ["FAILED", "CANCELLED"]:
            raise Exception(f"❌ GBIF Download {status}!")
        else:
            print(f"   [STATUS] {status}... (Checking again in 30s)")
            time.sleep(30)

def download_and_load(download_link):
    """Downloads ZIP, extracts CSV and streams to BigQuery."""
    zip_path = "gbif_cetaceans.zip"
    csv_filename = "occurrence.txt"
    
    print(f"📥 Downloading file from {download_link}...")
    with requests.get(download_link, stream=True) as r:
        r.raise_for_status()
        with open(zip_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    
    print(f"📦 Streaming Cetacean data to BigQuery...")
    
    def csv_generator():
        with zipfile.ZipFile(zip_path) as z:
            # Find the CSV file dynamically
            csv_files = [f for f in z.namelist() if f.endswith('.csv')]
            if not csv_files:
                raise Exception("❌ No CSV file found in the ZIP archive!")
            
            target_csv = csv_files[0]
            print(f"   [ZIP] Extracting: {target_csv}")
            
            with z.open(target_csv) as f:
                # SIMPLE_CSV is tab-separated
                chunks = pd.read_csv(f, sep='\t', chunksize=50000, low_memory=False)
                for i, chunk in enumerate(chunks):
                    print(f"   [DLT] Processing chunk {i+1} (approx { (i+1)*50000 } records)...")
                    # Clean column names for BigQuery compatibility
                    chunk.columns = [c.replace(" ", "_").lower() for c in chunk.columns]
                    yield chunk.to_dict(orient="records")

    pipeline = dlt.pipeline(
        pipeline_name="gbif_cetaceans_ingestion",
        destination="bigquery",
        dataset_name="biomonitor_raw"
    )
    
    load_info = pipeline.run(csv_generator(), table_name="raw_mammals", write_disposition="replace")
    print(load_info)
    
    if os.path.exists(zip_path):
        os.remove(zip_path)

if __name__ == "__main__":
    if not all([GBIF_USER, GBIF_PASSWORD, GBIF_EMAIL]):
        print("❌ Error: Missing credentials in .env file!")
    else:
        key = trigger_download()
        link = wait_for_download(key)
        download_and_load(link)
