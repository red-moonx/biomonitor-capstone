import dlt
import requests

def get_gbif_data(target_limit=10000):
    """Request global Mammalia data from GBIF's API using pagination."""
    url = "https://api.gbif.org/v1/occurrence/search"
    offset = 0
    records_fetched = 0
    page_size = 300 # Max allowed by GBIF per request

    print(f"Starting ingestion for {target_limit} records...")

    while records_fetched < target_limit:
        # Calculate how many to ask for in this page
        current_limit = min(page_size, target_limit - records_fetched)
        
        params = {
            "classKey": 359,  # Mammalia
            "limit": current_limit,
            "offset": offset,
            "occurrenceStatus": "PRESENT"
        }

        response = requests.get(url, params=params)
        response.raise_for_status()
        
        data = response.json()
        results = data.get("results", [])
        
        if not results:
            break
            
        for record in results:
            yield record
            records_fetched += 1
            
        offset += len(results)
        print(f"Progress: {records_fetched}/{target_limit} records fetched...")

def run_pipeline():
    """Configures and runs the dlt pipeline with GCS staging (Data Lake)"""
    
    # Using filesystem destination for GCS staging
    pipeline = dlt.pipeline(
        pipeline_name="mammal_monitor_global",
        destination="bigquery",
        staging="filesystem",
        dataset_name="biomonitor_data"
    )

    # Launch ingestion into the 'raw_mammals' table
    # 'replace' ensures we start fresh and delete any old data from previous tests
    info = pipeline.run(
        get_gbif_data(), 
        table_name="raw_mammals", 
        write_disposition="replace"
    )
    
    print(info)

if __name__ == "__main__":
    run_pipeline()
