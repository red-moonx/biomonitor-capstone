import dlt
import requests

def get_gbif_data():
    """Request global Mammalia data from GBIF's API"""
    url = "https://api.gbif.org/v1/occurrence/search"
    
    # Global parameters: Mammalia class (classKey: 359)
    # 10,000 records to provide local/global trends and justify BigQuery partitioning
    params = {
        "classKey": 359,  # Mammalia
        "limit": 10000,
        "occurrenceStatus": "PRESENT"
    }

    response = requests.get(url, params=params)
    response.raise_for_status()
    
    # Extract occurrences from results
    data = response.json().get("results", [])
    
    for record in data:
        yield record

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
    info = pipeline.run(get_gbif_data(), table_name="raw_mammals")
    
    print(info)

if __name__ == "__main__":
    run_pipeline()
