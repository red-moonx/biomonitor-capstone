import dlt
import requests

def get_gbif_data():
    """Request data to GBIF's API"""
    url = "https://api.gbif.org/v1/occurrence/search"
    
    # Initial parameters to test (500 ocurrences; mammals in Spain)
    params = {
        "classKey": 359,  # Mammalia
        "country": "ES",
        "limit": 500,
        "occurrenceStatus": "PRESENT"
    }

    response = requests.get(url, params=params)
    response.raise_for_status()
    
    # Extract occurences (in results)
    data = response.json().get("results", [])
    
    for record in data:
        yield record

def run_pipeline():
    """Config and running of dlt pipeline"""
    
    # Create pipeline. 
    # dlt reads the credentials (here, GCS credentials).
    pipeline = dlt.pipeline(
        pipeline_name="gbif_biodiversity",
        destination="bigquery",
        dataset_name="biomonitor_data" # created with Terraform
    )

    # Start loading data!
    info = pipeline.run(get_gbif_data(), table_name="raw_occurrences")
    
    print(info)

if __name__ == "__main__":
    run_pipeline()
