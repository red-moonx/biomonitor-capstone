import dlt
import requests
import time
from datetime import datetime

def get_gbif_data_month_by_month(start_year=2015, end_year=2026, limit_per_month=10000):
    """
    Request global Mammalia data from GBIF's API iterating through YEARS and MONTHS.
    This ensures a balanced seasonal dataset and bypasses the 100k offset limit.
    """
    url = "https://api.gbif.org/v1/occurrence/search"
    page_size = 300 
    global_records_fetched = 0

    print(f"Starting SCIENTIFIC ingestion (balanced by months) from {start_year} to {end_year}...")

    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            # Skip future months if we are in the current year
            if year == datetime.now().year and month > datetime.now().month:
                break
                
            offset = 0
            month_records_fetched = 0
            
            print(f">>> Fetching: {year}-{month:02d} (Target: {limit_per_month} records)")
            
            while month_records_fetched < limit_per_month:
                params = {
                    "taxonKey": 359,  # Mammalia (standard recursive key)
                    "year": year,
                    "month": month,
                    "limit": page_size,
                    "offset": offset,
                    "occurrenceStatus": "PRESENT"
                }

                try:
                    response = requests.get(url, params=params, timeout=30)
                    print(f"   [DEBUG] Calling: {response.url} | Status: {response.status_code}")
                    response.raise_for_status()
                    time.sleep(1) # Rate limiting protection
                except Exception as e:
                    print(f"   Error in {year}-{month} at offset {offset}: {e}")
                    break
                
                data = response.json()
                results = data.get("results", [])
                total_in_query = data.get("count", 0)
                
                if offset == 0:
                    print(f"   (API reports {total_in_query} total available for this month)")

                if not results:
                    break
                    
                for record in results:
                    yield record
                    month_records_fetched += 1
                    global_records_fetched += 1
                    
                offset += len(results)
                
                # Check GBIF safety limit
                if offset >= 100000:
                    break

            print(f"   Done {year}-{month:02d}: {month_records_fetched} records. (Total: {global_records_fetched})")

    print(f"\n--- Ingestion Finished. Total balanced records: {global_records_fetched} ---")

def run_pipeline():
    """Configures and runs the dlt pipeline with GCS staging (Data Lake)"""
    
    current_year = datetime.now().year
    start_year = current_year - 11 
    
    pipeline = dlt.pipeline(
        pipeline_name="mammal_monitor_global",
        destination="bigquery",
        staging="filesystem",
        dataset_name="biomonitor_data"
    )

    # We use limit_per_month=10000 to get a solid 1M+ dataset across 10 years
    info = pipeline.run(
        get_gbif_data_month_by_month(start_year=start_year, end_year=current_year, limit_per_month=10000), 
        table_name="raw_mammals", 
        write_disposition="replace"
    )
    
    print(info)

if __name__ == "__main__":
    run_pipeline()
