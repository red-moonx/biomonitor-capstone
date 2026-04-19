# 🌿 BioMonitor: Technical Implementation Report

## 1. Infrastructure as Code (Terraform)
I implemented a robust cloud foundation using **Terraform** to ensure reproducible and version-controlled infrastructure. 
*   **Provisioned Resources:** A Google Cloud Storage (GCS) bucket (`biomonitor-data-lake-decapstone`) acting as our Data Lake and a BigQuery dataset (`biomonitor_data`) for our Data Warehouse.
*   **IAM Management:** Configured a dedicated Service Account (`biomonitor-runner`) with granular permissions (BigQuery Admin & Storage Admin) to handle automated runs without exposing personal credentials.
*   **Portability:** Execution was containerized using Docker to avoid environment-specific dependency issues.

## 2. Data Ingestion (dlt)
For the ingestion layer, I used **dlt (Data Load Tool)** to build a scalable pipeline from the GBIF API.
*   **Pagination:** While the API defaults to 300 records per request, I implemented a custom Python generator with a pagination loop (offset-based) to reach a target of **10,000 global records**.
*   **Architecture (Lake -> Warehouse):** Following data engineering best practices, I configured a **GCS Staging** layer. Data is first landed as Parquet files in the Data Lake before being efficiently loaded into BigQuery, meeting the "End-to-End" criteria of a professional pipeline.

## 3. DBT: Transformation & Modeling
I used **dbt** to transform the raw, messy API data into a clean, analytical schema. I implemented a Staging Layer that selected 20 core columns from the 100+ provided by the API.

### Selected Columns for Biodiversity Analysis:
| Column | Description | Transformation Logic |
| :--- | :--- | :--- |
| `gbif_id` | Unique Record ID | Cast to String for downstream compatibility |
| `species` | Scientific name | Filtered for completeness |
| `latitude / longitude` | Geo-coordinates | Cast to Float64 for mapping |
| `occurrence_timestamp` | Event date/time | Parsed from ISO 8601 intervals |
| `species_order` | Taxonomic Order | Renamed to avoid SQL reserved keyword conflicts |
| `iucn_red_list_category`| Conservation Status | Retained for ecological hotspot analysis |
### 3.1 Staging Transformations
In the Staging layer, I implemented the following critical transformations to clean and standardize the data before further modeling:

*   **Type Casting:** Ensured all fields are optimized for BigQuery (e.g., coordinates to `FLOAT64`, timestamps for event dates).
*   **Field Renaming:** Renamed columns like `order` to `species_order` to avoid SQL reserved keyword conflicts and improve readability.
*   **Critical Data Filtering:** Excluded records missing Latitude/Longitude or Species names to ensure the integrity of spatial biodiversity analysis.
*   **Data Deduplication:** Implemented `ROW_NUMBER()` window functions to handle duplicate `gbif_id` records from the API, preventing duplicate counts in the analytics layer.

### 3.2 Data Quality & Validation
To ensure the integrity of the analytical layer, I implemented dbt's built-in testing suite (`unique`, `not_null`) across primary identifiers and mandatory geographical fields.

**Quality Findings & Resolutions:**
*   **Deduplication:** Identified records with duplicate `gbif_id`. Resolved by implementing a `ROW_NUMBER()` window function to retain the most recent entry.
*   **Schema Enforcement:** Found records missing essential species names or country codes. Implemented strict filtering in the staging layer to maintain 100% data completeness for downstream analytics.
*   **Temporal Integrity:** Handled "Invalid Timestamp" errors caused by ISO date intervals. Developed a resilient parsing strategy using `SAFE_CAST` and `SPLIT` to standardize event dates into a compatible BigQuery TIMESTAMP format.

**Result:** All transformations and quality tests have passed successfully, yielding a robust and production-ready staging layer.

## 4. Orchestration (Airflow)
*Planned/Current:* Using Apache Airflow to orchestrate the full cycle: triggering the `dlt` ingestion script followed by the `dbt build` command to ensure the pipeline is hands-off and reliable.

## 5. Visualization (Dashboard)
*Current State:* The cleaned tables are connected to a dashboard to visualize:
*   Global distribution of mammals.
*   Identification of biodiversity hotspots based on IUCN status.
*   Temporal trends in species registration.
