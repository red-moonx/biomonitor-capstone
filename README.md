# 🌿 BioMonitor: Global Biodiversity Tracking & Conservation Pipeline

## 📖 Problem Statement
Biodiversity data is currently being generated at an unprecedented scale from disparate sources: professional research, satellite monitoring, and large-scale citizen science platforms. However, this global data is often **inaccessible** due to complex API structures, **messy** with over 100 columns of inconsistent metadata, and **not optimized** for cost-effective spatial or temporal analysis.

**Global BioMonitor** solves this by providing an automated end-to-end ELT pipeline that:
1.  **Ingests** worldwide species occurrences without geographic restrictions.
2.  **Centralizes** raw data into a scalable Data Lake (GCS).
3.  **Transforms and Optimizes** the data in a BigQuery Data Warehouse using partitioning and clustering.
4.  **Visualizes** trends to help stakeholders identify ecological hotspots and data coverage gaps globally.

## 🏗️ Architecture
The pipeline follows standard data engineering best practices learned during the **Data Engineering Zoomcamp**:

```mermaid
graph TD
    subgraph "Infrastructure"
        TF[Terraform]
    end
    
    subgraph "Data Ingestion"
        API[GBIF API] -->|Extract| AF[Apache Airflow]
        AF -->|Load| GCS[Google Cloud Storage]
    end
    
    subgraph "Data Warehouse"
        GCS -->|Load| BQ[BigQuery]
        BQ -->|Transform| DBT[dbt]
    end
    
    subgraph "Analysis & Visualization"
        DBT -->|Serve| LS[Looker Studio]
    end

    TF -.->|Provision| GCS
    TF -.->|Provision| BQ
```

## 🛠️ Tech Stack
- **Cloud:** Google Cloud Platform (GCP)
- **Infrastructure as Code:** Terraform
- **Workflow Orchestration:** Apache Airflow
- **Data Ingestion:** dlt (Data Load Tool) / Python
- **Data Lake:** Google Cloud Storage (GCS)
- **Data Warehouse:** BigQuery
- **Analytics Engineering:** dbt (data build tool)
- **Visualization:** Looker Studio / Streamlit

## 📈 Dashboard Features
The final dashboard provides critical insights through:
1. **Species Distribution Map:** Identifying where biodiversity is thriving or under threat.
2. **Temporal Occurrence Trends:** Visualizing registration patterns over the last decades.
3. **Taxonomic Analysis:** Categorizing occurrences by class, order, and conservation status.

## 🚀 How to Reproduce
1. **Infrastructure:** Navigate to `/terraform` and run `terraform apply`.
2. **Orchestration:** Start Airflow using Docker Compose.
3. **Transformation:** Run `dbt build` to process the models in BigQuery.
4. **Dashboard:** Connect the BigQuery tables to Looker Studio.

---
*This project was completed as part of the [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp).*

