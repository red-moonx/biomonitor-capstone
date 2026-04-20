import dlt
import zipfile
import pandas as pd
import os
import requests
from dotenv import load_dotenv

# Load everything needed
load_dotenv()
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "google_credentials.json"

download_link = "https://api.gbif.org/v1/occurrence/download/request/0033820-260409193756587.zip"
zip_path = 'gbif_rescue.zip'

def download_file():
    if os.path.exists(zip_path):
        print("✅ El archivo ya existe localmente.")
        return
    print(f"📥 Bajando archivo de rescate (700k registros)...")
    with requests.get(download_link, stream=True) as r:
        r.raise_for_status()
        with open(zip_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    print("✅ Descarga completada.")

def csv_generator():
    with zipfile.ZipFile(zip_path) as z:
        csv_files = [f for f in z.namelist() if f.endswith('.csv')]
        target_csv = csv_files[0]
        print(f"📦 Procesando: {target_csv}")
        
        with z.open(target_csv) as f:
            # Optimize reading
            chunks = pd.read_csv(f, sep='\t', chunksize=50000, low_memory=False)
            for i, chunk in enumerate(chunks):
                print(f"   [DLT] Bloque {i+1} (50k registros)...")
                chunk.columns = [c.replace(" ", "_").lower() for c in chunk.columns]
                yield chunk.to_dict(orient="records")

def run_recovery():
    download_file()
        
    # CRITICAL: Specify location=EU to match our Terraform config
    pipeline = dlt.pipeline(
        pipeline_name="gbif_final_rescue_v2",
        destination=dlt.destinations.bigquery(location="EU"),
        dataset_name="biomonitor_raw"
    )
    
    print("🚀 Iniciando carga final a BigQuery (Región: EU)...")
    load_info = pipeline.run(csv_generator(), table_name="raw_mammals", write_disposition="replace")
    print("\n✅ ¡LOGRADO!")
    print(load_info)

if __name__ == "__main__":
    run_recovery()
