import os
import time
import re
from datetime import datetime

# Path to Airflow logs
LOGS_DIR = "/workspaces/biomonitor-capstone/airflow_home/logs/dag_id=biomonitor_pipeline"

def get_latest_log_file():
    """Finds the most recent log file for the ingestion task."""
    if not os.path.exists(LOGS_DIR):
        return None
    
    # Get all run directories
    runs = [d for d in os.listdir(LOGS_DIR) if os.path.isdir(os.path.join(LOGS_DIR, d))]
    if not runs:
        return None
    
    # Sort to get the latest run
    latest_run = sorted(runs)[-1]
    task_dir = os.path.join(LOGS_DIR, latest_run, "task_id=ingest_gbif_data")
    
    if not os.path.exists(task_dir):
        return None
    
    # Get the latest attempt log
    attempts = [f for f in os.listdir(task_dir) if f.endswith(".log")]
    if not attempts:
        return None
    
    latest_attempt = sorted(attempts)[-1]
    return os.path.join(task_dir, latest_attempt)

def monitor():
    print("🚀 Monitoring BioMonitor BULK Ingestion: CETACEANS (Order: 733) 🐳")
    print("📅 Range: 2021-2026 | Dataset size: ~700,000 records")
    print("-----------------------------------------------------------------")
    
    last_status = None
    
    while True:
        log_file = get_latest_log_file()
        now = datetime.now().strftime('%H:%M:%S')
        
        if not log_file:
            msg = "⏳ Waiting for Airflow task to start..."
            if msg != last_status:
                print(f"[{now}] {msg}")
                last_status = msg
        else:
            try:
                with open(log_file, "r") as f:
                    content = f.read()
                
                # Deduce current status
                current_msg = ""
                
                if "Pipeline run successfully" in content:
                    current_msg = "🏁 FINISHED! Cetaceans ingested to BigQuery."
                elif "Processing chunk" in content:
                    chunk_match = re.findall(r'Processing chunk (\d+) \(approx (\d+) records\)', content)
                    if chunk_match:
                        c_num, c_records = chunk_match[-1]
                        current_msg = f"📦 Loading to BigQuery | Chunk: {c_num} | Records: ~{c_records}"
                elif "🏁 Download is READY!" in content:
                    current_msg = "📥 Downloading ZIP file from GBIF..."
                elif "[STATUS]" in content:
                    status_match = re.findall(r'\[STATUS\] (\w+)', content)
                    if status_match:
                        status = status_match[-1]
                        current_msg = f"⏳ GBIF Preparing Package | Status: {status}"
                elif "✅ Download requested!" in content:
                    current_msg = "🚀 Request sent to GBIF (waiting for queue)"
                else:
                    current_msg = "🟢 Task started, initializing..."

                if current_msg != last_status:
                    print(f"[{now}] {current_msg}")
                    last_status = current_msg
                    
                if "FINISHED" in current_msg:
                    break
                    
            except Exception as e:
                pass
        
        time.sleep(5)

if __name__ == "__main__":
    monitor()
