import os
import time
import re
from datetime import datetime

# Path configuration
PROJECT_DIR = "/workspaces/biomonitor-capstone"
LOGS_DIR = os.path.join(PROJECT_DIR, "airflow_home/logs/dag_id=biomonitor_pipeline")

def calculate_total_months(start_year=2020):
    """Calculates total months to process from start_year to today."""
    now = datetime.now()
    total = (now.year - start_year) * 12 + now.month
    return total

def get_latest_log():
    """Finds the most recent log file for the ingestion task based on modification time."""
    if not os.path.exists(LOGS_DIR):
        return None
    
    # Find all run directories and their modification times
    runs = []
    for d in os.listdir(LOGS_DIR):
        path = os.path.join(LOGS_DIR, d)
        if os.path.isdir(path):
            runs.append((path, os.path.getmtime(path)))
    
    if not runs:
        return None
    
    # Sort by modification time (newest first)
    latest_run_path = sorted(runs, key=lambda x: x[1], reverse=True)[0][0]
    task_dir = os.path.join(latest_run_path, "task_id=ingest_gbif_data")
    
    if not os.path.exists(task_dir):
        return None
    
    # Find latest attempt
    attempts = []
    for f in os.listdir(task_dir):
        if f.endswith(".log"):
            path = os.path.join(task_dir, f)
            attempts.append((path, os.path.getmtime(path)))
    
    if not attempts:
        return None
    
    # Sort by modification time (newest first)
    latest_attempt_path = sorted(attempts, key=lambda x: x[1], reverse=True)[0][0]
    return latest_attempt_path

def monitor():
    start_year = 2020
    total_months = calculate_total_months(start_year)
    
    print(f"🚀 Monitoring BioMonitor Ingestion (every 5 minutes)...")
    print(f"📅 Plan: From {start_year}-01 to {datetime.now().strftime('%Y-%m')} ({total_months} months total)")
    print("-" * 65)
    
    while True:
        log_file = get_latest_log()
        
        if not log_file:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Waiting for Airflow task to generate logs...")
        else:
            try:
                with open(log_file, "r") as f:
                    content = f.read()
                
                # Matches "Fetching: YYYY-MM" to see what's current
                fetching_matches = re.findall(r'Fetching: (\d{4}-\d{2})', content)
                # Matches "Done YYYY-MM" to count completed months
                done_matches = re.findall(r'Done \d{4}-\d{2}', content)
                
                if fetching_matches:
                    last_month = fetching_matches[-1]
                    completed_count = len(done_matches)
                    percentage = (completed_count / total_months) * 100
                    
                    # Search for total records
                    total_match = re.findall(r'Total: (\d+)\)', content)
                    records_str = f" | Records: {total_match[-1]}" if total_match else ""
                    
                    status = "🟢 Processing" if "Finished" not in content else "🏁 Finished"
                    
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] {status}: {last_month} | Progress: {percentage:.1f}% ({completed_count}/{total_months} months){records_str}")
                else:
                    # Check if the task failed
                    if "Task failed" in content or "ERROR" in content:
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] 🔴 Task reported an error. Check logs.")
                    else:
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] ⏳ Task started, initializing ingestion engine...")
            except Exception as e:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] ⚠️ Error: {e}")
        
        time.sleep(300) # 5 minutes

if __name__ == "__main__":
    monitor()
