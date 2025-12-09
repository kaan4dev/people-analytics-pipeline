import os
import shutil
from datetime import datetime, timezone
import uuid

def ensure_dir(path: str):
    if not os.path.exists(path):
        os.makedirs(path)

def extract_file(source_path: str, raw_target_dir: str, source_system: str):
    if not os.path.exists(source_path):
        raise FileNotFoundError(f"Source file doesnt exist: {source_path}")
    
    extract_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    batch_id = str(uuid.uuid4())[:8]

    target_folder = os.path.join(raw_target_dir, extract_date)
    ensure_dir(target_folder)

    filename = os.path.basename(source_path)
    target_filename = f"{source_system}_batch_{batch_id}.csv"
    target_path = os.path.join(target_folder, target_filename)

    shutil.copy(source_path, target_path)

    print(f"Data extracted from {source_system}: {target_path}")

    manifest_path = os.path.join(target_folder, "manifest.txt")
    with open(manifest_path, "a") as mf:
        mf.write(f"{datetime.now(timezone.utc)} | {source_system} | {filename} | batch = {batch_id}\n")
        print(f"Manifest updated: {manifest_path}")

def run_extract():
    print("Starting extract process...\n")

    attendance_src = "data/raw/attendance/attendance_logs.csv"
    engagement_src = "data/raw/engagement/engagement_surveys.csv"
    performance_src = "data/raw/performance/performance_reviews.csv"
    ibm_hr_src = "data/raw/ibm_hr/employee_data.csv"

    attendance_target = "data/raw/attendance"
    engagement_target = "data/raw/engagement"
    performance_target = "data/raw/performance"
    ibm_hr_target = "data/raw/ibm_hr"

    extract_file(attendance_src, attendance_target, source_system="attendance_system")
    extract_file(engagement_src, engagement_target, source_system="engagement_system")
    extract_file(performance_src, performance_target, source_system="performance_system")
    extract_file(ibm_hr_src, ibm_hr_target, source_system="hr_core_system")

    print("\nExtract process done.")

if __name__ == "__main__":
    run_extract()


