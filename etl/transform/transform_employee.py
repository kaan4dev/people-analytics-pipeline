import os 
import pandas as pd
from datetime import datetime, timezone

def get_latest_batch_folder(base_path: str) -> str:
    folders = [
        f for f in os.listdir(base_path)
        if os.path.isdir(os.path.join(base_path, f))
    ]

    if not folders: 
        raise FileNotFoundError(f"No dated folder found in: {base_path}")
    
    folders.sort(reverse= True)
    latest = folders[0]
    return os.path.join(base_path, latest)

def get_latest_csv(path: str) -> str:
    files = [
        f for f in os.listdir(path)
        if f.endswith(".csv")
    ]

    if not files:
        raise FileNotFoundError(f"No csv files inside: {path}")
    
    files.sort(reverse=True)
    return os.path.join(path, files[0])

def ensure_dir(path: str):
    if not os.path.exists(path):
        os.makedirs(path)

def clean_employee(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("-", "_")
    )

    if "employee_id" in df.columns:
        df["employee_id"] = df["employee_id"].astype(str)

    if "department" in df.columns:
        df["department"] = df["department"].str.strip().str.title()

    if "age" in df.columns:
        df["age"] = pd.to_numeric(df["age"], errors="coerce").fillna(0).astype(int)

    if "monthly_income" in df.columns:
        df["monthly_income"] = pd.to_numeric(df["monthly_income"], errors="coerce")

    yes_no_cols = ["attrition", "over_time"]
    for col in yes_no_cols:
        if col in df.columns:
            df[col] = (
                df[col]
                .str.strip()
                .str.lower()
                .map({"yes": 1, "no": 0})
                .fillna(0)
            )

    return df

def run_employee_transform():
    print("Running employee transform...\n")

    raw_path = "data/raw/ibm_hr"
    latest_folder = get_latest_batch_folder(raw_path)
    latest_file = get_latest_csv(latest_folder)

    print(f"Latest batch folder is: {latest_folder}")
    print(f"\nLatest .csv file is: {latest_file}")

    df = pd.read_csv(latest_file)

    cleaned = clean_employee(df)

    extract_date = os.path.basename(latest_folder)
    staging_output = f"data/raw/staging/employee/{extract_date}"
    ensure_dir(staging_output)

    output_file = os.path.join(staging_output, "employee_cleaned.csv")
    cleaned.to_csv(output_file, index= False)
    print(f"Employee dataset cleaned and saved to: {output_file}")
    print("Employee transform is done.")

if __name__ == "__main__":
    run_employee_transform()