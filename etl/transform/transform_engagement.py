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

    folders.sort(reverse=True)
    latest = folders[0]
    return os.path.join(base_path, latest)

def get_latest_csv(path: str) -> str:
    files = [f for f in os.listdir(path) if f.endswith(".csv")]

    if not files:
        raise FileNotFoundError(f"No csv files inside: {path}")
    
    files.sort(reverse=True)
    latest_csv = files[0]

    return os.path.join(path, latest_csv)

def ensure_dir(path: str):
    if not os.path.exists(path):
        os.makedirs(path)

def clean_engagement(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (df.columns
    .str.strip()
    .str.lower()
    .str.replace(" ", "_")
    .str.replace("-", "_"))

    if "employee_id" in df.columns:
        df["employee_id"] = df["employee_id"].astype(str)

    date_col = None
    for col in ["survey_date", "date", "timestamp"]:
        if col in df.columns:
            date_col = col
            break

    if date_col is None:
        raise KeyError("Survey date column not found in engagement dataset.")

    df[date_col] = pd.to_datetime(df[date_col])
    df = df.rename(columns={date_col: "survey_date"})

    df["year"] = df["survey_date"].dt.year
    df["month"] = df["survey_date"].dt.month

    likert_map = {
        "strongly disagree": 1,
        "disagree": 2,
        "neutral": 3,
        "agree": 4,
        "strongly agree": 5
    }

    for col in df.columns:
        if df[col].dtype == "object":
            lower = df[col].str.lower()
            if lower.isin(likert_map).any():
                df[col] = lower.map(likert_map)

    return df

def run_engagement_transform():
    print("\nRunning engagement transform...\n")

    raw_path = "data/raw/engagement"
    latest_folder = get_latest_batch_folder(raw_path)
    latest_file = get_latest_csv(latest_folder)

    print(f"Latest batch folder is: {latest_folder}")
    print(f"Latest CSV file is: {latest_file}")

    df = pd.read_csv(latest_file)
    cleaned = clean_engagement(df)

    extract_date = os.path.basename(latest_folder)
    staging_output = f"data/staging/engagement/{extract_date}"
    ensure_dir(staging_output)
    
    output_file = os.path.join(staging_output, "engagement_cleaned.csv")
    cleaned.to_csv(output_file, index=False)

    print(f"Engagement dataset cleaned and saved to: {output_file}")
    print("Engagement transform is done.")


if __name__ == "__main__":
    run_engagement_transform()