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
    return os.path.join(base_path, folders[0])


def get_latest_csv(path: str) -> str:
    files = [f for f in os.listdir(path) if f.endswith(".csv")]

    if not files:
        raise FileNotFoundError(f"No CSV files inside: {path}")

    files.sort(reverse=True)
    return os.path.join(path, files[0])


def ensure_dir(path: str):
    if not os.path.exists(path):
        os.makedirs(path)


def clean_performance(df: pd.DataFrame) -> pd.DataFrame:
    # Normalize columns
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("-", "_")
    )

    if "employee_id" in df.columns:
        df["employee_id"] = df["employee_id"].astype(str)

    date_col = None
    for c in ["review_date", "date", "timestamp"]:
        if c in df.columns:
            date_col = c
            break

    if date_col is None:
        raise KeyError("No review date column found in performance dataset.")

    df[date_col] = pd.to_datetime(df[date_col])
    df = df.rename(columns={date_col: "review_date"})

    df["year"] = df["review_date"].dt.year
    df["quarter"] = df["review_date"].dt.quarter

    rating_map = {
        "needs improvement": 1,
        "meets expectations": 2,
        "exceeds expectations": 3,
        "outstanding": 4
    }

    if "rating" in df.columns and df["rating"].dtype == "object":
        df["rating"] = (
            df["rating"]
            .str.lower()
            .map(rating_map)
            .fillna(df["rating"])
        )

    if "score" in df.columns:
        df["score"] = pd.to_numeric(df["score"], errors="coerce")

    return df

def run_performance_transform():
    print("Running performance transform...\n")

    raw_path = "data/raw/performance"
    latest_folder = get_latest_batch_folder(raw_path)
    latest_file = get_latest_csv(latest_folder)

    print(f"Latest batch folder: {latest_folder}")
    print(f"Latest CSV: {latest_file}")

    df = pd.read_csv(latest_file)
    cleaned = clean_performance(df)

    extract_date = os.path.basename(latest_folder)
    staging_output = f"data/staging/performance/{extract_date}"
    ensure_dir(staging_output)

    output_file = os.path.join(staging_output, "performance_cleaned.csv")
    cleaned.to_csv(output_file, index=False)

    print(f"Performance dataset cleaned and saved to: {output_file}")
    print("Performance transform is done.")


if __name__ == "__main__":
    run_performance_transform()