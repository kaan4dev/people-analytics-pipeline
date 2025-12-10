import os
import pandas as pd
from datetime import datetime


def get_latest_batch_folder(base_path: str) -> str:
    folders = [
        f for f in os.listdir(base_path)
        if os.path.isdir(os.path.join(base_path, f))
    ]

    if not folders:
        raise FileNotFoundError(f"No dated folders in {base_path}")

    folders.sort(reverse=True)
    return os.path.join(base_path, folders[0])


def get_latest_csv(path: str) -> str:
    files = [f for f in os.listdir(path) if f.endswith(".csv")]
    if not files:
        raise FileNotFoundError(f"No CSV files in {path}")

    files.sort(reverse=True)
    return os.path.join(path, files[0])


def ensure_dir(path: str):
    if not os.path.exists(path):
        os.makedirs(path)


def clean_attendance(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("-", "_")
    )

    if "status" in df.columns:
        df["status"] = df["status"].astype(str).str.strip().str.lower()

        df["status"] = df["status"].replace({
            "present": 1,
            "absent": 0,
            "late": 0.5,
            "remote": 1,
            "leave": 0
        })

    timestamp_candidates = ["timestamp", "timestamp_local", "check_in"]

    timestamp_col = None
    for col in timestamp_candidates:
        if col in df.columns:
            timestamp_col = col
            break

    if timestamp_col:
        df[timestamp_col] = pd.to_datetime(df[timestamp_col])
        df = df.rename(columns={timestamp_col: "timestamp"})
        df["date"] = df["timestamp"].dt.date

    elif "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"]).dt.date

    else:
        raise KeyError(
            "No usable date or timestamp field found. Expected one of: "
            "timestamp, timestamp_local, check_in, date"
        )

    date_series = pd.to_datetime(df["date"])
    df["year"] = date_series.dt.year
    df["month"] = date_series.dt.month
    df["weekday"] = date_series.dt.day_name()

    df["employee_id"] = df["employee_id"].astype(str)

    sort_cols = ["employee_id"]
    if "timestamp" in df.columns:
        sort_cols.append("timestamp")
    else:
        sort_cols.append("date")

    df = df.sort_values(sort_cols)

    return df


def run_attendance_transform():
    print("\nRunning attendance transform...\n")

    raw_path = "data/raw/attendance"
    latest_folder = get_latest_batch_folder(raw_path)
    latest_file = get_latest_csv(latest_folder)

    print(f"Latest folder: {latest_folder}")
    print(f"Latest CSV: {latest_file}")

    df = pd.read_csv(latest_file)
    cleaned = clean_attendance(df)

    extract_date = os.path.basename(latest_folder)
    staging_output = f"data/staging/attendance/{extract_date}"
    ensure_dir(staging_output)

    output_file = os.path.join(staging_output, "attendance_cleaned.csv")
    cleaned.to_csv(output_file, index=False)

    print(f"Saved cleaned attendance dataset to: {output_file}")
    print("Attendance transform is done.\n")


if __name__ == "__main__":
    run_attendance_transform()
