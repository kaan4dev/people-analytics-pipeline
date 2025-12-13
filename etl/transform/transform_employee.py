import os
import pandas as pd

def get_latest_batch_folder(base_path: str) -> str:
    if not os.path.exists(base_path):
        raise FileNotFoundError(f"Raw path does not exist: {base_path}")

    folders = [
        f for f in os.listdir(base_path)
        if os.path.isdir(os.path.join(base_path, f))
    ]

    if not folders:
        raise FileNotFoundError(f"No dated folder found in: {base_path}")

    folders.sort(reverse=True)
    return os.path.join(base_path, folders[0])

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
    os.makedirs(path, exist_ok=True)


def clean_employee(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("-", "_")
    )

    if "employee_id" not in df.columns:
        for candidate in ["id", "employee_number", "emp_id", "employeeid"]:
            if candidate in df.columns:
                df = df.rename(columns={candidate: "employee_id"})
                break

    if "employee_id" not in df.columns:
        raise ValueError(
            "Employee dataset must contain a primary key column "
            "(employee_id / employee_number / id / emp_id)"
        )

    df["employee_id"] = df["employee_id"].astype(str)

    if "department" in df.columns:
        df["department"] = df["department"].astype(str).str.strip().str.title()

    if "age" in df.columns:
        df["age"] = pd.to_numeric(df["age"], errors="coerce")

    if "monthly_income" in df.columns:
        df["monthly_income"] = pd.to_numeric(df["monthly_income"], errors="coerce")

    yes_no_cols = ["attrition", "over_time"]
    for col in yes_no_cols:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.strip()
                .str.lower()
                .map({"yes": 1, "no": 0})
            )

    return df

def run_employee_transform():
    print("\nRunning employee transform...\n")

    raw_path = "data/raw/ibm_hr"
    latest_folder = get_latest_batch_folder(raw_path)
    latest_file = get_latest_csv(latest_folder)

    print(f"Latest batch folder: {latest_folder}")
    print(f"Latest CSV file: {latest_file}")

    df = pd.read_csv(latest_file)
    cleaned = clean_employee(df)

    extract_date = os.path.basename(latest_folder)
    staging_output = f"data/staging/employee/{extract_date}"
    ensure_dir(staging_output)

    output_file = os.path.join(staging_output, "employee_cleaned.csv")
    cleaned.to_csv(output_file, index=False)

    print(f"\nEmployee dataset saved to: {output_file}")
    print("Employee transform is done.")


if __name__ == "__main__":
    run_employee_transform()